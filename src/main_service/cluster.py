from typing import List, Optional, Callable
from concurrent.futures import ThreadPoolExecutor
from copy import copy
import requests
from dataclasses import dataclass
from datetime import datetime, timedelta
from uuid import uuid4
from time import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from google.cloud import firestore
from google.cloud.firestore import SERVER_TIMESTAMP, FieldFilter
from google.cloud.compute_v1 import InstancesClient
from google.api_core.exceptions import NotFound

from main_service import PROJECT_ID, TZ
from main_service.node import Node, Container, JustifiedDeletion
from main_service.helpers import Logger

# If a job has no nodes assigned within `MAX_TIME_TO_ASSIGN_NODE` sec of starting,
# something went wrong, and an error should be thrown.
MAX_TIME_TO_ASSIGN_NODE = 10

N_FOUR_STANDARD_CPU_TO_RAM = {2: 8, 4: 16, 8: 32, 16: 64, 32: 128, 48: 192, 64: 256, 80: 320}


def parallelism_capacity(machine_type: str, func_cpu: int, func_ram: int, func_gpu: bool):
    """What is the maximum number of parallel subjobs this machine_type can run a job with the
    following resource requirements at?
    """
    if machine_type == "a2-highgpu-1g":
        return 1
    elif machine_type.startswith("n4-standard") and machine_type.split("-")[-1].isdigit():
        vm_cpu = int(machine_type.split("-")[-1])
        vm_ram = N_FOUR_STANDARD_CPU_TO_RAM[vm_cpu]
        return min(vm_cpu // func_cpu, vm_ram // func_ram)
    raise ValueError(f"machine_type must be in: a2-highgpu-1g, n4-standard-X")


class NoNodesAssignedToJob(Exception):
    pass


@dataclass
class Job:
    id: str
    current_parallelism: int
    target_parallelism: int

    def to_dict(self):
        return dict(
            id=self.id,
            current_parallelism=self.current_parallelism,
            target_parallelism=self.target_parallelism,
        )


@dataclass
class GCEBurlaNode:
    # Represents GCE VM Instance running as a node in a Burla cluster.
    name: str  # Instance Name
    zone: str  # CA Zone
    status: str  # Instance Status


def reboot_node(node_svc_host, node_containers):
    try:
        response = requests.post(f"{node_svc_host}/reboot", json=node_containers)
        response.raise_for_status()
    except Exception as e:
        # if node already rebooting, skip.
        if "409" not in str(e):
            raise e


def reboot_nodes_with_job(db: firestore.Client, job_id: str):
    nodes_with_job_filter = FieldFilter("current_job", "==", job_id)
    nodes_with_job = db.collection("nodes").where(filter=nodes_with_job_filter).stream()

    with ThreadPoolExecutor() as executor:
        futures = []
        for node_snapshot in nodes_with_job:
            node_dict = node_snapshot.to_dict()
            future = executor.submit(reboot_node, node_dict["host"], node_dict["containers"])
            futures.append(future)

        # this raises any exceptions
        for future in as_completed(futures):
            future.result()


def reconcile(db: firestore.Client, logger: Logger, add_background_task: Callable):
    """
    Modify cluster such that: current state -> correct/optimal state.
    Every cluster operation (adding/deleting/assigning nodes) has a non 100% chance of success.
    To make sure the cluster works when actions fail, we CONSTANTLY check what the state
    should be then adjust things accordingly (what this function does).

    Below I remove nodes from the nodes list as they are addressed / fixed.
    """
    instance_client = InstancesClient()

    # load nodes from db
    nodes = []
    node_not_deleted = FieldFilter("deleted_at", "==", None)
    node_doc_refs = db.collection("nodes").where(filter=node_not_deleted).stream()
    for node_ref in node_doc_refs:
        node_doc = node_ref.to_dict()
        containers = [Container.from_dict(c) for c in node_doc.get("containers", [])]
        node = Node.from_previous_state(
            db=db,
            logger=logger,
            add_background_task=add_background_task,
            instance_name=node_doc["instance_name"],
            machine_type=node_doc["machine_type"],
            containers=containers if node_doc.get("containers") else None,
            started_booting_at=node_doc["started_booting_at"],
            finished_booting_at=node_doc["finished_booting_at"],
            host=node_doc["host"],
            zone=node_doc["zone"],
            current_job=node_doc["current_job"],
            parallelism=node_doc["parallelism"],
            instance_client=instance_client,
            delete_when_done=node_doc["delete_when_done"],
            is_deleting=node_doc["is_deleting"],
            is_booting=node_doc["is_booting"],
        )
        nodes.append(node)

    # Get list of burla nodes from GCE
    nodes_from_gce = []
    for zone, instances_scope in instance_client.aggregated_list(project=PROJECT_ID):
        vms = getattr(instances_scope, "instances", [])
        burla_tag = "burla-cluster-node"
        gce_nodes = [GCEBurlaNode(i.name, zone, i.status) for i in vms if burla_tag in i.tags.items]
        nodes_from_gce.extend(gce_nodes)

    # 1. Delete nodes that are in GCE and not tracked in the DB.
    names_of_nodes_from_db = [node.instance_name for node in nodes]
    for gce_node in nodes_from_gce:
        deleting_or_starting_status = ["STOPPING", "TERMINATED", "PROVISIONING"]
        gce_node_not_deleting_or_starting = gce_node.status not in deleting_or_starting_status
        node_in_gce_and_not_in_db = gce_node.name not in names_of_nodes_from_db

        if node_in_gce_and_not_in_db and gce_node_not_deleting_or_starting:
            msg = f"Deleting node {gce_node.name} because it is NOT IN the DB!"
            logger.log(msg + f" (instance-status={gce_node.status})", severity="WARNING")
            try:
                zone = gce_node.zone.split("/")[1]
                add_background_task(
                    instance_client.delete,
                    project=PROJECT_ID,
                    zone=zone,
                    instance=gce_node.name,
                )
            except NotFound:
                pass

    # 2. Check that status of each node in db, ensure status makes sense, correct accordingly.
    names_of_nodes_from_gce = [gce_node.name for gce_node in nodes_from_gce]
    for node in nodes:
        node_not_in_gce = node.instance_name not in names_of_nodes_from_gce
        status = node.status()

        if status in ["READY", "RUNNING", "DELETING"] and node_not_in_gce:
            # node in database but not in compute engine?
            db.collection("nodes").document(node.instance_name).update({"status": "DELETED"})
        elif status == "BOOTING":
            # been booting for too long ?
            time_since_boot = datetime.now(TZ) - node.started_booting_at
            gce_vm_should_exist_by_now = time_since_boot > timedelta(seconds=30 * 1)
            if gce_vm_should_exist_by_now:
                msg = f"Deleting node {node.instance_name} because it is not in GCE yet "
                msg += f"and it started booting {time_since_boot.total_seconds()}s ago."
                logger.log(msg)
                node.async_delete()
                nodes.remove(node)
        elif status == "RUNNING":
            # job is still active?
            job = db.collection("jobs").document(node.current_job).get().to_dict()
            job_ended = job.get("ended_at") is None
            if job_ended:
                msg = f"Rebooting node {node.instance_name}"
                logger.log(msg + f"because it's job ({node.current_job}) has ended.")
                node.async_reboot()
        elif status == "FAILED":
            # Delete node
            logger.log(f"Deleting node: {node.instance_name} because it has FAILED")
            node.async_delete()
            nodes.remove(node)

    # 3. Check that the cluster does or will match the specified default configuration.
    config = db.collection("cluster_config").document("cluster_config").get().to_dict()
    for spec in config["Nodes"]:
        # standby_nodes = [n for n in nodes if n.current_job is None]
        standby_nodes = [n for n in nodes if n.machine_type == spec["machine_type"]]

        # not enough of this machine_type on standby ? (add more standby nodes ?)
        if len(standby_nodes) < spec["quantity"]:
            node_deficit = spec["quantity"] - len(standby_nodes)
            for i in range(node_deficit):
                containers = [Container.from_dict(c) for c in spec["containers"]]
                machine = spec["machine_type"]
                instance_name = f"burla-node-{uuid4().hex}"
                msg = f"Adding another {machine} ({instance_name})"
                msg += f"because cluster is {node_deficit-i} short"
                logger.log(msg)

                def add_node(instance_name, machine_type, containers):
                    try:
                        Node.start(
                            db=db,
                            logger=logger,
                            add_background_task=add_background_task,
                            machine_type=machine_type,
                            instance_name=instance_name,
                            containers=containers,
                        )
                    except JustifiedDeletion:
                        # Thrown in Node constructor: Means node was correctly deleted mid-boot.
                        pass

                add_background_task(add_node, instance_name, spec["machine_type"], containers)

        # too many of this machine_type on standby ?  (remove some standby nodes ?)
        elif len(standby_nodes) > spec["quantity"]:
            nodes_to_remove = sorted(standby_nodes, key=lambda n: n.time_until_booted())
            num_extra_nodes = len(standby_nodes) - spec["quantity"]
            nodes_to_remove = nodes_to_remove[-num_extra_nodes:]

            for i, node in enumerate(nodes_to_remove):
                surplus = len(nodes_to_remove) - i
                logger.log(f"DELETING an {machine} node because cluster has {surplus} too many")
                node.async_delete()

    logger.log("DONE")
    # 4. Check that none of the current jobs are done, failed, or not being worked on.
    #    (they should be marked as having ended)
    #
    # job_not_over = FieldFilter("ended_at", "==", None)
    # job_doc_refs = db.collection("jobs").where(filter=job_not_over).stream()
    # for job_ref in job_doc_refs:
    #     job_doc = job_ref.to_dict()
    #     job = Job(
    #         id=job_ref.id,
    #         current_parallelism=job_doc["current_parallelism"],
    #         target_parallelism=job_doc["target_parallelism"],
    #     )
    #     nodes_assigned_to_job = [node for node in nodes if node.current_job == job.id]
    #     nodes_working_on_job = [n for n in nodes_assigned_to_job if n.status() == "RUNNING"]

    #     if not nodes_assigned_to_job:
    #         # mark job as ended
    #         job_ref = db.collection("jobs").document(job.id)
    #         job_ref.update({"ended_at": SERVER_TIMESTAMP})
    #     elif nodes_assigned_to_job and not nodes_working_on_job:
    #         # state of these nodes should be one of: please_reboot, booting, ready?
    #         # Nodes should have ultimatums, eg:
    #         #    Be in state X within Y amount of time or your state is set to: "FAILED"
    #         pass

    #     any_failed = False
    #     all_done = True
    #     for node in nodes_working_on_job:
    #         job_status = node.job_status(job_id=job.id)
    #         any_failed = job_status["any_subjobs_failed"]
    #         all_done = job_status["all_subjobs_done"]
    #         node_is_done = any_failed or job_status["all_subjobs_done"]

    #         if node_is_done and node.delete_when_done:
    #             node.async_delete()
    #             nodes.remove(node)
    #         elif node_is_done:
    #             add_background_task(reassign_or_remove_node, node)

    #     if any_failed or all_done:
    #         _remove_job_from_cluster_state_in_db(job.id)
    #     if any_failed:
    #         return "FAILED"
    #     if all_done:
    #         return "DONE"

    #     return "RUNNING"

    #
    # 5. Check that all jobs do or will match the target level of parallelism.
    #
    #
    #


class Cluster:
    """
    Burla is designed to have one single global cluster running in the backend at all times.
    Certain things here will not work if more than one cluster is running (shouldn't be possible).
    """

    def __init__(
        self,
        nodes: List[Node],
        jobs: List[Job],
        db: firestore.Client,
        logger: Logger,
        add_background_task: Callable,
        instance_client: Optional[InstancesClient] = None,
    ):
        self.nodes = nodes
        self.jobs = jobs
        self.db = db
        self.logger = logger
        self.add_background_task = add_background_task
        self.instance_client = instance_client
        self.reconciling = False

    @classmethod
    def from_database(
        cls,
        db: firestore.Client,
        logger: Logger,
        add_background_task: Callable,
        instance_client: Optional[InstancesClient] = None,
    ):
        instance_client = instance_client if instance_client else InstancesClient()

        self = cls(
            nodes=[],
            jobs=[],
            db=db,
            add_background_task=add_background_task,
            logger=logger,
            instance_client=instance_client,
        )

        node_not_deleted = FieldFilter("deleted_at", "==", None)
        node_doc_refs = self.db.collection("nodes").where(filter=node_not_deleted).stream()

        for node_ref in node_doc_refs:
            node_doc = node_ref.to_dict()
            containers = [Container.from_dict(c) for c in node_doc.get("containers", [])]
            node = Node.from_previous_state(
                db=db,
                logger=logger,
                add_background_task=self.add_background_task,
                instance_name=node_doc["instance_name"],
                machine_type=node_doc["machine_type"],
                containers=containers if node_doc.get("containers") else None,
                started_booting_at=node_doc["started_booting_at"],
                finished_booting_at=node_doc["finished_booting_at"],
                host=node_doc["host"],
                zone=node_doc["zone"],
                current_job=node_doc["current_job"],
                parallelism=node_doc["parallelism"],
                instance_client=instance_client,
                delete_when_done=node_doc["delete_when_done"],
                is_deleting=node_doc["is_deleting"],
                is_booting=node_doc["is_booting"],
            )
            self.nodes.append(node)

        job_not_over = FieldFilter("ended_at", "==", None)
        job_doc_refs = self.db.collection("jobs").where(filter=job_not_over).stream()

        for job_ref in job_doc_refs:
            job_doc = job_ref.to_dict()
            job = Job(
                id=job_ref.id,
                current_parallelism=job_doc["current_parallelism"],
                target_parallelism=job_doc["target_parallelism"],
            )
            self.jobs.append(job)
        return self

    def restart(self, force=False):
        """Force means: do not wait for nodes to finish current job before deleting."""
        self.instance_client = self.instance_client or InstancesClient()

        for node in copy(self.nodes):
            if (force == False) and (node.status() == "BUSY"):
                node.delete_when_done = True
                node.update_state_in_db()
            else:
                self.logger.log(f"Deleting node {node.instance_name} because cluster is restarting")
                node.async_delete()
                self.nodes.remove(node)

        config = self.db.collection("cluster_config").document("cluster_config").get().to_dict()
        machine_types = [n["machine_type"] for n in config["Nodes"] for _ in range(n["quantity"])]
        _containerss = [n["containers"] for n in config["Nodes"] for _ in range(n["quantity"])]

        containerss = []
        for _containers in _containerss:
            containerss.append([Container.from_dict(container) for container in _containers])

        def _add_node_logged(*a, **kw):
            instance_name = f"burla-node-{uuid4().hex}"
            self.logger.log(f"Cluster is restarting: adding node {instance_name}")
            self.add_node(*a, instance_name=instance_name, **kw)

        with ThreadPoolExecutor() as executor:
            list(executor.map(_add_node_logged, machine_types, containerss))

        self.logger.log("DONE RESTARTING RECONCILING NOW")
        self.add_background_task(reconcile, self.db, self.logger, self.add_background_task)

    def execute(
        self,
        job_id: str,
        func_cpu: int,
        func_ram: int,
        gpu: bool,
        target_parallelism: int,
        python_version: str,
        container_image: str,
        function_pkl: Optional[bytes] = None,
    ):
        """
        `function_pkl` is only sent if the function & inputs fit in a single request.
        """

        # assign job to any compatible nodes.
        # start an extra unassigned node, for every assigned node to maintain same #"READY" nodes.
        current_parallelism = 0
        for node in self.nodes:
            max_parallelism = parallelism_capacity(node.machine_type, func_cpu, func_ram, gpu)
            compatible_with_job = max_parallelism > 0
            is_ready = node.status() == "READY"

            if compatible_with_job and is_ready:
                parallelism = min([target_parallelism - current_parallelism, max_parallelism])
                node.execute(
                    job_id=job_id,
                    parallelism=parallelism,
                    function_pkl=function_pkl,
                    delete_when_done=True,
                )
                instance_name = f"burla-node-{uuid4().hex}"
                self.logger.log(f"adding node {instance_name} to maintain num nodes on standby.")
                self.add_node_async(node.machine_type, node.containers, instance_name)
                current_parallelism += parallelism

            if current_parallelism >= target_parallelism:
                break

        # start remaining machines necessary to achieve requested parallelism
        while current_parallelism < target_parallelism:
            machine_type = "a2-highgpu-1g" if gpu else "n4-standard-4"
            max_parallelism = parallelism_capacity(machine_type, func_cpu, func_ram, gpu)
            parallelism = min([target_parallelism - current_parallelism, max_parallelism])

            # TODO: (currently only compatible with default containers)
            # Eliminate `python_executable` config option.
            # Make a separate default container per desired python version,
            # each having the entrypoint `python` (same entrypoint as custom containers).
            # Then pass same container/version assigned to job through to the new node here.
            assert "/burla-job-containers/" in container_image  # <- temporary, read above.
            python_executable = f"/.pyenv/versions/{python_version}.*/bin/python{python_version}"
            container = Container(
                image=container_image,
                python_executable=python_executable,
                python_version=python_version,
            )
            instance_name = f"burla-node-{uuid4().hex}"
            self.logger.log(f"increase parallelism for job: {job_id}: adding node {instance_name}")
            self.add_node_async(
                machine_type,
                containers=[container],
                instance_name=instance_name,
                pre_assign_job=job_id,
                function_pkl=function_pkl,
                parallelism=parallelism,
                delete_when_done=True,
            )
            current_parallelism += max_parallelism

    def status(self, job_id: str):
        nodes_assigned_to_job = [node for node in self.nodes if node.current_job == job_id]
        nodes_working_on_job = [n for n in nodes_assigned_to_job if n.status() == "RUNNING"]

        job_ref = self.db.collection("jobs").document(job_id)
        job = job_ref.get().to_dict()
        job_is_done = bool(job.get("ended_at"))

        if job_is_done:
            return "DONE"

        if nodes_assigned_to_job == []:
            time_since_job_started = time() - job["started_at"].timestamp()
            if time_since_job_started > MAX_TIME_TO_ASSIGN_NODE:
                msg = f"Timeout! no nodes assigned to job after {time_since_job_started} sec??"
                raise NoNodesAssignedToJob(msg)
            else:
                any_failed = False
                all_done = False
        else:
            any_failed = False
            all_done = True
            for node in nodes_working_on_job:

                start = time()
                job_status = node.job_status(job_id=job_id)
                self.logger.log(f"got node job status after {time()-start}s")

                any_failed = job_status["any_subjobs_failed"]
                all_done = job_status["all_subjobs_done"]
                node_is_done = any_failed or all_done

                # if node_is_done and node.delete_when_done:
                #     msg = f"Deleting node {node.instance_name} because "
                #     msg += "job is done and node.delete_when_done = True"
                #     self.logger.log(msg)
                #     node.async_delete()
                # elif node_is_done:
                #     msg = f"reassigning or removing node {node.instance_name} because job {job_id} "
                #     msg += f"has no more inputs to process."
                #     self.logger.log(msg)
                #     add_background_task(self.reassign_or_remove_node, node
        # self.logger.log(f"done getting job status")

        if any_failed:
            return "FAILED"
        elif all_done:
            job_ref.update({"ended_at": SERVER_TIMESTAMP})
            self.logger.log(f"JOB {job_id} is done!")
            return "DONE"
        else:
            return "RUNNING"

    def add_node(
        self,
        machine_type: str,
        containers: list[Container],
        instance_name: Optional[str] = None,
        pre_assign_job: Optional[str] = None,
        function_pkl: Optional[bytes] = None,
        parallelism: Optional[int] = None,
        delete_when_done: Optional[bool] = False,
    ):
        try:
            if (pre_assign_job and not parallelism) or (parallelism and not pre_assign_job):
                raise ValueError("pre_assign_job and parallelism are mutually inclusive")
            elif pre_assign_job:
                node = Node.start_and_execute(
                    db=self.db,
                    logger=self.logger,
                    add_background_task=self.add_background_task,
                    machine_type=machine_type,
                    containers=containers,
                    job_id=pre_assign_job,
                    function_pkl=function_pkl,
                    parallelism=parallelism,
                    instance_name=instance_name,
                    delete_when_done=delete_when_done,
                )
            else:
                node = Node.start(
                    db=self.db,
                    logger=self.logger,
                    add_background_task=self.add_background_task,
                    machine_type=machine_type,
                    instance_name=instance_name,
                    containers=containers,
                )
            self.nodes.append(node)
        except JustifiedDeletion:
            # Thrown in Node constructor:
            # Means node was correctly deleted while booting and will not be added to `self.nodes`.
            pass

    def add_node_async(self, *a, **kw):
        self.add_background_task(self.add_node, *a, **kw)
