from typing import List, Optional
from concurrent.futures import ThreadPoolExecutor
from copy import copy
import requests
from dataclasses import dataclass
from datetime import datetime, timedelta
from uuid import uuid4
from time import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from fastapi import BackgroundTasks
from google.cloud import firestore
from google.cloud.firestore import SERVER_TIMESTAMP, FieldFilter
from google.cloud.compute_v1 import InstancesClient
from google.api_core.exceptions import NotFound

from main_service import PROJECT_ID, TZ
from main_service.node import Node, TOTAL_BOOT_TIME, TOTAL_REBOOT_TIME, Container, JustifiedDeletion
from main_service.helpers import Logger, add_logged_background_task

# If a job has no nodes assigned within `MAX_TIME_TO_ASSIGN_NODE` sec of starting,
# something went wrong, and an error should be thrown.
MAX_TIME_TO_ASSIGN_NODE = 10


def parallelism_capacity(machine_type: str, func_cpu: int, func_ram: int, gpu: bool):
    """What is the maximum number of parallel subjobs this machine_type can run a job with the
    following resource requirements at?
    """
    if machine_type == "a2-highgpu-1g":
        return 1
    elif machine_type == "n1-standard-96":
        return min(96 // func_cpu, 360 // func_ram)
    elif machine_type == "n1-standard-4":
        return min(4 // func_cpu, 15 // func_ram)
    raise ValueError(f"machine_type must be in: a2-highgpu-1g, n1-standard-96, n1-standard-4")


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


def reboot_nodes_with_job(DB: firestore.Client, job_id: str):
    nodes_with_job_filter = FieldFilter("current_job", "==", job_id)
    nodes_with_job = DB.collection("nodes").where(filter=nodes_with_job_filter).stream()

    def reboot_node(node_svc_host, node_containers):
        try:
            response = requests.post(f"{node_svc_host}/reboot", json=node_containers)
            response.raise_for_status()
        except Exception as e:
            # if node already rebooting, skip.
            if "409" not in str(e):
                raise e

    with ThreadPoolExecutor() as executor:
        futures = []
        for node_snapshot in nodes_with_job:
            node_dict = node_snapshot.to_dict()
            future = executor.submit(reboot_node, node_dict["host"], node_dict["containers"])
            futures.append(future)

        # this raises any exceptions
        for future in as_completed(futures):
            future.result()


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
        background_tasks: BackgroundTasks,
        instance_client: Optional[InstancesClient] = None,
    ):
        self.nodes = nodes
        self.jobs = jobs
        self.db = db
        self.logger = logger
        self.background_tasks = background_tasks
        self.instance_client = instance_client
        self.reconciling = False

    @classmethod
    def from_database(
        cls,
        db: firestore.Client,
        logger: Logger,
        background_tasks: BackgroundTasks,
        instance_client: Optional[InstancesClient] = None,
    ):
        instance_client = instance_client if instance_client else InstancesClient()

        self = cls(
            nodes=[],
            jobs=[],
            db=db,
            background_tasks=background_tasks,
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
                background_tasks=self.background_tasks,
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

        self.reconcile()

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
            machine_type = "a2-highgpu-1g" if gpu else "n1-standard-4"
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
        job_is_done = bool(job.get("done_at"))

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
                #     add_logged_background_task(
                #         self.background_tasks, self.logger, self.reassign_or_remove_node, node
                #     )
        # self.logger.log(f"done getting job status")

        if any_failed:
            return "FAILED"
        elif all_done:
            job_ref.update({"done_at": SERVER_TIMESTAMP})
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
                    background_tasks=self.background_tasks,
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
                    background_tasks=self.background_tasks,
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
        # allows service to respond while continuing to wait for node to start in the background.
        add_logged_background_task(self.background_tasks, self.logger, self.add_node, *a, **kw)

    def reassign_or_remove_node(self, node: Node):
        """
        It takes `TOTAL_BOOT_TIME` to boot up a node.
        It takes `TOTAL_REBOOT_TIME` to reboot a node (we dont need to recreate the vm for this).
        Its much faster (~2min) to reboot and reassign an extra node than it is to start a new one.

        This function checks if the provided `node` can be rebooted and reassigned to a new job or
        added to the the set of "READY" nodes that are waiting for new jobs (standby nodes).
        It this node can't be reassigned it's deleted.
        """
        # 1. Can we reassign this node to a job to speed up that job ?
        #    This should happen (first) over the standby speedup because of the UX implication.
        for job in self.jobs:
            future_parallelism = job.current_parallelism
            # These nodes are: booting, currently assigned to `job`
            replaceable_nodes = [n for n in self.nodes if n.current_job == job.id]
            replaceable_nodes = [n for n in replaceable_nodes if n.finished_booting_at == None]

            replaceable_nodes = sorted(replaceable_nodes, key=lambda n: n.time_until_booted())

            highest_remaining_boot_time = 0
            for replaceable_node in replaceable_nodes:
                future_parallelism += replaceable_node.target_parallelism
                time_spent_booting = datetime.now(TZ) - replaceable_node.started_booting_at
                remaining_boot_time = TOTAL_BOOT_TIME - time_spent_booting
                if remaining_boot_time > highest_remaining_boot_time:
                    highest_remaining_boot_time = remaining_boot_time
                    node_to_replace = replaceable_node
            # 1a. Will this job get to it's target_parallelism? If not, fill the gap with this node.
            if future_parallelism < job.target_parallelism:
                parallelism_deficit = job.target_parallelism - future_parallelism
                msg = f"Assigning node {node.instance_name} to job {job.id} to increase parallelism"
                msg += " at a faster rate."
                self.logger.log(msg)
                node.async_reboot_and_execute(job_id=job.id, parallelism=parallelism_deficit)
                return
            # 1b. Can this node start working on `job` faster than any of the other assigned nodes ?
            #     If so, replace the slower node, this will increase parallelism for the user faster
            if highest_remaining_boot_time > TOTAL_REBOOT_TIME:
                msg = f"Replacing node {node_to_replace.instance_name} with {node.instance_name} "
                msg += f"because node {node.instance_name} is predicted to begin work on job "
                msg += f"{node_to_replace.current_job} faster than node "
                msg += f"{node_to_replace.instance_name} would have.\n"
                msg += f"Deleting node {node_to_replace.instance_name} and assigning node "
                msg += f"{node.instance_name} to job {node_to_replace.current_job}."
                self.logger.log(msg)
                node.async_reboot_and_execute(
                    job_id=node_to_replace.current_job,
                    parallelism=node_to_replace.parallelism,
                    delete_when_done=node_to_replace.delete_when_done,
                )
                self.nodes.remove(node_to_replace)
                node_to_replace.async_delete()
                return

        # 2. Can we add to the set of "READY" nodes to reach standby faster?
        #    Doing so will increase the probablity of a low latency response if there is a request.
        config = self.db.collection("cluster_config").document("cluster_config").get().to_dict()

        # How many nodes of this `machine_type` should we have on standby ?
        target_num_replaceable_nodes = 0
        for node_spec in config["Nodes"]:
            if node.machine_type == node_spec["machine_type"]:
                target_num_replaceable_nodes = node_spec["quantity"]

        # These nodes: are not assigned to a job, are booting, have same machine_type as `node`
        _replaceable_nodes = [n for n in self.nodes if n.current_job is None]
        _replaceable_nodes = [n for n in _replaceable_nodes if n.finished_booting_at == None]
        replaceable_nodes = [n for n in _replaceable_nodes if n.machine_type == node.machine_type]

        # 2a. Will we have enough of these machines on standby? If not, add this machine.
        if len(replaceable_nodes) < target_num_replaceable_nodes:
            msg = f"Rebooting node {node.instance_name} to reach desired number of standby nodes "
            self.logger.log(msg + "in less time.")
            node.async_reboot()
            return

        highest_remaining_boot_time = 0
        for replaceable_node in replaceable_nodes:  # nodes not assigned to job that are booting.
            time_spent_booting = datetime.now(TZ) - replaceable_node.started_booting_at
            remaining_boot_time = TOTAL_BOOT_TIME - time_spent_booting
            if remaining_boot_time > highest_remaining_boot_time:
                highest_remaining_boot_time = remaining_boot_time
                node_to_replace = replaceable_node
        # 2b. Can this node reach the `READY` state faster than any of the other nodes ?
        #     Doing so will increase the probablity of a low latency response if there is a request.
        if highest_remaining_boot_time > TOTAL_REBOOT_TIME:
            msg = f"Replacing node {node_to_replace.instance_name} with {node.instance_name} "
            msg += f"because node {node.instance_name} is predicted to be READY faster than node"
            msg += f"{node_to_replace.instance_name} would (num standby nodes reached faster).\n"
            msg += f"Deleting node {node_to_replace.instance_name} and rebooting node "
            self.logger.log(msg + f"{node.instance_name}.")
            node.async_reboot()
            self.nodes.remove(node_to_replace)
            node_to_replace.async_delete()
            return

        # If this point is reached without having already returned, then that means this node cannot
        # be usefully reassigned, and should be deleted.
        msg = f"Deleting node {node.instance_name}, no useful way to reassign this node was found."
        self.logger.log(msg)
        self.nodes.remove(node)
        node.async_delete()

    def reconcile(self):
        """
        Modify cluster such that: current state -> correct/optimal state.
        Every cluster operation (adding/deleting/assigning nodes) has a non 100% chance of success.
        To make sure the cluster works when actions fail, we CONSTANTLY check what the state
        should be then adjust things accordingly (what this function does).
        """

        if self.reconciling:
            return
        else:
            self.reconciling = True

        # Get list of burla nodes from GCE
        node_info_from_gce = []
        for zone, instances_scope in self.instance_client.aggregated_list(project=PROJECT_ID):
            vms = getattr(instances_scope, "instances", [])
            nodes = [(i.name, zone, i.status) for i in vms if "burla-cluster-node" in i.tags.items]
            node_info_from_gce.extend(nodes)
        # Get updated list of nodes from DB
        # updated_cluster = self.__class__.from_database(self.db, self.logger, self.background_tasks)
        # self.nodes = updated_cluster.nodes
        # self.jobs = updated_cluster.jobs

        # 1. Delete nodes that are in GCE but not in the DB.
        nodes_from_db = [node.instance_name for node in self.nodes]
        for node_name, zone, status in node_info_from_gce:
            node_not_deleting_or_starting = status not in ["STOPPING", "TERMINATED", "PROVISIONING"]
            if (node_name not in nodes_from_db) and node_not_deleting_or_starting:
                zone = zone.split("/")[1]
                msg = f"Deleting node {node_name} because it is {status} and NOT IN THE DB!"
                self.logger.log(msg)
                try:
                    # not async to prevent `NotFound` errors (`.reconcile` is called constantly).
                    self.instance_client.delete(project=PROJECT_ID, zone=zone, instance=node_name)
                except NotFound:
                    pass

        # 2. Delete nodes that are in DB but not in GCE.
        nodes_from_gce = [node_name for node_name, _, _ in node_info_from_gce]
        for node in self.nodes:
            time_since_boot = datetime.now(TZ) - node.started_booting_at
            gce_vm_should_exist_by_now = time_since_boot > timedelta(seconds=30 * 1)
            gce_vm_is_missing = node.instance_name not in nodes_from_gce
            if gce_vm_is_missing and gce_vm_should_exist_by_now:
                msg = f"Deleting node {node.instance_name} because it is not in GCE yet "
                msg += f"and it started booting {time_since_boot.total_seconds()}s ago."
                self.logger.log(msg)
                node.async_delete()
                self.nodes.remove(node)

        # 3. Delete all failed nodes.
        for node in self.nodes:
            if node.status() == "FAILED":
                self.logger.log(f"Deleting node: {node.instance_name} because it has FAILED")
                node.async_delete()
                self.nodes.remove(node)

        # 4. Delete all nodes that have been stuck booting for too long.

        # 5. Check that the cluster does or will match the specified default configuration.
        #
        # This works too well and adds nodes before the first node can be added to the db
        #
        # config = self.db.collection("cluster_config").document("cluster_config").get().to_dict()
        # for spec in config["Nodes"]:
        #     standby_nodes = [n for n in self.nodes if n.current_job is None]
        #     standby_nodes = [n for n in standby_nodes if n.machine_type == spec["machine_type"]]

        #     # not enough of this machine_type on standby ? (add more standby nodes ?)
        #     if len(standby_nodes) < spec["quantity"]:
        #         node_deficit = spec["quantity"] - len(standby_nodes)
        #         for _ in range(node_deficit):
        #             containers = [Container.from_dict(c) for c in spec["containers"]]
        #             self.add_node_async(machine_type=spec["machine_type"], containers=containers)
        #     # too many of this machine_type on standby ?  (remove some standby nodes ?)
        #     elif len(standby_nodes) > spec["quantity"]:
        #         nodes_to_remove = sorted(standby_nodes, key=lambda n: n.time_until_booted())
        #         num_extra_nodes = len(standby_nodes) - spec["quantity"]
        #         nodes_to_remove = nodes_to_remove[-num_extra_nodes:]

        #         for node in nodes_to_remove:
        #             if node.time_until_booted() > TOTAL_REBOOT_TIME:
        #                 node.async_delete()  # no use reassigning if boot time is high.
        #             else:
        #                 node.reassign_or_remove_node()

        # 6. Check that none of the current jobs are done, failed, or not being worked on.
        #    (they should be marked as having ended)
        for job in self.jobs:
            nodes_assigned_to_job = [node for node in self.nodes if node.current_job == job.id]
            nodes_working_on_job = [n for n in nodes_assigned_to_job if n.status() == "RUNNING"]

            if not nodes_assigned_to_job:
                # mark job as ended
                job_ref = self.db.collection("jobs").document(job.id)
                job_ref.update({"ended_at": SERVER_TIMESTAMP})
            elif nodes_assigned_to_job and not nodes_working_on_job:
                # state of these nodes should be one of: please_reboot, booting, ready?
                # Nodes should have ultimatums, eg:
                #    Be in state X within Y amount of time or your state is set to: "FAILED"
                pass

            # any_failed = False
            # all_done = True
            # for node in nodes_working_on_job:
            #     job_status = node.job_status(job_id=job.id)
            #     any_failed = job_status["any_subjobs_failed"]
            #     all_done = job_status["all_subjobs_done"]
            #     node_is_done = any_failed or job_status["all_subjobs_done"]

            #     if node_is_done and node.delete_when_done:
            #         node.async_delete()
            #         self.nodes.remove(node)
            #     elif node_is_done:
            #         add_logged_background_task(
            #             self.background_tasks, self.logger, self.reassign_or_remove_node, node
            #         )

            # if any_failed or all_done:
            #     self._remove_job_from_cluster_state_in_db(job.id)
            # if any_failed:
            #     return "FAILED"
            # if all_done:
            #     return "DONE"

            # return "RUNNING"

        # 7. Check that all jobs do or will match the target level of parallelism.

        self.reconciling = False
