from typing import List, Optional
from concurrent.futures import ThreadPoolExecutor
from copy import copy
from dataclasses import dataclass
from datetime import datetime

from fastapi import BackgroundTasks
from google.cloud import firestore
from google.cloud.firestore import SERVER_TIMESTAMP
from google.cloud.compute_v1 import InstancesClient

from main_service import PROJECT_ID, TZ
from main_service.node import Node, TOTAL_BOOT_TIME, TOTAL_REBOOT_TIME
from main_service.helpers import Logger


def parallelism_capacity(machine_type: str, func_cpu: int, func_ram: int, gpu: bool):
    """What is the maximum number of parallel subjobs this machine_type can run a job with the
    following resource requirements at?
    """
    if not machine_type in ["a2-highgpu-1g", "n1-standard-96"]:
        raise ValueError(f"machine_type must be in: {['a2-highgpu-1g', 'n1-standard-96']}")
    if gpu:
        return 1
    else:
        return min(96 // func_cpu, 360 // func_ram)


@dataclass
class Job:
    id: str
    current_parallelism: int
    target_parallelism: int

    def to_dict(self):
        return dict(
            id=self.job_id,
            current_parallelism=self.current_parallelism,
            target_parallelism=self.target_parallelism,
        )


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
        self._reconciling = False

    @classmethod
    def from_database(
        cls,
        db: firestore.Client,
        logger: Logger,
        background_tasks: BackgroundTasks,
        instance_client: Optional[InstancesClient] = None,
    ):
        instance_client = instance_client if instance_client else InstancesClient()

        collection = db.collection("current_cluster")
        ordered_collection = collection.order_by("timestamp", direction=firestore.Query.DESCENDING)
        current_cluster = ordered_collection.limit(1).get()[0].to_dict()

        self = cls(
            nodes=[],
            jobs=[],
            db=db,
            background_tasks=background_tasks,
            logger=logger,
            instance_client=instance_client,
        )
        if node_info["finished_booting_at"] != None:
            node_info["finished_booting_at"] = node_info["finished_booting_at"].ToDatetime()

        for node_info in current_cluster["Nodes"]:
            node = Node.from_previous_state(
                db=db,
                logger=logger,
                background_tasks=self.background_tasks,
                instance_name=node_info["instance_name"],
                machine_type=node_info["machine_type"],
                started_booting_at=node_info["started_booting_at"].ToDatetime(),
                finished_booting_at=node_info["finished_booting_at"],
                delete_when_done=node_info["delete_when_done"],
                host=node_info["host"],
                zone=node_info["zone"],
                current_job=node_info["current_job"],
                instance_client=instance_client,
            )
            self.nodes.append(node)
        for job_info in current_cluster["Jobs"]:
            job = Job(
                id=job_info["id"],
                current_parallelism=job_info["current_parallelism"],
                target_parallelism=job_info["target_parallelism"],
            )
            self.job.append(job)
        return self

    def _add_job_to_db(self, job_id, target_parallelism):
        """TODO: This is  a race condition. wrap in transaction ??"""
        collection = self.db.collection("current_cluster")
        ordered_collection = collection.order_by("timestamp", direction=firestore.Query.DESCENDING)
        current_cluster = ordered_collection.limit(1).get()[0].to_dict()

        if job_id in [job["id"] for job in current_cluster["Jobs"]]:
            raise Exception(f"job_id {job_id} already in database ?")

        new_job = Job(id=job_id, current_parallelism=0, target_parallelism=target_parallelism)
        updated_cluster_state = {
            "timestamp": SERVER_TIMESTAMP,
            "Jobs": current_cluster["Jobs"] + [new_job.to_dict()],
            "Nodes": current_cluster["Nodes"],
        }
        self.db.collection("current_cluster").add(updated_cluster_state)

    def restart(self, force=False):
        """Force means: do not wait for nodes to finish current job before deleting."""
        self.instance_client = InstancesClient() if self.instance_client is None else None

        for node in copy(self.nodes):
            if (force == False) and (node.status() == "BUSY"):
                node.delete_when_done = True
                node.update_state_in_db()
            else:
                node.async_delete(self.background_tasks)
                self.nodes.remove(node)

        config = self.db.collection("cluster_config").document("cluster_config").get().to_dict()
        machine_types = [n["machine_type"] for n in config["Nodes"] for _ in range(n["quantity"])]

        with ThreadPoolExecutor() as executor:
            list(executor.map(self.add_node, machine_types))

    def execute(self, job_id: str):
        job = self.db.collection("jobs").document(job_id).get().to_dict()
        func_cpu, func_ram, gpu = job["func_cpu"], job["func_ram"], job["gpu"]
        target_parallelism = job["parallelism"]
        current_parallelism = 0

        self._add_job_to_db(self, job_id, target_parallelism)

        # assign job to any compatible nodes.
        # start an extra unassigned node, for every assigned node to maintain same #"READY" nodes.
        for node in self.nodes:
            max_parallelism = parallelism_capacity(node.machine_type, func_cpu, func_ram, gpu)
            compatible_with_job = max_parallelism > 0
            is_ready = node.status() == "READY"

            if compatible_with_job and is_ready:
                parallelism = min([target_parallelism - current_parallelism, max_parallelism])
                node.execute(job_id=job_id, parallelism=parallelism, delete_when_done=True)
                self.add_node_async(node.machine_type)  # <- maintain same #"READY" nodes
                current_parallelism += parallelism

            if current_parallelism >= target_parallelism:
                break

        # start remaining machines necessary to achieve requested parallelism
        while current_parallelism < target_parallelism:
            machine_type = "a2-highgpu-1g" if gpu else "n1-standard-96"
            max_parallelism = parallelism_capacity(machine_type, func_cpu, func_ram, gpu)
            parallelism = min([target_parallelism - current_parallelism, max_parallelism])
            self.add_node_async(
                machine_type, pre_assign_job=job_id, parallelism=parallelism, delete_when_done=True
            )
            current_parallelism += max_parallelism

    def add_node(
        self,
        machine_type: str,
        pre_assign_job: Optional[str] = None,
        parallelism: Optional[int] = None,
        delete_when_done: Optional[bool] = False,
    ):
        if (pre_assign_job and not parallelism) or (parallelism and not pre_assign_job):
            raise ValueError("pre_assign_job and parallelism are mutually inclusive")
        elif pre_assign_job:
            node = Node.start_and_execute(
                db=self.db,
                logger=self.logger,
                background_tasks=self.background_tasks,
                machine_type=machine_type,
                job_id=pre_assign_job,
                parallelism=parallelism,
                delete_when_done=delete_when_done,
            )
        else:
            node = Node.start(
                db=self.db,
                logger=self.logger,
                background_tasks=self.background_tasks,
                machine_type=machine_type,
            )
        self.nodes.append(node)

    def add_node_async(self, *a, **kw):
        # allows service to respond while continuing to wait for node to start in the background.
        self.background_tasks.add_task(self.add_node, *a, **kw)

    def status(self, job_id: str):
        if not self._reconciling:
            self.background_tasks.add_task(self.reconcile)

        nodes_assigned_to_job = [node for node in self.nodes if node.current_job == job_id]
        nodes_working_on_job = [node for node in nodes_assigned_to_job if not node.is_starting]

        for node in nodes_working_on_job:
            job_status = node.job_status(job_id=job_id)
            any_failed = job_status["any_subjobs_failed"]
            all_done = job_status["all_subjobs_done"]
            done = any_failed or all_done

            if done and node.delete_when_done:
                node.async_delete(self.background_tasks)
                self.nodes.remove(node)
            elif done:
                self.background_tasks.add_task(self.reassign_or_remove_node, node)

            if any_failed:
                return "FAILED"
            if all_done:
                return "DONE"

        if nodes_assigned_to_job == []:
            raise Exception(f"Cannot get job status, ZERO nodes are assigned to job: {job_id}")
        else:
            return "RUNNING"

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
                node.async_reboot_and_execute(job_id=job.id, parallelism=parallelism_deficit)
                return
            # 1b. Can this node start working on `job` faster than any of the other assigned nodes ?
            #     If so, replace the slower node, this will increase parallelism for the user faster
            if highest_remaining_boot_time > TOTAL_REBOOT_TIME:
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
        replaceable_nodes = [n for n in self.nodes if n.current_job is None]
        replaceable_nodes = [n for n in replaceable_nodes if n.finished_booting_at == None]
        replaceable_nodes = [n for n in replaceable_nodes if n.machine_type == node.machine_type]

        # 2a. Will we have enough of these machines on standby? If not, add this machine.
        if len(replaceable_nodes) < target_num_replaceable_nodes:
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
            node.async_reboot()
            self.nodes.remove(node_to_replace)
            node_to_replace.async_delete()
            return

        # If this point is reached without having already returned, then that means this node cannot
        # be usefully reassigned, and should be deleted.
        self.nodes.remove(node)
        node.async_delete()

    def reconcile(self):
        """
        Modify cluster such that: current state -> correct/optimal state.
        Every cluster operation (adding/deleting/assigning nodes) has a non 100% chance of success.
        To make sure the cluster works when actions fail, we CONSTANTLY check what the state
        should be then adjust things accordingly (what this function does).
        """
        self._reconciling = True
        # update self with latest info from database
        self = self.from_database(
            cls=self.__class__,
            db=self.db,
            logger=self.logger,
            background_tasks=self.background_tasks,
            instance_client=self.instance_client,
        )
        # Get list of burla nodes from GCE
        nodes_and_zones_from_gce = []
        response = self.instance_client.aggregated_list(project=PROJECT_ID)
        for zone, instances_scope in response.items():
            instances = getattr(instances_scope, "instances", [])
            nodes = [(i.name, zone) for i in instances if "burla-cluster-node" in i.tags.items]
            nodes_and_zones_from_gce.extend(nodes)

        # 1. Delete nodes that are in GCE but not in the DB.
        nodes_from_db = [node.instance_name for node in self.nodes]
        for node_name, zone in nodes_and_zones_from_gce:
            if node_name not in nodes_from_db:
                # not async to prevent `NotFound` errors (`.reconcile` is called constantly).
                self.instance_client.delete(project=PROJECT_ID, zone=zone, instance=node_name)

        # 2. Delete nodes that are in DB but not in GCE.
        nodes_from_gce = [node_name for node_name, _ in nodes_and_zones_from_gce]
        for node in self.nodes:
            if node.instance_name not in nodes_from_gce:
                node.async_delete()
                self.nodes.remove(node)

        # 3. Delete all failed nodes.
        for node in self.nodes:
            if node.status() == "FAILED":
                node.async_delete()
                self.nodes.remove(node)

        # 4. Check that the cluster does or will match the specified default configuration.
        config = self.db.collection("cluster_config").document("cluster_config").get().to_dict()
        for spec in config["Nodes"]:
            standby_nodes = [n for n in self.nodes if n.current_job is None]
            standby_nodes = [n for n in standby_nodes if n.machine_type == spec["machine_type"]]

            # not enough of this machine_type on standby ? (add more standby nodes ?)
            if len(standby_nodes) < spec["quantity"]:
                node_deficit = spec["quantity"] - len(standby_nodes)
                for _ in range(node_deficit):
                    self.add_node_async(machine_type=spec["machine_type"])
            # too many of this machine_type on standby ?  (remove some standby nodes ?)
            elif len(standby_nodes) > spec["quantity"]:
                nodes_to_remove = sorted(standby_nodes, key=lambda n: n.time_until_booted())
                num_extra_nodes = len(standby_nodes) - spec["quantity"]
                nodes_to_remove = nodes_to_remove[-num_extra_nodes:]

                for node in nodes_to_remove:
                    if node.time_until_booted() > TOTAL_REBOOT_TIME:
                        node.async_delete()  # no use reassigning if boot time is high.
                    else:
                        node.reassign_or_remove_node()

        # 5. Check that none of the current jobs are done or failed.
        #    (they should not exist in the jobs list in the `cluster_state` collection)
        for job in self.jobs:
            nodes_assigned_to_job = [n for n in self.nodes if n.current_job == job.id]
            for node in nodes_assigned_to_job:
                job_status = node.job_status(job.id)

        # Check that all jobs do or will match the target level of parallelism.

        self._reconciling = False
