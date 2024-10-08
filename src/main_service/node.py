"""
This needs to be run once per project or the port 8080 will not be open!
```
from google.cloud.compute_v1 import Firewall, FirewallsClient, Allowed
firewall = Firewall(
    name="burla-cluster-node-firewall",
    allowed=[Allowed(I_p_protocol="tcp", ports=["8080"])],
    direction="INGRESS",
    network="global/networks/default",
    target_tags=["burla-cluster-node"],
)
FirewallsClient().insert(project=PROJECT_ID, firewall_resource=firewall).result()
```

The disk image was built by creating a blank debian-12 instance then running the following:
(basically just installs git, docker, and gcloud, and authenticates docker using gcloud.)
```
apt-get update && apt-get install -y git ca-certificates curl gnupg
apt install -y python3-pip

install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/debian/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg
chmod a+r /etc/apt/keyrings/docker.gpg
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/debian \
  $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
  tee /etc/apt/sources.list.d/docker.list > /dev/null
apt-get update
apt-get install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

# use gcloud (should be installed) it to authenticate docker with GAR
gcloud auth configure-docker us-docker.pkg.dev

# install latest node service and pip install packages for faster node starts (less to install)
git clone --depth 1 --branch ??? https://github.com/Burla-Cloud/node_service.git
# pull latest container service image for faster node starts (less to download)
docker pull us-docker.pkg.dev/<YOUR PROJECT HERE>/burla-job-containers/default/image-nogpu:???
```
"""

import os
import json
import requests
from dataclasses import dataclass, asdict
from requests.exceptions import ConnectionError, ConnectTimeout, Timeout
from time import sleep, time
from uuid import uuid4
from typing import Optional, Callable

from google.api_core.exceptions import NotFound, ServiceUnavailable, Conflict
from google.cloud import firestore
from google.cloud.firestore import Increment
from google.cloud.compute_v1 import (
    AttachedDisk,
    NetworkInterface,
    AttachedDiskInitializeParams,
    Metadata,
    Items,
    AccessConfig,
    ServiceAccount,
    Tags,
    InstancesClient,
    Instance,
    Scheduling,
)

from main_service import PROJECT_ID, IN_PROD
from main_service.helpers import Logger


@dataclass
class Container:
    image: int
    python_executable: str
    python_version: str

    @classmethod
    def from_dict(cls, _dict: dict):
        return cls(
            image=_dict["image"],
            python_executable=_dict["python_executable"],
            python_version=_dict["python_version"],
        )

    def to_dict(self):
        return asdict(self)


class JustifiedDeletion(Exception):
    # Thrown if node is correctly deleted mid startup
    pass


class UnjustifiedDeletion(Exception):
    # Thrown if node is incorrectly deleted mid startup
    pass


# This is 100% guessed, is used for unimportant estimates / ranking
TOTAL_BOOT_TIME = 60
TOTAL_REBOOT_TIME = 30

# default compute engine svc account
if IN_PROD:
    GCE_DEFAULT_SVC = "1057122726382-compute@developer.gserviceaccount.com"
else:
    PROJECT_NUM = os.environ["PROJECT_NUM"]
    GCE_DEFAULT_SVC = f"{PROJECT_NUM}-compute@developer.gserviceaccount.com"

DEFAULT_DISK_IMAGE = "projects/burla-prod/global/images/burla-cluster-node-image-5"

NODE_START_TIMEOUT = 60 * 2
NODE_SVC_PORT = "8080"
ACCEPTABLE_ZONES = ["us-central1-b", "us-central1-c", "us-central1-f", "us-central1-a"]
NODE_SVC_VERSION = "0.8.4"  # <- this maps to a git tag/release or branch


class Node:
    """
    TODO: Error not thrown when `start` called with accellerator optimized machine type ??
    """

    def __init__(self):
        # Prevents instantiation of nodes that do not exist.
        err_msg = "Please use `Node.start`, `Node.start_and_execute`, or `Node.from_previous_state`"
        raise NotImplementedError(err_msg)

    @classmethod
    def _init(
        cls,
        db: firestore.Client,
        logger: Logger,
        add_background_task: Callable,
        machine_type: str,
        started_booting_at: int,
        finished_booting_at: Optional[int] = None,
        inactivity_shutdown_time_sec: Optional[int] = None,
        instance_name: Optional[str] = None,
        containers: Optional[list[Container]] = None,
        host: Optional[str] = None,
        zone: Optional[str] = None,
        current_job: Optional[str] = None,
        parallelism: Optional[int] = None,
        instance_client: Optional[InstancesClient] = None,
        delete_when_done: bool = False,
        is_booting=False,
        is_deleting=False,
    ):
        self = cls.__new__(cls)
        self.db = db
        self.logger = logger
        self.add_background_task = add_background_task
        self.instance_name = f"burla-node-{uuid4().hex}" if instance_name is None else instance_name
        self.machine_type = machine_type
        self.containers = containers
        self.started_booting_at = started_booting_at
        self.finished_booting_at = finished_booting_at
        self.inactivity_shutdown_time_sec = inactivity_shutdown_time_sec
        self.host = host
        self.zone = zone
        self.current_job = current_job
        self.parallelism = parallelism
        self.instance_client = instance_client if instance_client else InstancesClient()
        self.delete_when_done = delete_when_done
        self.is_deleting = is_deleting
        self.is_booting = is_booting
        self._deleted = False
        return self

    @classmethod
    def from_previous_state(
        cls,
        db: firestore.Client,
        logger: Logger,
        add_background_task: Callable,
        instance_name: str,
        machine_type: str,
        delete_when_done: bool,
        containers: Optional[list[Container]] = None,
        started_booting_at: Optional[int] = None,  # time NODE (NOT VM) started booting
        finished_booting_at: Optional[int] = None,  # time NODE (NOT VM) finished booting
        inactivity_shutdown_time_sec: Optional[int] = None,
        host: Optional[str] = None,
        zone: Optional[str] = None,
        current_job: Optional[str] = None,
        parallelism: Optional[int] = None,
        instance_client: Optional[InstancesClient] = None,
        is_deleting=None,
        is_booting=None,
    ):
        if (finished_booting_at is not None) and (not host or not zone):
            raise ValueError("host and zone required for running nodes")

        return cls._init(
            db=db,
            logger=logger,
            add_background_task=add_background_task,
            instance_name=instance_name,
            machine_type=machine_type,
            containers=containers,
            started_booting_at=started_booting_at,
            finished_booting_at=finished_booting_at,
            inactivity_shutdown_time_sec=inactivity_shutdown_time_sec,
            delete_when_done=delete_when_done,
            host=host,
            zone=zone,
            current_job=current_job,
            parallelism=parallelism,
            instance_client=instance_client,
            is_deleting=is_deleting,
            is_booting=is_booting,
        )

    @classmethod
    def start(
        cls,
        db: firestore.Client,
        logger: Logger,
        add_background_task: Callable,
        machine_type: str,
        instance_name: Optional[str] = None,
        containers: Optional[list[Container]] = None,
        inactivity_shutdown_time_sec: Optional[int] = None,
        disk_image: str = DEFAULT_DISK_IMAGE,
        disk_size: int = 10,  # <- (Gigabytes) minimum is 10 due to disk image
        instance_client: Optional[InstancesClient] = None,
    ):
        self = cls._init(
            db=db,
            logger=logger,
            add_background_task=add_background_task,
            instance_name=instance_name,
            machine_type=machine_type,
            containers=containers,
            inactivity_shutdown_time_sec=inactivity_shutdown_time_sec,
            started_booting_at=time(),
            instance_client=instance_client,
        )
        self.is_booting = True
        self.update_state_in_db()
        self.__start(disk_image=disk_image, disk_size=disk_size)
        return self

    def time_until_booted(self):
        time_spent_booting = time() - self.started_booting_at
        time_until_booted = TOTAL_BOOT_TIME - time_spent_booting
        return max(0, time_until_booted)

    def update_state_in_db(self):
        node_ref = self.db.collection("nodes").document(self.instance_name)
        node = node_ref.get().to_dict()
        node_previously_deleted = (node or {}).get("deleted_at")
        node_just_deleted = self._deleted
        node_is_new = node is None

        current_state = self.to_dict()
        current_status = current_state["status"]

        if node_previously_deleted:
            return
        elif node_just_deleted:
            current_state["deleted_at"] = time()

        if node_is_new:
            node_ref.set(current_state)
            previous_status = current_status
        else:
            node_ref.update(current_state)
            previous_status = node["status"]

        if self.current_job != None:
            job_ref = self.db.collection("jobs").document(self.current_job)
            job_is_not_done = not job_ref.get().to_dict().get("ended_at")

            if job_is_not_done and self.parallelism:
                if previous_status != "RUNNING" and current_status == "RUNNING":
                    job_ref.update({"current_parallelism": Increment(self.parallelism)})
                    msg = f"Node {self.instance_name} now working on job {self.current_job}, "
                    msg += f"parallelism increased by {self.parallelism}"
                    self.logger.log(msg)
                elif previous_status == "RUNNING" and current_status != "RUNNING":
                    job_ref.update({"current_parallelism": Increment(-self.parallelism)})
                    msg = f"Node {self.instance_name} no longer working on job {self.current_job}, "
                    msg += f"parallelism decreased by {self.parallelism}"
                    self.logger.log(msg)

    def to_dict(self):
        return dict(
            instance_name=self.instance_name,
            status=self.status(),
            host=self.host,
            zone=self.zone,
            machine_type=self.machine_type,
            current_job=self.current_job,
            parallelism=self.parallelism,
            containers=[container.to_dict() for container in self.containers] or None,
            inactivity_shutdown_time_sec=self.inactivity_shutdown_time_sec,
            delete_when_done=self.delete_when_done,
            started_booting_at=self.started_booting_at,
            finished_booting_at=self.finished_booting_at,
            is_booting=self.is_booting,
            is_deleting=self.is_deleting,
            deleted_at=None,
        )

    def status(self, timeout=1, timeout_remaining=None):
        """
        Returns one of: `BOOTING`, `RUNNING`, `READY`, `FAILED`, `DELETING`.
        """
        start = time()
        timeout_remaining = timeout if timeout_remaining is None else timeout_remaining
        has_timed_out = timeout_remaining < 0
        status = None

        if self.host is not None:
            try:
                response = requests.get(f"{self.host}/", timeout=1)
                response.raise_for_status()
                status = response.json()["status"]
            except (ConnectionError, ConnectTimeout, Timeout):
                pass

        status = "BOOTING" if self.is_booting and status is None else status
        status = "DELETING" if self.is_deleting else status

        if has_timed_out:
            # (service theoretically should have started and responded by now)
            self.logger.log(f"STATUS TIMEOUT after {timeout}s: {self.instance_name} is FAILED")
            status = "FAILED"

        if status is None:
            sleep(2)
            elapsed_time = time() - start
            return self.status(timeout=timeout, timeout_remaining=timeout_remaining - elapsed_time)
        else:
            return status

    def __start(self, disk_image: str, disk_size: int):
        disk_params = AttachedDiskInitializeParams(source_image=disk_image, disk_size_gb=disk_size)
        disk = AttachedDisk(auto_delete=True, boot=True, initialize_params=disk_params)

        network_name = "global/networks/default"
        access_config = AccessConfig(name="External NAT", type="ONE_TO_ONE_NAT")
        network_interface = NetworkInterface(name=network_name, access_configs=[access_config])

        scheduling = Scheduling(provisioning_model="SPOT", instance_termination_action="DELETE")

        access_anything_scope = "https://www.googleapis.com/auth/cloud-platform"
        service_account = ServiceAccount(email=GCE_DEFAULT_SVC, scopes=[access_anything_scope])

        startup_script = self.__get_startup_script()
        shutdown_script = self.__get_shutdown_script()
        startup_script_metadata = Items(key="startup-script", value=startup_script)
        shutdown_script_metadata = Items(key="shutdown-script", value=shutdown_script)
        for zone in ACCEPTABLE_ZONES:
            try:
                instance = Instance(
                    name=self.instance_name,
                    machine_type=f"zones/{zone}/machineTypes/{self.machine_type}",
                    disks=[disk],
                    network_interfaces=[network_interface],
                    service_accounts=[service_account],
                    metadata=Metadata(items=[startup_script_metadata, shutdown_script_metadata]),
                    tags=Tags(items=["burla-cluster-node"]),
                    scheduling=scheduling,
                )
                self.instance_client.insert(
                    project=PROJECT_ID, zone=zone, instance_resource=instance
                ).result()
                instance_created = True
                break

            except ServiceUnavailable:  # <- not enough instances in this zone.
                instance_created = False
            except Conflict:  # <- means vm was deleted while starting.
                node = self.db.collection("nodes").document(self.instance_name).get().to_dict()
                node_is_deleting = (node or {}).get("is_deleting") == True
                node_was_deleted = (node or {}).get("deleted_at")
                deletion_is_justified = node and (node_is_deleting or node_was_deleted)

                # when is "deletion justified" ?
                # sometimes master svc will realize it doesent need a node just after requesting it
                # example: job needs more parallelism, then finishes before new node is ready
                if deletion_is_justified:
                    raise JustifiedDeletion(f"Node {self.instance_name} deleted while starting.")
                else:
                    msg = f"UNJUSTIFIED DELETION: Node {self.instance_name} deleted while starting."
                    raise UnjustifiedDeletion(msg)

        if not instance_created:
            raise Exception(f"Unable to provision {instance} in any of: {ACCEPTABLE_ZONES}")

        instance = self.instance_client.get(
            project=PROJECT_ID, zone=zone, instance=self.instance_name
        )
        external_ip = instance.network_interfaces[0].access_configs[0].nat_i_p

        self.host = f"http://{external_ip}:{NODE_SVC_PORT}"
        self.zone = zone

        start = time()
        status = self.status()
        while status != "READY":
            sleep(1)
            booting_too_long = (time() - start) > 60 * 3
            status = self.status()

            if status == "FAILED" or booting_too_long:
                try:
                    kwargs = dict(project=PROJECT_ID, zone=self.zone, instance=self.instance_name)
                    self.instance_client.delete(**kwargs)
                except (NotFound, ValueError):
                    pass
                self._deleted = True
                msg = f"Node {self.instance_name} Failed to start! (timeout={booting_too_long})"
                raise Exception(msg)

        self.is_booting = False
        self.finished_booting_at = time()
        self.update_state_in_db()

    def __get_startup_script(self):
        return f"""
        #! /bin/bash
        # This script installs and starts the node service
        
        # Increases max num open files so we can have more connections open.
        # ulimit -n 4096 # baked into image when I built `burla-cluster-node-image-5`

        gcloud config set account {GCE_DEFAULT_SVC}

        git clone --depth 1 --branch {NODE_SVC_VERSION} https://github.com/Burla-Cloud/node_service.git
        cd node_service
        python3.11 -m pip install --break-system-packages .
        echo "Done installing packages."

        export IN_PROD="{IN_PROD}"
        export INSTANCE_NAME="{self.instance_name}"
        export PROJECT_ID="{PROJECT_ID}"
        export CONTAINERS='{json.dumps([c.to_dict() for c in self.containers])}'
        export INACTIVITY_SHUTDOWN_TIME_SEC="{self.inactivity_shutdown_time_sec}"

        python3.11 -m uvicorn node_service:app --host 0.0.0.0 --port 8080 --workers 1 --timeout-keep-alive 600
        """

    def __get_shutdown_script(self):
        firestore_base_url = "https://firestore.googleapis.com"
        firestore_db_url = f"{firestore_base_url}/v1/projects/{PROJECT_ID}/databases/(default)"
        firestore_document_url = f"{firestore_db_url}/documents/nodes/{self.instance_name}"
        return f"""
        #! /bin/bash
        # This script marks the node as "DELETED" in the database when the vm instance is shutdown.
        # This is necessary due to situations where instances are preempted,
        # otherwise the `main_service` doesn't know which vm's are still running when starting a job,
        # checking if they are still running is too slow, increasing latency.

        # record environment variable indicating whether this instance was preempted.
        preempted_instances_matching_filter=$( \
            gcloud compute operations list \
            --filter="operationType=compute.instances.preempted AND targetLink:instances/{self.instance_name}" \
        )
        # Set PREEMPTED to true if the output is non-empty, otherwise false
        export PREEMPTED=$([ -n "$preempted_instances_matching_filter" ] && echo true || echo false)

        curl -X PATCH \
        -H "Authorization: Bearer $(gcloud auth print-access-token)" \
        -H "Content-Type: application/json" \
        -d '{{
            "fields": {{
                "status": {{
                    "stringValue": "DELETED"
                }},
                "preempted": {{
                    "booleanValue": '"$PREEMPTED"'
                }}
            }}
        }}' \
        "{firestore_document_url}?updateMask.fieldPaths=status&updateMask.fieldPaths=preempted"
        """
