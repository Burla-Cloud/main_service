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

# install gcloud and use it to authenticate docker with GAR also
```
"""

import os
import json
import requests
from dataclasses import dataclass, asdict
from requests.exceptions import ConnectionError, ConnectTimeout, Timeout
from time import sleep, time
from uuid import uuid4
from typing import Optional
from datetime import datetime, timedelta

from fastapi import BackgroundTasks
from google.api_core.exceptions import NotFound, ServiceUnavailable, Conflict
from google.cloud import firestore
from google.cloud.firestore import SERVER_TIMESTAMP, Increment
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
)

from main_service import PROJECT_ID, TZ, IN_DEV
from main_service.helpers import get_secret, Logger, add_logged_background_task


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


# This was guessed
TOTAL_BOOT_TIME = timedelta(seconds=60 * 3)
TOTAL_REBOOT_TIME = timedelta(seconds=60 * 1)

# default compute engine svc account
if IN_DEV:
    GCE_DEFAULT_SVC = "140225958505-compute@developer.gserviceaccount.com"
else:
    GCE_DEFAULT_SVC = "1057122726382-compute@developer.gserviceaccount.com"


NODE_START_TIMEOUT = 60 * 5
NODE_SVC_PORT = "8080"
ACCEPTABLE_ZONES = ["us-central1-a", "us-central1-b", "us-central1-c", "us-central1-f"]
NODE_SVC_VERSION = "test"  # "v0.1.37"  # <- this maps to a git tag /  github release


def get_startup_script(instance_name: str):
    return f"""
    #! /bin/bash
    # This script installs and starts the node service 
    # This script uses git instead of the github api because the github api SUCKS

    # Increases max num open files so we can have more connections open.
    # ulimit -n 4096

    # METADATA_SVC_HOST="http://metadata.google.internal"
    # PRIVATE_KEY_URL="$METADATA_SVC_HOST/computeMetadata/v1/instance/attributes/ssh-private-key"
    # curl $PRIVATE_KEY_URL -H "Metadata-Flavor: Google" > /root/.ssh/id_rsa
    # chmod 600 ~/.ssh/id_rsa

    # eval "$(ssh-agent -s)"
    # ssh-add /root/.ssh/id_rsa

    # This needs to be here, I can't figure out how to remove it from the image.
    rm -rf node_service

    # export GIT_SSH_COMMAND="ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no"
    git clone --depth 1 --branch {NODE_SVC_VERSION} git@github.com:Burla-Cloud/node_service.git
    # git clone --depth 1 git@github.com:Burla-Cloud/node_service.git
    cd node_service
    python3.11 -m pip install --break-system-packages .

    export IN_PRODUCTION="{os.environ.get('IN_PRODUCTION')}"
    export INSTANCE_NAME="{instance_name}"
    python3.11 -m uvicorn node_service:app --host 0.0.0.0 --port 8080 --workers 1 --timeout-keep-alive 600
    """


class Node:
    """
    This class is designed to be called only by the `main_service.cluster.Cluster` class.

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
        background_tasks: BackgroundTasks,
        machine_type: str,
        started_booting_at: datetime,
        instance_name: Optional[str] = None,
        containers: Optional[list[Container]] = None,
        finished_booting_at: Optional[datetime] = None,
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
        self.background_tasks = background_tasks
        self.instance_name = f"burla-node-{uuid4().hex}" if instance_name is None else instance_name
        self.machine_type = machine_type
        self.containers = containers
        self.started_booting_at = started_booting_at
        self.finished_booting_at = finished_booting_at
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
        background_tasks: BackgroundTasks,
        instance_name: str,
        machine_type: str,
        delete_when_done: bool,
        containers: Optional[list[Container]] = None,
        started_booting_at: Optional[datetime] = None,  # time NODE (NOT VM) started booting
        finished_booting_at: Optional[datetime] = None,  # time NODE (NOT VM) finished booting
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
            background_tasks=background_tasks,
            instance_name=instance_name,
            machine_type=machine_type,
            containers=containers,
            started_booting_at=started_booting_at,
            finished_booting_at=finished_booting_at,
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
    def start_and_execute(
        cls,
        db: firestore.Client,
        logger: Logger,
        background_tasks: BackgroundTasks,
        machine_type: str,
        job_id: str,
        parallelism: int,
        function_pkl: Optional[bytes] = None,
        instance_name: Optional[str] = None,
        containers: Optional[list[Container]] = None,
        delete_when_done: bool = False,
        disk_image: str = "global/images/burla-cluster-node-image-4",
        disk_size: int = 1000,  # <- (Gigabytes) minimum is 1000 due to disk image
        instance_client: Optional[InstancesClient] = None,
    ):
        """
        TODO: Node should only start the exact containers it needs in this situation instead
        of all of them. This will dramatically cut startup time.
        """
        self = cls._init(
            db=db,
            logger=logger,
            background_tasks=background_tasks,
            instance_name=instance_name,
            machine_type=machine_type,
            containers=containers,
            started_booting_at=datetime.now(TZ),
            current_job=job_id,
            parallelism=parallelism,
            delete_when_done=delete_when_done,
            instance_client=instance_client,
        )
        self.is_booting = True
        self.update_state_in_db()
        self.logger.log(f"node {instance_name} written to db.")
        self._start(disk_image=disk_image, disk_size=disk_size)
        self.execute(
            job_id=self.current_job,
            parallelism=parallelism,
            function_pkl=function_pkl,
            delete_when_done=self.delete_when_done,
        )
        return self

    @classmethod
    def start(
        cls,
        db: firestore.Client,
        logger: Logger,
        background_tasks: BackgroundTasks,
        machine_type: str,
        instance_name: Optional[str] = None,
        containers: Optional[list[Container]] = None,
        disk_image: str = "global/images/burla-cluster-node-image-4",
        disk_size: int = 1000,  # <- (Gigabytes) minimum is 1000 due to disk image
        instance_client: Optional[InstancesClient] = None,
    ):
        self = cls._init(
            db=db,
            logger=logger,
            background_tasks=background_tasks,
            instance_name=instance_name,
            machine_type=machine_type,
            containers=containers,
            started_booting_at=datetime.now(TZ),
            instance_client=instance_client,
        )
        self.is_booting = True
        self.update_state_in_db()
        self._start(disk_image=disk_image, disk_size=disk_size)
        return self

    def time_until_booted(self):
        time_spent_booting = datetime.now(TZ) - self.started_booting_at
        time_until_booted = TOTAL_BOOT_TIME - time_spent_booting
        return max(0, time_until_booted)

    def _start(self, disk_image: str, disk_size: int):
        disk_params = AttachedDiskInitializeParams(source_image=disk_image, disk_size_gb=disk_size)
        disk = AttachedDisk(auto_delete=True, boot=True, initialize_params=disk_params)

        network_name = "global/networks/default"
        access_config = AccessConfig(name="External NAT", type="ONE_TO_ONE_NAT")
        network_interface = NetworkInterface(name=network_name, access_configs=[access_config])

        access_anything_scope = "https://www.googleapis.com/auth/cloud-platform"
        service_account = ServiceAccount(email=GCE_DEFAULT_SVC, scopes=[access_anything_scope])

        startup_script = get_startup_script(self.instance_name)
        startup_script_metadata = Items(key="startup-script", value=startup_script)
        ssh_key_metadata = Items(key="ssh-private-key", value=get_secret("deploybot-private-key"))
        for zone in ACCEPTABLE_ZONES:
            try:
                instance = Instance(
                    name=self.instance_name,
                    machine_type=f"zones/{zone}/machineTypes/{self.machine_type}",
                    disks=[disk],
                    network_interfaces=[network_interface],
                    service_accounts=[service_account],
                    metadata=Metadata(items=[startup_script_metadata, ssh_key_metadata]),
                    tags=Tags(items=["burla-cluster-node"]),
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

        status = self.status(timeout=NODE_START_TIMEOUT)  # <- won't return until service is up
        while status != "READY":
            if status == "FAILED":
                self.delete()
                raise Exception(f"Node {self.instance_name} Failed to start!")
            elif status not in ["BOOTING", "BOOTING", "PLEASE_REBOOT"]:
                raise Exception(f"UNEXPECTED STATE WHILE BOOTING: {status}")
            elif status == "PLEASE_REBOOT":
                self.reboot()  # <- node doesn't boot containers automatically
            sleep(5)
            status = self.status()

        self.is_booting = False
        self.finished_booting_at = datetime.now(TZ)
        self.update_state_in_db()

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
            current_state["deleted_at"] = SERVER_TIMESTAMP

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
            delete_when_done=self.delete_when_done,
            started_booting_at=self.started_booting_at,
            finished_booting_at=self.finished_booting_at,
            is_booting=self.is_booting,
            is_deleting=self.is_deleting,
            deleted_at=None,
        )

    def execute(
        self,
        job_id: str,
        parallelism: int,
        function_pkl: Optional[bytes] = None,
        delete_when_done: bool = False,
    ):
        self.delete_when_done = delete_when_done  # <- actual deletion handled in Cluster.status
        self.current_job = job_id

        payload = {"parallelism": parallelism}
        if function_pkl:
            files = dict(function_pkl=function_pkl)
            data = dict(request_json=json.dumps(payload))
            response = requests.post(f"{self.host}/jobs/{job_id}", files=files, data=data)
        else:
            response = requests.post(f"{self.host}/jobs/{job_id}", json=payload)
        response.raise_for_status()

        self.parallelism = parallelism
        self.update_state_in_db()

    def reboot(self):
        if self.status() != "BOOTING":
            containers_json = [container.to_dict() for container in self.containers]
            response = requests.post(f"{self.host}/reboot", json=containers_json)
            response.raise_for_status()
            self.is_booting = True

        # confirm node is rebooting
        status = self.status()
        if status not in ["BOOTING", "READY"]:
            raise Exception(f"Node {self.instance_name} failed start rebooting! status={status}")

        # poll status until done rebooting:
        while status != "READY":
            if status == "FAILED":
                self.delete()
                raise Exception(f"Node {self.instance_name} Failed to start!")
            elif status != ["BOOTING"]:
                raise Exception(f"UNEXPECTED STATE WHILE BOOTING: {status}")
            else:
                sleep(5)
                status = self.status()
        self.is_booting = False

    def async_reboot(self):
        add_logged_background_task(self.background_tasks, self.logger, self.reboot)

    def reboot_and_execute(
        self,
        job_id: str,
        parallelism: int,
        function_pkl: Optional[bytes] = None,
        delete_when_done: bool = False,
    ):
        if self.status() != "READY":  # <- if status is "READY" node must have already rebooted.
            self.reboot()
        self.execute(
            job_id=job_id,
            parallelism=parallelism,
            function_pkl=function_pkl,
            delete_when_done=delete_when_done,
        )

    def async_reboot_and_execute(self, *a, **kw):
        add_logged_background_task(
            self.background_tasks, self.logger, self.reboot_and_execute, *a, **kw
        )

    def status(self, timeout=1, timeout_remaining=None):
        """
        Returns one of: `PLEASE_REBOOT`, `BOOTING`, `RUNNING`, `READY`, `FAILED`, `DELETING`.
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

    def job_status(self, job_id: str):
        """Returns: `all_subjobs_done: bool, any_subjobs_failed: bool`"""
        response = requests.get(f"{self.host}/jobs/{job_id}")
        response.raise_for_status()
        job_status = response.json()

        if job_status["all_subjobs_done"] or job_status["any_subjobs_failed"]:
            self.update_state_in_db()

        return job_status

    def delete(self):
        """
        TODO:
        Should "gracefully" stop any subjob executors first, to prevent half/executed subjobs.
        ^^ is not urgent because nodes almost always not deleted while executing ??
        """
        self.is_deleting = True
        try:
            self.instance_client.delete(
                project=PROJECT_ID, zone=self.zone, instance=self.instance_name
            )
        except (NotFound, ValueError):
            pass

        self._deleted = True
        self.update_state_in_db()

    def async_delete(self):
        self.is_deleting = True
        self.update_state_in_db()
        add_logged_background_task(self.background_tasks, self.logger, self.delete)
