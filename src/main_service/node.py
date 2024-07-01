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
import requests
from dataclasses import dataclass, asdict
from requests.exceptions import ConnectionError, ConnectTimeout, Timeout
from time import sleep, time
from uuid import uuid4
from typing import Optional
from datetime import datetime, timedelta

from fastapi import BackgroundTasks
from google.api_core.exceptions import NotFound, ServiceUnavailable
from google.cloud import firestore
from google.cloud.firestore import SERVER_TIMESTAMP
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

from main_service import PROJECT_ID, TZ
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


# This was guessed
TOTAL_BOOT_TIME = timedelta(seconds=60 * 4)
TOTAL_REBOOT_TIME = timedelta(seconds=60 * 2)

# default compute engine svc account
GCE_DEFAULT_SVC = "140225958505-compute@developer.gserviceaccount.com"

NODE_START_TIMEOUT = 60 * 5
NODE_SVC_PORT = "8080"
ACCEPTABLE_ZONES = ["us-central1-a", "us-central1-b", "us-central1-c", "us-central1-f"]
NODE_SVC_VERSION = "test"  # "v0.1.37"  # <- this maps to a git tag /  github release
NODE_STARTUP_SCRIPT = f"""
#! /bin/bash
# This script installs and starts the node service 
# This script uses git instead of the github api because the github api SUCKS

# Increases max num open files so we can have more connections open.
ulimit -n 4096

METADATA_SVC_HOST="http://metadata.google.internal"
PRIVATE_KEY_URL="$METADATA_SVC_HOST/computeMetadata/v1/instance/attributes/ssh-private-key"
curl $PRIVATE_KEY_URL -H "Metadata-Flavor: Google" > /root/.ssh/id_rsa
chmod 600 ~/.ssh/id_rsa

eval "$(ssh-agent -s)"
ssh-add /root/.ssh/id_rsa

# This needs to be here, I can't figure out how to remove it from the image.
rm -rf node_service

export GIT_SSH_COMMAND="ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no"
git clone --depth 1 --branch {NODE_SVC_VERSION} git@github.com:Burla-Cloud/node_service.git
# git clone --depth 1 git@github.com:Burla-Cloud/node_service.git
cd node_service
python3.11 -m pip install --break-system-packages .

export IN_PRODUCTION="{os.environ.get('IN_PRODUCTION')}"
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
        instance_name: str,
        machine_type: str,
        started_booting_at: datetime,
        containers: Optional[list[Container]] = None,
        finished_booting_at: Optional[datetime] = None,
        host: Optional[str] = None,
        zone: Optional[str] = None,
        current_job: Optional[str] = None,
        parallelism: Optional[int] = None,
        instance_client: Optional[InstancesClient] = None,
        delete_when_done: bool = False,
        is_deleting=False,
        is_rebooting=False,
        is_booting=False,
    ):
        self = cls.__new__(cls)
        self.db = db
        self.logger = logger
        self.background_tasks = background_tasks
        self.instance_name = instance_name
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
        self.is_rebooting = is_rebooting
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
        is_rebooting=None,
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
            is_rebooting=is_rebooting,
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
            instance_name=f"burla-node-{uuid4().hex}",
            machine_type=machine_type,
            containers=containers,
            started_booting_at=datetime.now(TZ),
            current_job=job_id,
            parallelism=parallelism,
            delete_when_done=delete_when_done,
            instance_client=instance_client,
        )
        self.is_booting = True
        msg = f"Node (NOT THE VM INSTANCE) {self.instance_name}"
        msg += f" started booting at {self.started_booting_at}"
        self.logger.log(msg, started_booting_at=self.started_booting_at)

        self.update_state_in_db()
        self._start(disk_image=disk_image, disk_size=disk_size)
        self.execute(
            job_id=self.current_job, parallelism=parallelism, delete_when_done=self.delete_when_done
        )
        return self

    @classmethod
    def start(
        cls,
        db: firestore.Client,
        logger: Logger,
        background_tasks: BackgroundTasks,
        machine_type: str,
        containers: Optional[list[Container]] = None,
        disk_image: str = "global/images/burla-cluster-node-image-4",
        disk_size: int = 1000,  # <- (Gigabytes) minimum is 1000 due to disk image
        instance_client: Optional[InstancesClient] = None,
    ):
        self = cls._init(
            db=db,
            logger=logger,
            background_tasks=background_tasks,
            instance_name=f"burla-node-{uuid4().hex}",
            machine_type=machine_type,
            containers=containers,
            started_booting_at=datetime.now(TZ),
            instance_client=instance_client,
        )
        self.is_booting = True
        msg = f"Node (NOT THE VM INSTANCE) {self.instance_name}"
        msg += f" started booting at {self.started_booting_at}"
        self.logger.log(msg, started_booting_at=str(self.started_booting_at))

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

        startup_script_metadata = Items(key="startup-script", value=NODE_STARTUP_SCRIPT)
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

                now = datetime.now(TZ)
                msg = f"VM Instance (NOT THE NODE) {self.instance_name} started booting at {now}"
                self.logger.log(msg, started_booting_at=str(now))
                break

            except ServiceUnavailable:  # <- not enough instances in this zone.
                instance_created = False

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
            elif status not in ["BOOTING", "REBOOTING", "PLEASE_REBOOT"]:
                raise Exception(f"UNEXPECTED STATE WHILE BOOTING: {status}")
            elif status == "PLEASE_REBOOT":
                self.reboot()  # <- node doesn't boot containers automatically
            sleep(5)
            status = self.status()

        self.finished_booting_at = datetime.now(TZ)
        msg = f"Node (NOT THE VM INSTANCE) {self.instance_name}"
        msg += f" finished booting at {self.finished_booting_at}"
        self.logger.log(msg, finished_booting_at=str(self.finished_booting_at))
        self.is_booting = False
        self.update_state_in_db()

    def update_state_in_db(self):
        collection = self.db.collection("current_cluster")
        ordered_collection = collection.order_by("timestamp", direction=firestore.Query.DESCENDING)
        current_cluster = ordered_collection.limit(1).get()[0].to_dict()

        node_matches_self = lambda node: node["instance_name"] == self.instance_name
        node_not_in_db = not any([node_matches_self(n) for n in current_cluster["Nodes"]])
        node_is_deleting_or_deleted = self.is_deleting or getattr(self, "_deleted", None)
        node_is_new = node_not_in_db and not node_is_deleting_or_deleted

        # remove & replace node with updated one
        for node in current_cluster["Nodes"]:
            if node["instance_name"] == self.instance_name:
                previous_status = node["status"]
                current_cluster["Nodes"].remove(node)
                break
        current_state = self.to_dict()
        current_status = current_state["status"]
        if self._deleted == False:
            current_cluster["Nodes"].append(current_state)

        if node_is_new:
            print(f"added {self.instance_name} to db")
            previous_status = current_status

        # remove & replace job with updated one
        if self.current_job != None:
            # is this job still active ?
            matching_jobs = [j for j in current_cluster["Jobs"] if j["id"] == self.current_job]
            job = matching_jobs[0] if matching_jobs else None
            # if it is: update and replace
            if job:
                current_cluster["Jobs"].remove(job)
                if previous_status != "RUNNING" and current_status == "RUNNING":
                    job["current_parallelism"] += self.parallelism
                elif previous_status == "RUNNING" and current_status != "RUNNING":
                    job["current_parallelism"] -= self.parallelism
                current_cluster["Jobs"].append(job)

        # update timestamp & save to db
        current_cluster["timestamp"] = SERVER_TIMESTAMP
        self.db.collection("current_cluster").add(current_cluster)

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
            is_rebooting=self.is_rebooting,
            is_deleting=self.is_deleting,
        )

    def execute(self, job_id: str, parallelism: int, delete_when_done: bool = False):
        self.delete_when_done = delete_when_done  # <- actual deletion handled in Cluster.status
        self.current_job = job_id
        self.update_state_in_db()

        response = requests.post(f"{self.host}/jobs/{job_id}", json={"parallelism": parallelism})
        response.raise_for_status()

        self.parallelism = parallelism
        self.update_state_in_db()

    def reboot(self):
        if self.status() != "REBOOTING":
            containers_json = [container.to_dict() for container in self.containers]
            response = requests.post(f"{self.host}/reboot", json=containers_json)
            response.raise_for_status()
            self.is_rebooting = True

        # confirm node is rebooting
        status = self.status()
        if status not in ["REBOOTING", "READY"]:
            raise Exception(f"Node {self.instance_name} failed start rebooting! status={status}")

        # poll status until done rebooting:
        while status != "READY":
            if status == "FAILED":
                self.delete()
                raise Exception(f"Node {self.instance_name} Failed to start!")
            elif status != ["REBOOTING"]:
                raise Exception(f"UNEXPECTED STATE WHILE REBOOTING: {status}")
            else:
                sleep(5)
                status = self.status()
        self.is_rebooting = False

    def async_reboot(self):
        add_logged_background_task(self.background_tasks, self.logger, self.reboot)

    def reboot_and_execute(self, job_id: str, parallelism: int, delete_when_done: bool = False):
        if self.status() != "READY":  # <- if status is "READY" node must have already rebooted.
            self.reboot()
        self.execute(job_id=job_id, parallelism=parallelism, delete_when_done=delete_when_done)

    def async_reboot_and_execute(self, *a, **kw):
        add_logged_background_task(
            self.background_tasks, self.logger, self.reboot_and_execute, *a, **kw
        )

    def status(self, timeout=1, timeout_remaining=None):
        """
        Returns one of: `PLEASE_REBOOT`, `REBOOTING`, `RUNNING`, `READY`, `FAILED`, `DELETING`.
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
        status = "REBOOTING" if self.is_rebooting and status is None else status
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

        now = datetime.now(TZ)
        self.logger.log(f"Node {self.instance_name} deleted at {now}", deleted_at=str(now))
        self._deleted = True
        self.update_state_in_db()

    def async_delete(self):
        self.is_deleting = True
        self.update_state_in_db()
        add_logged_background_task(self.background_tasks, self.logger, self.delete)
