import os
from typing import List
from time import sleep
from requests import get

from google.cloud.firestore_v1.document import DocumentReference
from google.protobuf.duration_pb2 import Duration
from google.cloud.run_v2 import JobsClient
from google.cloud.run_v2.types import (
    Job,
    ExecutionTemplate,
    TaskTemplate,
    Container,
    EnvVar,
    ResourceRequirements,
)

from main_service import PROJECT_ID


ENV_BUILDER_CODE = """
import tarfile
import subprocess
import tempfile
import os
import sys

from google.cloud import storage
from google.cloud import firestore

PROJECT_ID = os.environ["PROJECT_ID"]
JOBS_BUCKET = f"burla-jobs--" + PROJECT_ID

JOB_ID = os.environ["BURLA_JOB_ID"]
job_ref = firestore.Client(project=PROJECT_ID).collection("jobs").document(JOB_ID)

pkgs_formatted = []
for pkg in job_ref.get().to_dict()["env"]["packages"]:
    if pkg.get("version"):
        pkgs_formatted.append(f"{pkg['name']}=={pkg['version']}")
    else:
        pkgs_formatted.append(pkg["name"])

temp_dir = tempfile.mkdtemp()
command = [
    sys.executable,
    "-m",
    "pip",
    "install",
    *pkgs_formatted,
    "--target",
    temp_dir,
    "--use-deprecated",
    "legacy-resolver"
]
result = subprocess.run(command, stderr=subprocess.PIPE)
print("DONE EXECUTING PIP INSTALL COMMAND")
if result.returncode != 0:
    job_ref.update({"env.install_error": result.stderr.decode()})
    sys.exit(0)

print("TARRING")
tar_path = tempfile.mktemp(suffix=".tar.gz")
with tarfile.open(tar_path, "w:gz") as tar:
    tar.add(temp_dir, arcname=os.path.sep)
print("DONE TARRING")

print("UPLOADING")
bucket_name = JOBS_BUCKET
blob_name = f"{JOB_ID}/env.tar.gz"
storage_client = storage.Client()
bucket = storage_client.get_bucket(bucket_name)
blob = bucket.blob(blob_name)
blob.upload_from_filename(tar_path)
print("DONE UPLOADING")

uri = f"gs://{JOBS_BUCKET}/{blob_name}"
job_ref.update({"env.uri": uri})
print(f"Successfully built environment at: {uri}")
"""
ENV_BUILDER_SCRIPT = 'python -c "{}"'.format(ENV_BUILDER_CODE.replace('"', r"\""))
SWAP_PACKAGES = {"psycopg2": "psycopg2-binary"}


def start_building_environment(
    packages: List[dict], job_ref: DocumentReference, job_id: str, image: str
):
    # clean packages list
    is_pip_pkg = lambda pkg: get(f"https://pypi.org/pypi/{pkg}/json").status_code != 404
    pip_packages = [pkg for pkg in packages if is_pip_pkg(pkg["name"])]
    for pkg in pip_packages:
        pkg["name"] = SWAP_PACKAGES.get(pkg["name"], pkg["name"])
    env = {"env.is_copied_from_client": True, "env.packages": packages, "env.image": image}
    job_ref.update(env)

    # start google cloud run job
    container = Container(
        image=image,
        command=["/bin/sh", "-c", ENV_BUILDER_SCRIPT],
        env=[
            EnvVar(name="BURLA_JOB_ID", value=job_id),
            EnvVar(name="PROJECT_ID", value=PROJECT_ID),,
        ],
        resources=ResourceRequirements(limits={"memory": "32Gi", "cpu": "8"}),
    )
    timeout = Duration(seconds=3600)  # 1hr
    task_template = TaskTemplate(max_retries=1, containers=[container], timeout=timeout)
    job = Job(template=ExecutionTemplate(template=task_template))
    client = JobsClient()

    cloud_run_job_id = f"env-build-{job_id}"
    parent_resource = f"projects/{PROJECT_ID}/locations/us-central1"
    cloud_run_job_name = f"{parent_resource}/jobs/{cloud_run_job_id}"
    client.create_job(parent=parent_resource, job=job, job_id=cloud_run_job_id)

    while client.get_job(name=cloud_run_job_name).reconciling:
        sleep(1)
    client.run_job(name=cloud_run_job_name)
