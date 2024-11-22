import json
import asyncio
from time import time
from typing import Callable
from uuid import uuid4

import slack_sdk
from fastapi import APIRouter, Depends
from google.cloud.firestore_v1 import FieldFilter
from google.cloud.compute_v1 import InstancesClient
from starlette.responses import StreamingResponse
from googleapiclient import discovery
from googleapiclient.errors import HttpError
from concurrent.futures import ThreadPoolExecutor

from main_service import DB, IN_PROD, PROJECT_ID, get_logger, get_add_background_task_function
from main_service.cluster import reconcile
from main_service.node import Container, Node
from main_service.helpers import Logger, get_secret

router = APIRouter()


@router.post("/v1/cluster/restart")
def restart_cluster(
    add_background_task: Callable = Depends(get_add_background_task_function),
    logger: Logger = Depends(get_logger),
):
    start = time()

    try:
        # Before starting, update cluster status to 'BOOTING'
        DB.collection('cluster-status').document('status').set({
            'status': 'BOOTING',
            'error': None
        })
    
        instance_client = InstancesClient()

        if IN_PROD:
            client = slack_sdk.WebClient(token=get_secret("slackbot-token"))
            client.chat_postMessage(channel="user-activity", text="Someone started the prod cluster.")

        futures = []
        executor = ThreadPoolExecutor(max_workers=32)

        # delete all nodes
        node_filter = FieldFilter("status", "in", ["READY", "BOOTING", "RUNNING"])
        for node_snapshot in DB.collection("nodes").where(filter=node_filter).stream():
            node = Node.from_snapshot(DB, logger, add_background_task, node_snapshot, instance_client)
            futures.append(executor.submit(node.delete))

        # add nodes according to cluster_config doc
        def _add_node_logged(machine_type, containers, inactivity_time):
            Node.start(
                db=DB,
                logger=logger,
                add_background_task=add_background_task,
                machine_type=machine_type,
                containers=containers,
                inactivity_shutdown_time_sec=inactivity_time,
                verbose=True,
            )
            

        config = DB.collection("cluster_config").document("cluster_config").get().to_dict()
        for node_spec in config["Nodes"]:
            for _ in range(node_spec["quantity"]):
                machine_type = node_spec["machine_type"]
                containers = [Container.from_dict(c) for c in node_spec["containers"]]
                inactivity_time = node_spec.get("inactivity_shutdown_time_sec")
                future = executor.submit(_add_node_logged, machine_type, containers, inactivity_time)
                futures.append(future)

        # wait until all operations done
        [future.result() for future in futures]
        executor.shutdown(wait=True)

        # After starting, update cluster status to 'ON'
        DB.collection('cluster-status').document('status').set({
            'status': 'ON',
            'error': None
        })

        logger.log("Done restarting, reconciling ...")
        add_background_task(reconcile, DB, logger, add_background_task)

    except Exception as e:
        logger.log(f"Error during cluster restart: {e}")
        # Update status to 'ERROR'
        DB.collection('cluster-status').document('status').set({
            'status': 'ERROR',
            'error': str(e)
        })
        return {"status": "error", "message": f"Error during cluster restart: {e}"}, 500

    duration = time() - start
    logger.log(f"Restarted after {duration//60}m {duration%60}s")
    return "Success"


@router.get("/v1/cluster")
async def cluster_info():
    node_name_to_status = {}

    async def node_stream():
        while True:
            status_filter = FieldFilter("status", "not-in", ["DELETED", "FAILED"])
            node_docs = list(DB.collection("nodes").where(filter=status_filter).stream())
            nodes = [doc.to_dict() for doc in node_docs]
            node_names = [node["instance_name"] for node in nodes]
            names_of_deleted_nodes = set(node_name_to_status.keys()) - set(node_names)

            # Broadcast deleted nodes:
            for node_name in names_of_deleted_nodes:
                event_data = dict(nodeId=node_name, deleted=True)
                yield f"data: {json.dumps(event_data)}\n\n"
                del node_name_to_status[node_name]
                print(f"deleted node: {event_data}")

            # Broadcast status updates:
            for node in nodes:
                instance_name = node["instance_name"]
                hardware_type = node["machine_type"]
                current_status = node["status"]
                previous_status = node_name_to_status.get(instance_name)

                if current_status != previous_status:
                    node_name_to_status[instance_name] = current_status
                    event_data = dict(nodeId=instance_name, status=current_status, machine=hardware_type)
                    yield f"data: {json.dumps(event_data)}\n\n"
                    print(f"updated status: {event_data}")

            await asyncio.sleep(1)

    return StreamingResponse(node_stream(), media_type="text/event-stream")


@router.get("/v1/cluster/status")
async def cluster_status():
    async def event_stream():
        previous_status = None
        while True:
            status_doc = DB.collection('cluster-status').document('status').get().to_dict()
            if status_doc:
                status = status_doc.get('status', 'OFF')
            else:
                status = 'OFF'

            # Only send update if status has changed
            if status != previous_status:
                event_data = {"status": status}
                yield f"data: {json.dumps(event_data)}\n\n"
                previous_status = status

            await asyncio.sleep(2)  # Non-blocking sleep for 2 second

    return StreamingResponse(event_stream(), media_type="text/event-stream")


@router.post("/v1/cluster/shutdown")
def shutdown_cluster():
    project_id = PROJECT_ID

    # Create the Compute Engine service
    service = discovery.build('compute', 'v1')

    # Step 1: Update Firestore to indicate shutdown is in progress
    try:
        # Set 'status' to 'SHUTTING DOWN'
        DB.collection('cluster-status').document('status').set({
            'status': 'SHUTTING DOWN',
            'error': None
        })
    except Exception as e:
        print(f"Failed to update Firestore before shutdown: {e}")
        return {"status": "error", "message": "Failed to update Firestore before shutdown"}, 500

    try:
        # Step 2: Get all zones in the project
        request = service.zones().list(project=project_id)
        zones = []

        while request is not None:
            response = request.execute()
            zones.extend(zone['name'] for zone in response.get('items', []))
            request = service.zones().list_next(previous_request=request, previous_response=response)

        # Step 3: Iterate over all zones and delete active instances
        for zone in zones:
            request = service.instances().list(project=project_id, zone=zone)

            while request is not None:
                response = request.execute()

                if 'items' in response:
                    for instance in response['items']:
                        if instance['status'] in ['PROVISIONING', 'STAGING', 'RUNNING']:
                            instance_name = instance['name']
                            print(f"Deleting active VM: {instance_name} in zone: {zone}")

                            delete_request = service.instances().delete(
                                project=project_id, zone=zone, instance=instance_name
                            )
                            delete_request.execute()

                request = service.instances().list_next(previous_request=request, previous_response=response)

    except HttpError as err:
        # Step 4: Handle HttpError (from Google API)
        print(f"An HTTP error occurred: {err}")
        try:
            DB.collection('cluster-status').document('status').set({
                'status': 'ERROR',
                'error': f"HttpError: {err}"
            })
        except Exception as firestore_err:
            print(f"Failed to update Firestore after HttpError: {firestore_err}")
        return {"status": "error", "message": f"An HTTP error occurred: {err}"}, 500

    except Exception as e:
        # Step 5: Handle generic errors
        print(f"Unexpected error occurred: {e}")
        try:
            DB.collection('cluster-status').document('status').set({
                'status': 'ERROR',
                'error': f"UnexpectedError: {e}"
            })
        except Exception as firestore_err:
            print(f"Failed to update Firestore after unexpected error: {firestore_err}")
        return {"status": "error", "message": f"Unexpected error occurred: {e}"}, 500

    # Step 6: After shutdown completes, update Firestore to indicate cluster is OFF
    try:
        DB.collection('cluster-status').document('status').set({
            'status': 'OFF',
            'error': None
        })
    except Exception as e:
        print(f"Failed to update Firestore after successful shutdown: {e}")
        return {"status": "error", "message": "Shutdown completed, but failed to update Firestore"}, 500

    return {"status": "success", "message": "Cluster successfully shut down"}, 200
