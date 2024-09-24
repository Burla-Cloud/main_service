import json
import asyncio
from time import time
from typing import Callable

from fastapi import APIRouter, Depends
from google.cloud.firestore_v1 import FieldFilter
from starlette.responses import StreamingResponse
from googleapiclient import discovery
from googleapiclient.errors import HttpError

from main_service import DB, get_logger, get_add_background_task_function, PROJECT_ID
from main_service.cluster import Cluster
from main_service.helpers import Logger

router = APIRouter()


@router.post("/v1/cluster/restart")
def restart_cluster(
    add_background_task: Callable = Depends(get_add_background_task_function),
    logger: Logger = Depends(get_logger),
):
    start = time()
    cluster = Cluster.from_database(db=DB, logger=logger, add_background_task=add_background_task)
    cluster.restart(force=True)
    duration = time() - start
    logger.log(f"Restarted after {duration//60}m {duration%60}s")
    return "Success"


@router.get("/v1/cluster")
async def cluster_info():
    node_name_to_status = {}

    async def node_stream():
        while True:
            status_filter = FieldFilter("status", "not-in", ["DELETED", "DELETING"])
            node_docs = list(DB.collection("nodes").where(filter=status_filter).stream())
            nodes = [doc.to_dict() for doc in node_docs]
            node_names = [node["instance_name"] for node in nodes]
            names_of_deleted_nodes = set(node_name_to_status.keys()) - set(node_names)

            # brodcast deleted nodes:
            for node_name in names_of_deleted_nodes:
                event_data = dict(nodeId=node_name, deleted=True)
                yield f"data: {json.dumps(event_data)}\n\n"
                del node_name_to_status[node_name]
                print(f"deleted node: {event_data}")

            # brodcast status updates:
            for node in nodes:
                instance_name = node["instance_name"]
                current_status = node["status"]
                previous_status = node_name_to_status.get(instance_name)

                if current_status != previous_status:
                    node_name_to_status[instance_name] = current_status
                    event_data = dict(nodeId=instance_name, status=current_status)
                    yield f"data: {json.dumps(event_data)}\n\n"
                    print(f"updated status: {event_data}")

            await asyncio.sleep(1)

    return StreamingResponse(node_stream(), media_type="text/event-stream")


@router.post("/v1/cluster/shutdown")
def shutdown_cluster():
    # Create the Compute Engine service
    service = discovery.build('compute', 'v1')
    project_id = PROJECT_ID
    
    try:
        # Get all zones in the project
        request = service.zones().list(project=project_id)
        zones = []

        while request is not None:
            response = request.execute()
            zones.extend(zone['name'] for zone in response.get('items', []))
            request = service.zones().list_next(previous_request=request, previous_response=response)

        # Iterate over all zones and list instances
        for zone in zones:
            request = service.instances().list(project=project_id, zone=zone)

            while request is not None:
                response = request.execute()

                if 'items' in response:
                    for instance in response['items']:
                        # Only delete VMs with statuses indicating they are active
                        if instance['status'] in ['PROVISIONING', 'STAGING', 'RUNNING']:
                            instance_name = instance['name']
                            print(f"Deleting active VM: {instance_name} in zone: {zone}")
                            
                            # Delete the instance
                            delete_request = service.instances().delete(
                                project=project_id, zone=zone, instance=instance_name
                            )
                            delete_request.execute()
                
                request = service.instances().list_next(previous_request=request, previous_response=response)

    except HttpError as err:
        print(f"An error occurred: {err}")
    except Exception as e:
        print(f"Unexpected error: {e}")

    return "Cluster Off"