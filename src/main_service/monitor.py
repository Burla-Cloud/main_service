import time
import sys
from google.cloud import firestore
from google.cloud.firestore_v1 import FieldFilter
import os

os.environ["GRPC_VERBOSITY"] = "ERROR"
os.environ["GLOG_minloglevel"] = "2"

# Initialize Firestore client
db = firestore.Client(project="joe-test-407923")

previous_statuses = {}

def check_node_statuses():
    # Reference the 'nodes' collection
    nodes_ref = db.collection('nodes')

    # Create a FieldFilter for the 'status' field to only include 'RUNNING' and 'BOOTING'
    status_filter = FieldFilter("status", "in", ["RUNNING", "BOOTING", None])

    # Apply the filter to the query
    query = nodes_ref.where(filter=status_filter)

    # Query the documents in the 'nodes' collection that match the filter
    docs = query.stream()

    # Track current nodes
    current_node_ids = set()

    for doc in docs:
        node_data = doc.to_dict()
        doc_id = doc.id
        current_status = node_data.get('status')
        previous_status = previous_statuses.get(doc_id)

        # Track this node in the current set
        current_node_ids.add(doc_id)

        # Apply your logic for status changes
        if current_status == 'BOOTING' and previous_status != 'BOOTING':
            print(f"{time.ctime()}: Node {doc_id} is BOOTING up.")
        elif current_status == 'RUNNING' and previous_status == 'BOOTING':
            print(f"{time.ctime()}: Node {doc_id} has transitioned from BOOTING to RUNNING.")

        # Update the dictionary with the current status
        previous_statuses[doc_id] = current_status

    # Check for nodes that were in previous states but are now missing
    for doc_id in list(previous_statuses.keys()):
        if doc_id not in current_node_ids and previous_statuses[doc_id] in ('BOOTING', 'RUNNING'):
            print(f"{time.ctime()}: Node {doc_id} is being DELETED from {previous_statuses[doc_id]} status.")
            # Optionally remove the node from previous_statuses if it's considered deleted
            del previous_statuses[doc_id]

    # Stop the script if there are no more nodes to track
    if not previous_statuses and not current_node_ids:
        print("All nodes have been processed and deleted. Exiting script.")
        return True

    return False

if __name__ == "__main__":
    while True:
        if check_node_statuses():
            break
        time.sleep(2)  # Polling interval of 2 seconds
