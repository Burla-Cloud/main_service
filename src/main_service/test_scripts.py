# # from google.cloud import firestore
# # from google.cloud.firestore_v1 import FieldFilter
# # import os

# # # Suppress logging warnings
# # os.environ["GRPC_VERBOSITY"] = "ERROR"
# # os.environ["GLOG_minloglevel"] = "2"

# # def get_filtered_nodes():
# #     # Initialize Firestore client
# #     db = firestore.Client(project="joe-test-407923")

# #     # Reference the 'nodes' collection
# #     nodes_ref = db.collection('nodes')

# #     # Create a FieldFilter for the 'status' field
# #     status_filter = FieldFilter("status", "in", ["RUNNING", "BOOTING", "READY"])

# #     # Apply the filter to the query
# #     query = nodes_ref.where(filter=status_filter)

# #     # Execute the query and stream the results
# #     docs = query.stream()

# #     # Print the document IDs and their status
# #     for doc in docs:
# #         print(f"Node ID: {doc.id}, "
# #         f"Status: {doc.to_dict().get('status')}, "
# #         f"Machine: {doc.to_dict().get('machine_type')}, "
# #         f"Zone: {doc.to_dict().get('zone')}")


# # if __name__ == "__main__":
# #     get_filtered_nodes()



# import time
# import sys
# import random
# import os
# from google.cloud import firestore

# os.environ["GRPC_VERBOSITY"] = "ERROR"
# os.environ["GLOG_minloglevel"] = "2"

# # Initialize Firestore client
# db = firestore.Client(project="joe-test-407923")

# # List of statuses to cycle through
# # statuses = ["BOOTING", "RUNNING", "DELETING"]
# statuses = ["DELETING"]

# def create_or_update_node_status(node_id, status):
#     # Reference to the specific node document
#     doc_ref = db.collection('nodes').document(node_id)

#     # Set or update the status field
#     doc_ref.set({"status": status})

#     print(f"Node {node_id} status set to {status}")

# def cycle_statuses_for_nodes():
#     node_ids = [f'node_{i}' for i in range(1, 20)] 

#     for status in statuses:
#         for node_id in node_ids:
#             create_or_update_node_status(node_id, status)
#             # Introduce a small delay between updates to simulate close timing
#             delay = random.uniform(1,2)  # Random delay between 1 and 3 seconds
#             time.sleep(delay)

# if __name__ == "__main__":
#     cycle_statuses_for_nodes()

from burla import remote_parallel_map

x = [1, 3, 4, 5, 9, 10, 11]

def basic_function(var_1):
    var_1**3
    return var_1

results = remote_parallel_map(basic_function, x)

print(results)