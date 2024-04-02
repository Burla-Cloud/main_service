#### Main Service

This is a component of the open-source cluster compute software [Burla](https://github.com/Burla-Cloud/burla).

The "main service" is a fastapi webservice designed to be deployed in [google-cloud-run](cloud.google.com/run).  
This service acts as a traditional "head node" would, as well as handing other responsibilities.  
This service is responsible for:

- Adding/removing/managing nodes in the cluster.
- Routing requests from clients to the correct [Node-Service](https://github.com/Burla-Cloud/node_service)'s.
- Aggregating logs/errors/results in the database so they can be retrieved by any clients.
- Interfacing with GCS on behalf of any users.
- Building python environments such that they can be later downloaded in environments where user-code is executed.

Every "main service" instance has it's own [google-cloud-firestore](cloud.google.com/firestore) database associated with it.  
It is not possible to run more than one "main-service" instance in any single VPC.  
It is not possible to run more than one "cluster" using a single "main-service".  
Clusters are designed such that they autoscale quickly and execute many jobs (calls to `remote_partallel_map`) in parallel.
