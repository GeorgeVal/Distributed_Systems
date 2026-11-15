# README

 Implemented functions:
 

 - **User Login**
 - **User Registration**
 - **User Deletion**
 - **Job Submission**
 - **View Jobs**

System Architecture:
The platform is built on a 3-layer architecture consisting of a Client, Master, and Worker:   
Client Layer (UI & Auth): Users must authenticate with a password. Upon login, an authorization service generates a token that identifies the user's role as either admin or user. Authenticated users can submit jobs consisting of map and reduce functions alongside a filename. These jobs can be submitted via a Command-Line Interface (CLI), an API in a scripting language, or a graphical User Interface (UI) built with HTML and Flask.   
Master Layer (Manager): The manager acts as the master node. It receives jobs from the client. It divides the job into smaller tasks. The manager then creates worker pods and assigns the split jobs to these workers. It monitors the tasks, handles task completion, and reassigns any failed tasks to different workers.   
Worker Layer: Worker nodes receive the assigned job partitions from the manager. They read the input data. The workers then execute the map, reduce, and shuffle phases. They process the data, store intermediate data locally, and provide the final output files.   

What we did **not** implement:

**Fault Tolerance** mechanism on pods (restart them if they fail)
When submitting job, **standard map and reduce functions are used** (static)

## How to deploy the implementation

Run **deployment_script.bash** while having a kubernetes cluster active. In a new terminal check whether every pod is running, then use:

    kubectl port-forward service/ui-service 5003:5003


Go to http://localhost:5003/ on your browser and enjoy !!

## Credentials

When you create and deploy the app for the first time use the following credentials:

 - **Admin Login:** Username: **admin**, Password:**pass**
 - **Minio login:**  Access Key: **minio**,	Secret Key: **minio123**

Acknowledgments
This was a collaborative project developed by Farmakis Konstantinos, Mavrikaki Filia and George Valavanis.

To access minio use:

    kubectl port-forward service/minio 9000:9000

****
