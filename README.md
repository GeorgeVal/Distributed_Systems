# README

 Implemented functions:
 

 - **User Login**
 - **User Registration**
 - **User Deletion**
 - **Job Submission**
 - **View Jobs**

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

To access minio use:

    kubectl port-forward service/minio 9000:9000

****
