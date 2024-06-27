# manager.py


import ssl
import sys
from flask import Flask, request, jsonify
from kubernetes.client.api import core_v1_api
from kubernetes import client, config
from pymongo import MongoClient
from minio import Minio
import requests


#config.load_kube_config()
config.load_incluster_config()

app = Flask(__name__)

#client = MongoClient('mongodb://mongodb-service.default.svc.cluster.local:27777/', serverSelectionTimeoutMS=5000)
clientMongo = MongoClient('mongodb://mongodb-service:27777/')

clientMinio = Minio(
        "10.244.1.185:9000",
        access_key="minio",
        secret_key="minio123",
        secure=False
    )

def uploadFile(source_file):
    # The destination bucket and filename on the MinIO server
    bucket_name = "python-test-bucket"
    destination_file = "my-test-file.txt"

    


    # Make the bucket if it doesn't exist.
    found = clientMinio.bucket_exists(bucket_name)
    if not found:
         clientMinio.make_bucket(bucket_name)
         print("Created bucket", bucket_name)
    else:
         print("Bucket", bucket_name, "already exists")

    # Upload the file, renaming it in the process
    clientMinio.fput_object(
        bucket_name, destination_file, source_file,
    )
    print(
        source_file, "successfully uploaded as object",
        destination_file, "to bucket", bucket_name,
    )

db = clientMongo['Manager_db']
db_auth =clientMongo['auth_db']
jobs_collection = db['jobs']

#jobs = {}


#tokens from our mongodb 'tokens'
tokens_collection = db_auth['tokens']

global job_id_counter 
job_id_counter = 1





job_data = {
        "job_id": "2",
        "status": "submitted",
        "job_type": "wordcount",
        "map_script": "def map_function(line): return line.split()",
        "reduce_script": "def reduce_function(key, values): return sum(values)",
        "input_file": "/mnt/data/job_{}_input.txt".format(job_id_counter)
    }
job_id_counter += 1

print("Inserting new job in db...")
jobs_collection.insert_one(job_data)


@app.route('/submit_job', methods=['POST'])
def submit_job():
    token = request.headers.get('Authorization')
    #if token not in tokens_collection:
    #    return jsonify({"error": "Unauthorized"}), 403
    data = request.json
    job_type = data['job_type']
    map_script = data['map_script']
    reduce_script = data['reduce_script']
    input_file = data['input_file']
    mappers = data["mappers"]
    reducers = data["reducers"]
    print(f'input_file: {input_file}')
    global job_id_counter
    #job_id = job_id_counter
    job_id = 1
    job_id_counter += 1
    input_file_path = f"/data/pv0001/job_{job_id}_input.txt"
    #with open(input_file_path, 'wb') as f:
    #    f.write(input_file.encode())

    with open(input_file_path, 'w') as file:
        file.write(input_file)

    uploadFile(input_file_path)

    # Register the job in MongoDB
    job_data = {
        "job_id": job_id,
        "status": "submitted",
        "job_type": job_type,
        "map_script": map_script,
        "reduce_script": reduce_script,
        "input_file": input_file_path
    }
    jobs_collection.insert_one(job_data)

    # Assign job to a mapper
    for i in range(len(mappers)):
        assign_job_to_worker(job_id, job_type, map_script, reduce_script, input_file_path)

    # Assign job to a reducer
    for i in range(len(mappers)):
        assign_job_to_worker(job_id, job_type, map_script, reduce_script, input_file_path)
    
    return jsonify({"message": "Job submitted successfully", "job_id": job_id}), 200

@app.route('/create-pod', methods=['POST'])
def open_pod():
    '''
    This method launches a pod in kubernetes cluster according to command
    '''

    

    core_v1 = core_v1_api.CoreV1Api()
    pod_name = "worker_pod"
    namespace = "default"
    api_response = None
    try:
        api_response = core_v1.read_namespaced_pod(name=pod_name,
                                                        namespace=namespace)
    except Exception as e:
        if e.status != 404:
            print("Unknown error: %s" % e)
            exit(1)

    if not api_response:
        # Create pod manifest
        pod_manifest = {
            'apiVersion': 'v1',
            'kind': 'Pod',
            'metadata':{
                'name': pod_name,
                'labels':{
                    'app': 'worker'
                    },
            },
            'spec':{
                'containers'[{
                    'name': 'worker',
                    'image': 'fmavrikaki/worker-service:latest',
                    'ports':{
                        'containerPort': '5004',
                    }
            }]
            }
        }
        
        
        api_response = core_v1.create_namespaced_pod(body=pod_manifest,                                                          namespace=namespace)

        while True:
            api_response = core_v1.read_namespaced_pod(name=pod_name,
                                                            namespace=namespace)
            if api_response.status.phase != 'Pending':
                break
        
        print(f'Pod {pod_name} in {namespace} created.')

        return jsonify({"message": f"Pod {pod_name} created in {namespace} default"}), 200



def assign_job_to_worker(job_id, job_type, map_script, reduce_script, input_file):
    try:
        v1 = client.CoreV1Api()
        namespace = "default"  # Adjust to your namespace
        pods = v1.list_namespaced_pod(namespace=namespace, label_selector="app=worker")
        
        available_workers = [pod.metadata.name for pod in pods.items if pod.status.phase == "Running"]
        
        if not available_workers:
            print("No available workers found")
            return
        
        # Simple round-robin load balancing
        worker_index = job_id % len(available_workers)
        assigned_worker = available_workers[worker_index]

        result = ""
        for worker in available_workers:
            result += "new available worker: "+worker+"\n"

        debug_file_path = f"/data/pv0001/debug.txt"
        #with open(input_file_path, 'wb') as f:
        #    f.write(input_file.encode())

        with open(debug_file_path, 'w') as file:
            file.write(debug_file_path)

        
        # Update the job with the assigned worker details in MongoDB
        job_update = {
            "worker": assigned_worker,
            "status": "assigned"
        }
        jobs_collection.update_one({"job_id": job_id}, {"$set": job_update})

        url = "10.244.1.168:5004/execute"
        data = {
            "task_type": "map",  # or "reduce"
            "input_file_path": "path/to/input_file.txt",
            "output_file_path": "path/to/output_file.txt",  # optional
            "key": "some_key"  # only needed for "reduce" task_type
        }

        requests.post(url, json="data")

    except Exception as e:
        print(f"Failed to assign job {job_id}: {str(e)}")

@app.route('/jobs', methods=['GET'])
def get_jobs():
    token = request.headers.get('Authorization')
    token_entry = tokens_collection.find_one({'token': token})
    #if not token_entry:
    #    return jsonify({"error": "Token Not Found"}), 403

    jobs = list(jobs_collection.find({}, {'_id': False}))
    return jsonify(jobs), 200

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5002)