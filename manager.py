# manager.py

import random
import ssl
import sys
import time
from flask import Flask, request, jsonify
from kubernetes.client.api import core_v1_api
from kubernetes import client, config
from pymongo import MongoClient
from minio import Minio
import os
import requests
from time import sleep

#config.load_kube_config()
config.load_incluster_config()

app = Flask(__name__)

#client = MongoClient('mongodb://mongodb-service.default.svc.cluster.local:27777/', serverSelectionTimeoutMS=5000)
clientMongo = MongoClient('mongodb://mongodb-service:27777/')


clientMinio = Minio(
        "minio:9000",
        access_key="minio",
        secret_key="minio123",
        secure=False
    )

global allMappers
global allMappersConst
global allReducers
global allReducersConst
allMappers = -5
allReducers = -5
allMappersConst = -5
allReducersConst = -5
db = clientMongo['Manager_db']
db_auth =clientMongo['auth_db']
jobs_collection = db['jobs']

#tokens from our mongodb 'tokens'
tokens_collection = db_auth['tokens']


def createBuckets():
     buckets = ["map-bucket", "reduce-bucket"]
     # Make the bucket if it doesn't exist.
     for bucket_name in buckets:
        found = clientMinio.bucket_exists(bucket_name)
        if not found:
            clientMinio.make_bucket(bucket_name)

def uploadFile(source_file, bucket_name, destination_file):


    # Make the bucket if it doesn't exist.
    found = clientMinio.bucket_exists(bucket_name)
    if not found:
         clientMinio.make_bucket(bucket_name)

    # Upload the file, renaming it in the process
    clientMinio.fput_object(
        bucket_name, destination_file, source_file,
    )





def split_file(jobId, bucket_name, object_name, file_name, n):
    # Read the input file
    clientMinio.fget_object("input-bucket", f"input{jobId}.txt", f"input{jobId}.txt")

    with open(f"input{jobId}.txt", 'r') as f:
        lines = f.readlines()

    # Total number of lines
    total_lines = len(lines)
    
    # Number of lines per file
    lines_per_file = total_lines // n
    remainder = total_lines % n

    # Create the output files and write the lines to each file
    start = 0
    for i in range(n):
        end = start + lines_per_file + (1 if i < remainder else 0)  # Add an extra line to the first few files if there is a remainder
        with open(f'input-{i}.txt', 'w') as f:
            for j in range(start, end):
                f.write(lines[j])
        start = end
        formatted_number = f"{i:02}"
        fullString = "worker-pod"+str(jobId)+formatted_number+".txt"
        bucket_name = "input-bucket" 
        uploadFile(f'input-{i}.txt', bucket_name, fullString)





@app.route('/submit_job', methods=['POST'])
def submit_job():
    global allMappersConst
    global allMappers 
    global allReducers
    global allReducersConst
    token = request.headers.get('Authorization')
    #if token not in tokens_collection:
    #    return jsonify({"error": "Unauthorized"}), 403
    data = request.json
    job_type = data['job_type']
    map_script = data['map_script']
    reduce_script = data['reduce_script']
    mappers = data["mappers"]
    reducers = data["reducers"]
    job_id = data["job_id"]
    #job_id = job_id_counter
    input_file_path = f"/data/pv0001/job_{job_id}_input.txt"

    with open(f'stating.txt', 'w') as f:
            f.write(f"{job_id}\n")

    allMappers = int(mappers)
    allMappersConst = int(mappers)
    allReducers = int(reducers)
    allReducersConst = int(reducers)

    # os.makedirs(os.path.dirname(input_file_path), exist_ok=True)

    #source_file = input_file_path
    #bucket_name = "input-bucket"
    #destination = f"job_{job_id}_input_tmp.txt"
    #uploadFile(source_file, bucket_name, destination)
    #with open(input_file_path, 'wb') as f:
    #    f.write(input_file.encode())


    split_file(job_id, "input-bucket", "input{job_id}.txt", "input{job_id}.txt", int(mappers))

    #uploadFile(input_file_path)

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

    pod_ip={}
    # Assign job to a mapper
    for i in range(int(mappers)):
        pod_ip[i]=assign_job_to_worker(job_id, "map", i)
        with open(f'debug-{i}.txt', 'w') as f:
            f.write(f"{pod_ip[i]}\n")
        

    
    return jsonify({"message": "Job submited."}), 200

def assign_job_to_worker(job_id, flag, idHelper):
    formatted_number = f"{idHelper:02}"
    fullString = "worker-pod"+str(job_id)+formatted_number
    if(flag == "reduce"):
        fullString = 'r'+fullString
    pod_ip=create_pod(fullString, flag, job_id)
    return pod_ip
    #return jsonify({"message": "Full string is ", "fullString": job_id}), 200


def delete_all_files(bucket_name):
    try:
        objects = clientMinio.list_objects(bucket_name, recursive=True)
        for obj in objects:
            clientMinio.remove_object(bucket_name, obj.object_name)
        print(f"All files in bucket '{bucket_name}' have been deleted.")
    except Exception as e:
        print(f"Error deleting files in bucket '{bucket_name}': {e}")


#@app.route('/create-job', methods=['POST'])
def create_pod(pod_name,flag, jobId):
    '''
    This method launches a pod in kubernetes cluster according to command
    '''
    
    global allMappers 
    global allReducers
    core_v1 = core_v1_api.CoreV1Api()
    namespace = "default"
    mappers = allMappers

    with open(f'jobId.txt', 'w') as f:
            f.write(f"{jobId}\n")
    # Create pod manifest
    pod_manifest = {
        'apiVersion': 'v1',
        'kind': 'Pod',
        'metadata': {
            'name': pod_name,
            'labels': {
                'app': 'worker'
            }
        },
        'spec': {
            'containers': [{
                'name': 'worker',
                'image': 'kfarmakis/worker-service:latest',
                'ports': [{
                    'containerPort': 5004
                }]
            }]
        }
    }

    # Create the pod
    api_response = core_v1.create_namespaced_pod(body=pod_manifest,
        namespace='default')
    
    
        #---new
    
    # Get the name of the created pod
    pod_name = api_response.metadata.name
    if flag == "map":
        service_name = pod_name
    elif flag == "reduce":
        service_name = pod_name+"-reducer"
    else:
        print("Invalid flag")

    # Wait for the pod to be in 'Running' state
    while True:
        pod_status = core_v1.read_namespaced_pod(name=pod_name, namespace='default')
        if pod_status.status.phase == 'Running':
            break

    # Create a service manifest
    service_manifest = {
        'apiVersion': 'v1',
        'kind': 'Service',
        'metadata': {
            'name': f'{service_name}-service'
        },
        'spec': {
            'selector': {
                'app': 'worker'
            },
            'ports': [{
                'protocol': 'TCP',
                'port': 5004,
                'targetPort': 5004
            }],
            'type': 'ClusterIP'
        }
    }

    # Create the service
    service_response = core_v1.create_namespaced_service(namespace=namespace, body=service_manifest)

    # Get the service's cluster IP
    service_ip = service_response.spec.cluster_ip

    # Construct the filename
    filename = f"{service_name}.txt"
    print(f"{flag}: {filename}")
    sleep(2)    
    # Send a POST request to the pod via the service
    random_number = random.randint(1, 10000)
    with open(f'{random_number}debug-request{pod_name}.txt', 'w') as f:
                    f.write(f"stelnei {flag} request sthn ip = {service_ip}\n mappers:{int(mappers)}")
    response = requests.post(f"http://{service_name}-service.{namespace}.svc.cluster.local:5004/execute", json={"pod_name": service_name, "job_id": jobId, "input_file_path": filename, "task_type": flag, "reducers": int(allReducersConst), "mappers": int(allMappersConst)})

    sleep(2)

    # Parse the JSON response
    response_data = response.json()
    
    # Access the 'flag' value
    flag_value = response_data.get('flag')
    if response.status_code == 200 and  flag_value == "map":
        v1=client.CoreV1Api()
        v1.delete_namespaced_pod(name=service_name,namespace=namespace)
        v1.delete_namespaced_service(name=f"{service_name}-service",namespace=namespace)
        allMappers = allMappers - 1
        if allMappers == 0:
            # Assign job to a reducers
            for i in range(int(allReducers)):
                with open(f'debug-reduce-{i}.txt', 'w') as f:
                    f.write(f"mpainei reduce gia i = {i}\n")
                assign_job_to_worker(jobId, "reduce", i)
    elif response.status_code == 200 and flag_value == "reduce":
        with open("finalReduceStart.txt", 'w') as f:
                f.write(f"{pod_name}")
        v1=client.CoreV1Api()
        v1.delete_namespaced_pod(name=pod_name,namespace=namespace)
        v1.delete_namespaced_service(name=f"{service_name}-service",namespace=namespace)
        allReducers = allReducers - 1
        if allReducers == 0:
            delete_all_files("map-bucket")
            # Create pod manifest
            namespace = "default"
            pod_manifest = {
                'apiVersion': 'v1',
                'kind': 'Pod',
                'metadata': {
                    'name': f"final-reduce{jobId}-worker",
                    'labels': {
                        'app': 'worker'
                    }
                },
                'spec': {
                    'containers': [{
                        'name': 'worker',
                        'image': 'kfarmakis/worker-service:latest',
                        'ports': [{
                            'containerPort': 5004
                        }]
                    }]
                }
            }

            # Create the pod
            api_response = core_v1.create_namespaced_pod(body=pod_manifest,
                namespace='default')

            # Create a service manifest
            service_manifest = {
                'apiVersion': 'v1',
                'kind': 'Service',
                'metadata': {
                    'name': f'final-reduce{jobId}-service'
                },
                'spec': {
                    'selector': {
                        'app': 'worker'
                    },
                    'ports': [{
                        'protocol': 'TCP',
                        'port': 5004,
                        'targetPort': 5004
                    }],
                    'type': 'ClusterIP'
                }
            }

            # Create the service
            service_response = core_v1.create_namespaced_service(namespace=namespace, body=service_manifest)
            sleep(10)
            # Get the service's cluster IP
            service_ip = service_response.spec.cluster_ip
            with open("sendFinal.txt", 'w') as f:
                f.write("mpainei")
            response = requests.post(f"http://final-reduce{jobId}-service.{namespace}.svc.cluster.local:5004/finalReduce", json={"job_id": jobId, "reducers": allReducersConst})
            sleep(4)
            if response.status_code == 200:
                v1.delete_namespaced_pod(name=f"final-reduce{jobId}-worker",namespace=namespace)
                v1.delete_namespaced_service(name=f"final-reduce{jobId}-service",namespace=namespace)
            
            return response

    else:
        return jsonify({"error": f"Worker failed for flag: {flag}"}), 403



    # Get the pod's IP address
    #pod_ip = pod_status.status.pod_ip

    #----new
    #filename= f"{pod_name}.txt"

    #response = requests.post(f"{pod_name}:5004/execute", json={"pod_name": pod_name, "input_file_path": filename, "task_type": flag})
    

    return service_ip
    #return jsonify({"message": f"Pod {pod_name}, pod ip {pod_ip} created in {namespace} default"}), 200


def delete_job(api_instance):
    # Delete job
    api_response = api_instance.delete_namespaced_job(
        name="map",
        namespace="default",
        body=clientMongo.V1DeleteOptions(
            propagation_policy='Foreground',
            grace_period_seconds=5))
    print("Job deleted. status='%s'" % str(api_response.status))

@app.route('/terminate', methods=['POST'])
def terminate_pod():
    data = request.json()
    pod_name = data['pod_name']

    #Delete pod with name pod_name

    #if not token_entry:
    #    return jsonify({"error": "Token Not Found"}), 403

    jobs = list(jobs_collection.find({}, {'_id': False}))
    return jsonify(jobs), 200    

@app.route('/jobs', methods=['GET'])
def get_jobs():
    #token = request.headers.get('Authorization')
    #token_entry = tokens_collection.find_one({'token': token})
    #if not token_entry:
    #    return jsonify({"error": "Token Not Found"}), 403

    jobs = list(jobs_collection.find({}, {'_id': False}))
    return jsonify(jobs), 200

if __name__ == "__main__":
     # Configs can be set in Configuration class directly or using helper
    # utility. If no argument provided, the config will be loaded from
    # default location.

    # config.load_kube_config()
    config.load_incluster_config()
    createBuckets()


    # Create a job object with client-python API. The job we

    app.run(host='0.0.0.0', port=5002)