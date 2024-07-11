from flask import Flask, request, jsonify, render_template, redirect, url_for, flash
from kubernetes.client.api import core_v1_api
import os, hashlib
from werkzeug.utils import secure_filename
import requests
from pymongo import MongoClient
from kubernetes import client, config
from time import sleep, time
from minio import Minio


app = Flask(__name__)
app.secret_key = 'supersecretkey'  # Change this in a real application

# Get service URLs from environment variables
AUTH_URL = os.getenv('AUTH_URL', 'http://auth-service:5000')

clientM = MongoClient('mongodb://mongodb-service:27777/')


db = clientM['auth_db']
users_collection = db['users']
tokens_collection = db['tokens']

db2 = clientM['jobsDB']
job_collection = db2['jobs']

db3 = clientM['Manager_db']
jobs_data_collection = db3['jobs']


clientMinio = Minio(
        "minio:9000",
        access_key="minio",
        secret_key="minio123",
        secure=False
    )


global jobId
jobId = 1

def uploadFile(source_file, bucket_name, destination_file):


    # Make the bucket if it doesn't exist.
    found = clientMinio.bucket_exists(bucket_name)
    if not found:
         clientMinio.make_bucket(bucket_name)

    # Upload the file, renaming it in the process
    clientMinio.fput_object(
        bucket_name, destination_file, source_file,
    )

def authenticate(token,user_type):
   
    token_entry = tokens_collection.find_one({'token': token})

    if  token_entry==None:
        return 'Token not Found'
   
    user_entry = users_collection.find_one({'username': token_entry['username']})

    
    
    if user_type == 'admin' and user_entry['role'] != "admin":
        return 'Unauthorized'
    
    
    return 1

def auth_login(username,password):
   
        user_entry = users_collection.find_one({'username': username})

        if user_entry==None or user_entry['password'] != hashlib.sha256(password.encode()).hexdigest():
            return "Invalid credentials"
        else:
            return 1
    
def create_manager_statefulset_and_service(jobId):
    

    config.load_incluster_config()

    # Load Kubernetes configuration
    core_v1 = core_v1_api.CoreV1Api()

    # Create pod manifest
    pod_manifest = {
        'apiVersion': 'v1',
        'kind': 'Pod',
        'metadata': {
            'name': f'manager{jobId}',
            'labels': {
                'app': 'manager'
            }
        },
        'spec': {
            'serviceAccountName': 'ui-account',
            'containers': [{
                'name': 'manager',
                'image': 'georgeval/manager-service:latest',
                'ports': [{
                    'containerPort': 5002
                }]
            }]
        }
    }
    with open(f'2.txt', 'w') as f:
                    f.write(f"\n")
    # Create the pod
    core_v1.create_namespaced_pod(body=pod_manifest,
        namespace='default')
    sleep(5)
    with open(f'3.txt', 'w') as f:
                    f.write(f"\n")

    # Create a service manifest
    service_manifest = {
        'apiVersion': 'v1',
        'kind': 'Service',
        'metadata': {
            'name': f'manager{jobId}-service'
        },
        'spec': {
            'selector': {
                'app': f'manager'
            },
            'ports': [{
                'protocol': 'TCP',
                'port': 5002,
                'targetPort': 5002
            }],
            'type': 'ClusterIP'
        }
    }

    # Create the service
    core_v1.create_namespaced_service(namespace='default', body=service_manifest)
    with open(f'4.txt', 'w') as f:
                    f.write(f"\n")
    sleep(5)
    
    # # Define the StatefulSet manifest
    # statefulset = {
    #     "apiVersion": "apps/v1",
    #     "kind": "StatefulSet",
    #     "metadata": {"name": "manager"},
    #     "spec": {
    #         "serviceName": "manager",
    #         "replicas": 3,
    #         "selector": {"matchLabels": {"app": "manager"}},
    #         "template": {
    #             "metadata": {"labels": {"app": "manager"}},
    #             "spec": {
    #                 "serviceAccountName": "ui-account",
    #                 "containers": [{
    #                     "name": "manager",
    #                     "image": "kfarmakis/manager-service:latest",
    #                     "volumeMounts": [{"name": "data-storage", "mountPath": "/data/pv0001/"}],
    #                     "ports": [{"containerPort": 5002}]
    #                 }],
    #                 "volumes": [{
    #                     "name": "data-storage",
    #                     "persistentVolumeClaim": {"claimName": "data-minio-0"}
    #                 }]
    #             }
    #         },
    #         "volumeClaimTemplates": [
    #             {
    #                 "metadata": {"name": "data-minio-0"},
    #                 "spec": {
    #                     "accessModes": ["ReadWriteMany"],
    #                     "resources": {"requests": {"storage": "100M"}}
    #                 }
    #             },
    #             {
    #                 "metadata": {"name": "data-minio-1"},
    #                 "spec": {
    #                     "accessModes": ["ReadWriteMany"],
    #                     "resources": {"requests": {"storage": "100M"}}
    #                 }
    #             },
    #             {
    #                 "metadata": {"name": "data-minio-2"},
    #                 "spec": {
    #                     "accessModes": ["ReadWriteMany"],
    #                     "resources": {"requests": {"storage": "100M"}}
    #                 }
    #             }
    #         ]
    #     }
    # }

    # # Create the StatefulSet
    # api_instance = client.AppsV1Api()
    # api_instance.create_namespaced_stateful_set(namespace="default", body=statefulset)

    # # Create a Service for the manager StatefulSet
    # service = {
    #     "apiVersion": "v1",
    #     "kind": "Service",
    #     "metadata": {"name": "manager-service"},
    #     "spec": {
    #         "selector": {"app": "manager"},
    #         "ports": [{"protocol": "TCP", "port": 5002, "targetPort": 5002}]
    #     }
    # }

    # api_instance = client.CoreV1Api()
    #api_instance.create_namespaced_service(namespace="default", body=service)

    

def get_manager_service_url(manager_service):
    api_instance = client.CoreV1Api()
    service = api_instance.read_namespaced_service(name=manager_service, namespace="default")
    ip = service.spec.cluster_ip
    port = service.spec.ports[0].port
    return f"http://{ip}:{port}"

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/admin/register', methods=['GET', 'POST'])
def register_user():
    if request.method == 'POST':
        data = request.form
        token = data.get('token')
        auth = authenticate(token,'admin')
        if  auth!= 1:
            flash(auth, 'error')
            return redirect(url_for('register_user'))

        username = data.get('username')
        password = data.get('password')
        role = data.get('role')
        headers = {'Authorization': token}
        response = requests.post(f"{AUTH_URL}/register", headers=headers, json={"username": username, "password": password, "role": role})
        flash(response.json())
        return redirect(url_for('register_user'))
    return render_template('register.html')

@app.route('/admin/login', methods=['GET', 'POST'])
def login_user():
    if request.method == 'POST':
        data = request.form
        username = data.get('username')
        password = data.get('password')
        auth = auth_login(username,password)
        if  auth!= 1:
            flash(auth, 'error')
            return redirect(url_for('login_user'))
        response = requests.post(f"{AUTH_URL}/login", json={"username": username, "password": password})
        flash(response.json())
        if response.status_code == 200:
            token = response.json().get("token")
            flash(f"Login successful, token: {token}")
        return redirect(url_for('login_user'))
    return render_template('login.html')

@app.route('/admin/delete_user', methods=['GET', 'POST'])
def delete_user():
    if request.method == 'POST':
        data = request.form
        token = data.get('token')
        auth = authenticate(token,'admin')
        if  auth!= 1:
            flash(auth, 'error')
            return redirect(url_for('delete_user'))

        username = data.get('username')
        headers = {'Authorization': token}
        response = requests.delete(f"{AUTH_URL}/delete_user", headers=headers, json={"username": username})
        flash(response.json())
        return redirect(url_for('delete_user'))
    return render_template('delete_user.html')

@app.route('/jobs/submit', methods=['GET', 'POST'])
def submit_job():
    jobId = None
    if request.method == 'POST':
        data = request.form
        token = data.get('token')



        # Retrieve the largest jobId from MongoDB
        largest_job = job_collection.find_one(sort=[("jobId", -1)])
        if largest_job:
            jobId = largest_job['jobId'] + 1
        else:
            jobId = 1

        # Insert the new jobId into MongoDB
        job_collection.insert_one({"jobId": jobId})


        auth = authenticate(token,'user')
        if  auth!= 1:
           flash(auth, 'error')
           return redirect(url_for('submit_job'))
        with open(f'0.txt', 'w') as f:
                    f.write(f"jobId\n")
        with open(f'1.txt', 'w') as f:
                    f.write(f"{jobId}\n")
        create_manager_statefulset_and_service(jobId)
        with open(f'9.txt', 'w') as f:
                            f.write(f"{jobId}\n")
        namespace = "default"
        
        #"http://manager{jobId}-service.{namespace}.svc.cluster.local:5004/execute"
        with open(f'30.txt', 'w') as f:
                            f.write(f"{jobId}\n")
        mappers = data.get('mappers')
        with open(f'31.txt', 'w') as f:
                            f.write(f"{jobId}\n")
        reducers = data.get('reducers')
        with open(f'32.txt', 'w') as f:
                            f.write(f"{jobId}\n")
        map_script = data.get('map_script')
        with open(f'33.txt', 'w') as f:
                            f.write(f"{jobId}\n")
        reduce_script = data.get('reduce_script')
        with open(f'34.txt', 'w') as f:
                            f.write(f"{jobId}\n")
        #input_file = data.get('file_path')
        
        file = request.files['file']

        filename = secure_filename(file.filename)

        file_path = os.path.join('', filename) 

        file.save(file_path)

        uploadFile(filename, "input-bucket", f"input{jobId}.txt")

        with open(f'test.txt', 'w') as f:
                            f.write(f"{filename}\n")

        headers = {'Authorization': token, "Content-Type":"application/json"}
        sleep(4)    
        response = requests.post(f"http://manager{jobId}-service.{namespace}.svc.cluster.local:5002/submit_job", headers=headers, json={"job_id": jobId, "map_script":map_script, "reduce_script":reduce_script,"job_type": "map-reduce", "mappers": mappers, "reducers": reducers})
        if response.status_code ==200:
            v1=client.CoreV1Api()
            v1.delete_namespaced_pod(name=f"manager{jobId}",namespace=namespace)
            v1.delete_namespaced_service(name=f"manager{jobId}-service",namespace=namespace)
        jobId = jobId + 1
        flash(response.json())
        return redirect(url_for('submit_job'))
    return render_template('submit.html')

@app.route('/jobs/view_jobs', methods=['GET', 'POST'])
def view_jobs():
    if request.method == 'POST':
        token = request.form['token']
        auth = authenticate(token,'user')
        if  auth!= 1:
            flash(auth, 'error')
            return redirect(url_for('view_jobs'))

        jobs = list(jobs_data_collection.find({}, {'_id': False}))
        flash(jobs)
        return redirect(url_for('view_jobs'))
    return render_template('view.html')

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5003)