from flask import Flask, request, jsonify, render_template, redirect, url_for, flash
import os, hashlib
import requests
from pymongo import MongoClient
from kubernetes import client, config
from time import sleep, time

app = Flask(__name__)
app.secret_key = 'supersecretkey'  # Change this in a real application

# Get service URLs from environment variables
AUTH_URL = os.getenv('AUTH_URL', 'http://auth-service:5000')
MANAGER_URL = os.getenv('MANAGER_URL', 'http://manager-service:5002')

clientM = MongoClient('mongodb://mongodb-service:27777/')
db = clientM['auth_db']
users_collection = db['users']
tokens_collection = db['tokens']

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
    
def create_manager_statefulset_and_service():
    # Load Kubernetes configuration
    config.load_incluster_config()

    
    # Define the StatefulSet manifest
    statefulset = {
        "apiVersion": "apps/v1",
        "kind": "StatefulSet",
        "metadata": {"name": "manager"},
        "spec": {
            "serviceName": "manager",
            "replicas": 1,
            "selector": {"matchLabels": {"app": "manager"}},
            "template": {
                "metadata": {"labels": {"app": "manager"}},
                "spec": {
                    "containers": [{
                        "name": "manager",
                        "image": "georgeval/manager-service:latest",
                        "volumeMounts": [{"name": "data-storage", "mountPath": "/data/pv0001/"}],
                        "ports": [{"containerPort": 5002}]
                    }],
                    "volumes": [{
                        "name": "data-storage",
                        "persistentVolumeClaim": {"claimName": "data-minio-0"}
                    }]
                }
            },
            "volumeClaimTemplates": [
                {
                    "metadata": {"name": "data-minio-0"},
                    "spec": {
                        "accessModes": ["ReadWriteMany"],
                        "resources": {"requests": {"storage": "100M"}}
                    }
                },
                {
                    "metadata": {"name": "data-minio-1"},
                    "spec": {
                        "accessModes": ["ReadWriteMany"],
                        "resources": {"requests": {"storage": "100M"}}
                    }
                },
                {
                    "metadata": {"name": "data-minio-2"},
                    "spec": {
                        "accessModes": ["ReadWriteMany"],
                        "resources": {"requests": {"storage": "100M"}}
                    }
                }
            ]
        }
    }

    # Create the StatefulSet
    api_instance = client.AppsV1Api()
    api_instance.create_namespaced_stateful_set(namespace="default", body=statefulset)

    # Create a Service for the manager StatefulSet
    service = {
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {"name": "manager-service"},
        "spec": {
            "selector": {"app": "manager"},
            "ports": [{"protocol": "TCP", "port": 5002, "targetPort": 5002}]
        }
    }

    api_instance = client.CoreV1Api()
    api_instance.create_namespaced_service(namespace="default", body=service)

    

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
    if request.method == 'POST':
        data = request.form
        token = data.get('token')
        auth = authenticate(token,'user')
        if  auth!= 1:
            flash(auth, 'error')
            return redirect(url_for('submit_job'))

        create_manager_statefulset_and_service()

        print("Sleeping..")
        sleep(15)

        manager_url = get_manager_service_url("manager-service")

        mappers = data.get('mappers')
        reducers = data.get('reducers')
        map_script = data.get('map_script')
        reduce_script = data.get('reduce_script')
        job_type = data.get('job_type')
        input_file = data.get('file_path')
        headers = {'Authorization': token, "Content-Type":"application/json"}
        response = requests.post(f"{manager_url}/submit_job", headers=headers, json={"job_type":job_type, "map_script":map_script, "reduce_script":reduce_script,"input_file":input_file, "mappers": mappers, "reducers": reducers})
        flash(response.json())
        return redirect(url_for('submit_job'))
    return render_template('submit.html')

@app.route('/jobs/view', methods=['GET', 'POST'])
def view_jobs():
    if request.method == 'POST':
        token = request.form['token']
        auth = authenticate(token,'user')
        if  auth!= 1:
            flash(auth, 'error')
            return redirect(url_for('view_jobs'))

        headers = {'Authorization': token}
        response = requests.get(f"{MANAGER_URL}/jobs", headers=headers)
        flash(response.json())
        return redirect(url_for('view_jobs'))
    return render_template('view_jobs.html')

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5003)
