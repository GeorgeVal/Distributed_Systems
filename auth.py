#auth.py
import sys
import subprocess
from kubernetes import client, config
from flask import Flask, request, jsonify
from pymongo import MongoClient
import hashlib
import uuid

app = Flask(__name__)

# Initialize MongoDB client
client = MongoClient('mongodb://mongodb-service:27777/')
db = client['auth_db']
users_collection = db['users']
tokens_collection = db['tokens']

# Insert admin user if not exists
if users_collection.count_documents({'username': 'admin'}) == 0:
    users_collection.insert_one({
        'username': 'admin',
        'password': hashlib.sha256("pass".encode()).hexdigest(),
        'role': 'admin'
    })



def generate_token(username):
    return hashlib.sha256(f"{username}{uuid.uuid4()}".encode()).hexdigest()

@app.route('/register', methods=['POST'])
def register():
    token = request.headers.get('Authorization')
    token_entry = tokens_collection.find_one({'token': token})
    user_entry = users_collection.find_one({'username': token_entry['username']})
    
    if not token_entry:
        return jsonify({"error": "Token Not Found"}), 403
    
    if user_entry['role'] != "admin":
        return jsonify({"error": "Unauthorized"}), 403

    data = request.json
    username = data['username']
    password = data['password']
    role = data['role']
    
    if users_collection.find_one({'username': username}):
        return jsonify({"error": "User already exists"}), 400
    
    users_collection.insert_one({
        'username': username,
        'password': hashlib.sha256(password.encode()).hexdigest(),
        'role': role
    })
    
    return jsonify({"message": "User registered successfully"}), 201

@app.route('/login', methods=['POST'])
def login():
    data = request.json
    username = data['username']
    password = data['password']
    user_entry = users_collection.find_one({'username': username})
    
    if not user_entry or user_entry['password'] != hashlib.sha256(password.encode()).hexdigest():
        return jsonify({"error": "Invalid credentials"}), 401
    token = generate_token(username)

    tokens_collection.insert_one({'token': token, 'username': username})
    
    return jsonify({"token": token}), 200

@app.route('/delete_user', methods=['DELETE'])
def delete_user():
    token = request.headers.get('Authorization')
    token_entry = tokens_collection.find_one({'token': token})
    if not token_entry or users_collection.find_one({'username': token_entry['username']})['role'] != 'admin':
        return jsonify({"error": "Unauthorized"}), 403

    data = request.json
    username = data['username']
    
    if not users_collection.find_one({'username': username}):        
        return jsonify({"error": "User does not exist"}), 400
    users_collection.delete_one({'username': username})

    return jsonify({"message": "User deleted successfully"}), 200


jobs = {}
job_id_counter = 1


@app.route('/submit_job', methods=['POST'])
def submit_job():
    token = request.headers.get('Authorization')
    if token not in tokens:
        return jsonify({"error": "Unauthorized"}), 403

    mapper = request.form['mapper']
    reducer = request.form['reducer']
    input_file = request.files['input_file']

    global job_id_counter
    job_id = job_id_counter
    job_id_counter += 1

    input_file_path = f"/mnt/data/job_{job_id}_input.txt"
    with open(input_file_path, 'wb') as f:
        f.write(input_file.read())

    # Split input file for mapper tasks
    with open(input_file_path, 'r') as f:
        lines = f.readlines()

    chunk_size = len(lines) // 3
    chunks = [lines[i:i + chunk_size] for i in range(0, len(lines), chunk_size)]

    for i, chunk in enumerate(chunks):
        job_name = f"map-task-{job_id}-{i}"
        create_k8s_job(job_name, mapper, "map", chunk)

    # Register the job
    jobs[job_id] = {
        "status": "submitted",
        "mapper": mapper,
        "reducer": reducer,
        "input_file": input_file_path
    }

    return jsonify({"message": "Job submitted successfully", "job_id": job_id}), 200

@app.route('/jobs', methods=['GET'])
def get_jobs():
    token = request.headers.get('Authorization')
    if token not in tokens:
        return jsonify({"error": "Unauthorized"}), 403

    return jsonify(jobs), 200

def create_k8s_job(job_name, function, task_type, input_data):
    batch_v1 = client.BatchV1Api()
    job_manifest = {
        'apiVersion': 'batch/v1',
        'kind': 'Job',
        'metadata': {
            'name': job_name
        },
        'spec': {
            'template': {
                'spec': {
                    'containers': [{
                        'name': job_name,
                        'image': 'worker:latest',
                        'command': ['python', 'worker.py', task_type],
                        'stdin': ''.join(input_data)
                    }],
                    'restartPolicy': 'Never'
                }
            }
        }
    }
    batch_v1.create_namespaced_job(body=job_manifest, namespace='default')

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000)
