# manager.py

import sys
from flask import Flask, request, jsonify
from kubernetes import client, config

config.load_kube_config()

app = Flask(__name__)

jobs = {}
job_id_counter = 1

# Dummy tokens dictionary to simulate authentication (replace with real auth system)
tokens = {
    "valid_token": "user1"
}

@app.route('/submit_job', methods=['POST'])
def submit_job():
    token = request.headers.get('Authorization')
    if token not in tokens:
        return jsonify({"error": "Unauthorized"}), 403

    job_type = request.form['job_type']
    script = request.form['script']
    input_file = request.files['input_file']

    global job_id_counter
    job_id = job_id_counter
    job_id_counter += 1

    input_file_path = f"/mnt/data/job_{job_id}_input.txt"
    with open(input_file_path, 'wb') as f:
        f.write(input_file.read())

    # Register the job
    jobs[job_id] = {
        "status": "submitted",
        "job_type": job_type,
        "script": script,
        "input_file": input_file_path
    }

    # Assign job to a worker
    assign_job_to_worker(job_id, job_type, script, input_file_path)

    return jsonify({"message": "Job submitted successfully", "job_id": job_id}), 200

def assign_job_to_worker(job_id, job_type, script, input_file):
    # Get a list of available worker pods
    v1 = client.CoreV1Api()
    pods = v1.list_namespaced_pod(namespace='default', label_selector='app=worker')

    if not pods.items:
        jobs[job_id]['status'] = 'failed'
        return

    # Assign the job to the first available worker pod
    worker_pod = pods.items[0].metadata.name

    # Create a Kubernetes job for the worker
    batch_v1 = client.BatchV1Api()
    job_manifest = {
        'apiVersion': 'batch/v1',
        'kind': 'Job',
        'metadata': {
            'name': f"{job_type}-job-{job_id}"
        },
        'spec': {
            'template': {
                'spec': {
                    'containers': [{
                        'name': f"{job_type}-job",
                        'image': 'worker-service:latest',
                        'command': ['python', 'worker.py', job_type],
                        'env': [
                            {'name': 'JOB_SCRIPT', 'value': script},
                            {'name': 'JOB_INPUT', 'value': input_file}
                        ]
                    }],
                    'restartPolicy': 'Never'
                }
            }
        }
    }
    batch_v1.create_namespaced_job(body=job_manifest, namespace='default')
    jobs[job_id]['status'] = 'assigned'

@app.route('/jobs', methods=['GET'])
def get_jobs():
    token = request.headers.get('Authorization')
    if token not in tokens:
        return jsonify({"error": "Unauthorized"}), 403

    return jsonify(jobs), 200

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5002)
