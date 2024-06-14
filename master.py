# master.py

import sys
import subprocess
from kubernetes import client, config
import json

config.load_kube_config()

def schedule_job(mapper, reducer, input_file):
    # Split input file for mapper tasks
    with open(input_file, 'r') as f:
        lines = f.readlines()

    # Assuming we split lines into chunks for simplicity
    chunk_size = len(lines) // 3
    chunks = [lines[i:i + chunk_size] for i in range(0, len(lines), chunk_size)]

    # Create Kubernetes jobs for mapper tasks
    for i, chunk in enumerate(chunks):
        job_name = f"map-task-{i}"
        create_k8s_job(job_name, mapper, "map", chunk)

    # Wait for mapper jobs to complete and gather results
    map_results = gather_map_results()

    # Create Kubernetes jobs for reducer tasks
    create_k8s_job("reduce-task", reducer, "reduce", map_results)

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
                        'stdin': input_data
                    }],
                    'restartPolicy': 'Never'
                }
            }
        }
    }
    batch_v1.create_namespaced_job(body=job_manifest, namespace='default')

def gather_map_results():
    # Placeholder function to gather map results
    return []

if __name__ == "__main__":
    mapper = sys.argv[1]
    reducer = sys.argv[2]
    input_file = sys.argv[3]
    schedule_job(mapper, reducer, input_file)
