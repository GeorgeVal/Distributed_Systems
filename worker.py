# worker.py
from flask import Flask, request, jsonify
import os, math
from collections import defaultdict
from minio import Minio
from minio.error import S3Error

app = Flask(__name__)

clientMinio = Minio(
        "minio:9000",
        access_key="minio",
        secret_key="minio123",
        secure=False
    )


def download_from_minio(bucket_name, object_name, file_path):
    try:
        clientMinio.fget_object(bucket_name, object_name, file_path)
    except S3Error as e:
        raise Exception(f"Failed to download file from MinIO: {e}")

def upload_to_minio(bucket_name, object_name, file_path):
    try:
        clientMinio.fput_object(bucket_name, object_name, file_path)
    except S3Error as e:
        raise Exception(f"Failed to upload file to MinIO: {e}")
    

def map_function(value, output_file):
    with open(output_file, 'a') as f:
        for word in value.split():
            f.write(f"{word}\t1\n")

def reduce_function(fkey, values, output_file):
    with open(output_file, 'w') as f:
        f.write(f"{fkey}\t{values}\n")

def shuffle_function(input_file, num_shufflers, shuffle_output_prefix):
    shuffle_dict = defaultdict(list)
    with open(input_file, 'r') as f:
        for line in f:
            key, value = line.strip().split("\t", 1)
            shuffle_dict[key].append(int(value))
    
    keys = list(shuffle_dict.keys())
    total_keys = len(keys)
    keys_per_file = math.ceil(total_keys / num_shufflers)
    
    shuffle_files = []
    for i in range(num_shufflers):
        output_file = f"{shuffle_output_prefix}_{i}.txt"
        shuffle_files.append(output_file)
        with open(output_file, 'w') as f:
            for key in keys[i * keys_per_file:(i + 1) * keys_per_file]:
                f.write(f"{key}\t{sum(shuffle_dict[key])}\n")
    return shuffle_files


@app.route('/execute', methods=['POST'])
def execute_task():
    task_type = request.json['task_type']
    input_file_path = request.json['input_file_path']
    pod_name = request.json['pod_name']

    i = 0

    if task_type == 'map':
        bucket_name = 'input-bucket' 
    else:
        bucket_name = 'map-bucket'
        minio_file = f"map{pod_name}-shuffle-output_{i}.txt"
        local_input_file = f"/tmp/{pod_name}-output_{i}.txt"
        while download_from_minio(bucket_name, minio_file, local_input_file):
            i = i+1
            minio_file = f"map{pod_name}-shuffle-output_{i}.txt"
            local_input_file = f"/tmp/{pod_name}-reduce-output_{i}.txt"



    

    

    if task_type == "map":
        output_bucket_name = f'{task_type}-bucket'
        output_object_prefix = task_type + str(pod_name)
        
        local_input_file = f"/tmp/{input_file_path}"
        local_output_file = f"/tmp/{task_type}_output.txt"

        # Download input file from MinIO
        try:
            download_from_minio(bucket_name, input_file_path, local_input_file)
        except Exception as e:
            return jsonify({"error": str(e)}), 500

        


    if not os.path.isfile(local_input_file) and task_type=="=map":
        with open(local_input_file, 'r') as f:
            input_data = f.read()  # Read entire file content as plain text

        map_function(input_data, local_output_file)

         # Shuffle the map output into multiple files
        num_shufflers =  4#request.json.get('num_shufflers', 1)
        shuffle_output_prefix = "shuffle-output"
        shuffle_files = shuffle_function(local_output_file, num_shufflers, shuffle_output_prefix)

        # Upload shuffled files to MinIO
        shuffle_object_names = []
        for shuffle_file in shuffle_files:
            shuffle_object_name = f"{output_object_prefix}-{os.path.basename(shuffle_file)}"
            try:
                upload_to_minio(output_bucket_name, shuffle_object_name, shuffle_file)
                shuffle_object_names.append(shuffle_object_name)
            except Exception as e:
                return jsonify({"error": str(e)}), 500
            
        return jsonify({"status": "Map task executed and shuffled successfully", "shuffle_output_files": shuffle_object_names}), 200
    
    elif task_type == "reduce":
        
        total_data = ""

        for j in range(i):
            local_input_file = f"/tmp/{pod_name}-reduce-output_{j}.txt"

            with open(local_input_file, 'r') as f:
                input_data = f.read()

            total_data = total_data + input_data
        print(total_data)

        fkey = total_data[0].strip().split(maxsplit=1)[0]
    
        current_key = fkey
        current_values = 0
        for line in input_data:
            key, value = line.strip().split("\t", 1)
            if current_key == key:
                current_values = value
            
        
        reduce_function(fkey, current_values,local_output_file)

        # Upload the reduce output to MinIO
        reduce_object_name = f"{output_object_prefix}-{fkey}.txt"
        try:
            upload_to_minio(output_bucket_name, reduce_object_name, local_output_file)
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    return jsonify({"status": "Task executed successfully"}), 200

if __name__ == "__main__":
        app.run(host='0.0.0.0', port=5004)
