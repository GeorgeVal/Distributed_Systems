# worker.py
from uu import Error
from flask import Flask, request, jsonify
import os, math, re
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


def download_from_minio(bucket_name, object_name, file_path, safe=True):
    if safe:
        try:
            clientMinio.fget_object(bucket_name, object_name, file_path)
            return True
        except S3Error as e:
            raise Exception(f"Failed to download file from MinIO: {e}")
    else:
        try:
            clientMinio.fget_object(bucket_name, object_name, file_path)
            return True
        except:
            return False

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
    keys_per_file = round(total_keys / num_shufflers)
    
    shuffle_files = []
    count = 0
    for i in range(num_shufflers):
        start_index = i * keys_per_file
        end_index = start_index + keys_per_file
        keys_slice = keys[start_index:end_index]
        
        if keys_slice:  # Only write if the slice is not empty
            count += 1
            output_file = f"{shuffle_output_prefix}_{i}.txt"
            shuffle_files.append(output_file)
            with open(output_file, 'w') as f:
                for key in keys_slice:
                    f.write(f"{key}\t{sum(shuffle_dict[key])}\n")
    return shuffle_files, count


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
        minio_file = f"map{str(pod_name).replace('-reducer', '')}-shuffle-output_{i}.txt"
        local_input_file = f"/tmp/{pod_name}-output_{i}.txt"
        while download_from_minio(bucket_name, minio_file, local_input_file, False):
                
                i = i+1
                minio_file = f"map{pod_name}-shuffle-output_{i}.txt"
                local_input_file = f"/tmp/{pod_name}-output_{i}.txt"

                if i==0:
                    raise Error("why is i still 0")



    

    
    local_output_file = f"/tmp/{task_type}_output.txt"
    output_bucket_name = f'{task_type}-bucket'


    if task_type == "map":
        output_object_prefix = task_type + str(pod_name)
        
        local_input_file = f"/tmp/{input_file_path}"

        # Download input file from MinIO
        try:
            download_from_minio(bucket_name, input_file_path, local_input_file)
        except Exception as e:
            return jsonify({"error": str(e)}), 500

        


    if os.path.isfile(local_input_file) and task_type=="map":
        with open(local_input_file, 'r') as f:
            input_data = f.read()  # Read entire file content as plain text

        map_function(input_data, local_output_file)

         # Shuffle the map output into multiple files
        num_shufflers =  4#request.json.get('num_shufflers', 1)
        shuffle_output_prefix = "shuffle-output"
        shuffle_files, count = shuffle_function(local_output_file, num_shufflers, shuffle_output_prefix)

        # Upload shuffled files to MinIO
        shuffle_object_names = []
        for shuffle_file in shuffle_files:
            shuffle_object_name = f"{output_object_prefix}-{os.path.basename(shuffle_file)}"
            try:
                upload_to_minio(output_bucket_name, shuffle_object_name, shuffle_file)
                shuffle_object_names.append(shuffle_object_name)
            except Exception as e:
                return jsonify({"error": str(e)}), 500
            
        return jsonify({"status": "Map task executed and shuffled successfully", "shuffle_output_files": count}), 200
    
    elif task_type == "reduce":
        
        total_data = ""

        
        local_input_file = f"/tmp/{pod_name}-output_0.txt"

        with open(local_input_file, 'r') as f:
            input_data = f.read()


        #fkey = total_data[0].strip().split(maxsplit=1)[0]
        fkey = input_data.strip().split("\t")[0]
    
        current_key = fkey
        current_values = 0


        for j in range(i):
            local_input_file = f"/tmp/{pod_name}-output_{j}.txt"
            file =  open(local_input_file, 'r')
            input_data = file.readlines()

            for line in input_data:
                try:
                    key, value = line.strip().split("\t", 1)
                    if current_key == key:
                        current_values += int(value)
                except ValueError:
                    raise ValueError(f"{input_data}, line:{line}, f:{f}, total_data:{total_data}")
            
            
        
        reduce_function(fkey, current_values,local_output_file)

        # Upload the reduce output to MinIO
        reduce_object_name = f"{pod_name}-reduce-{fkey}.txt"
        try:
            upload_to_minio(output_bucket_name, reduce_object_name, local_output_file)
        except Exception as e:
            return jsonify({"error": str(e)}), 500

        return jsonify({"status": "Reduce task executed successfully"}), 200
    
    return jsonify({"status": "Something bad happened"}), 403


if __name__ == "__main__":
        app.run(host='0.0.0.0', port=5004)
