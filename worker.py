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
    
@app.route('/finalReduce', methods=['POST'])
def finalReduce():
    with open("0.txt", 'w') as f:
            f.write("lol")
    bucket_name = "reduce-bucket"
    objects = clientMinio.list_objects(bucket_name, recursive=True)
    reducers = request.json['reducers'] 
    jobId = request.json["job_id"]

    for i in range(0, reducers):
        with open(f"test{i}.txt", 'w') as f:
            f.write("lol")
        localFilePath = f"reduce-{i}.txt"
        minioFilePath = f"worker-pod{jobId}{convert_to_two_digit_string(i)}-reducer-reduce-output.txt"
        download_from_minio(bucket_name, minioFilePath, localFilePath)


    with open('combined.txt', 'w') as outfile:
        for i in range(0, reducers):
            with open(f"reduce-{i}.txt") as infile:
                outfile.write(infile.read())
    output_data = reduce_full_function("combined.txt")
    with open("final-reduct.txt", 'w') as f:
            f.write(output_data)
    
    upload_to_minio("reduce-bucket", f"final-output{jobId}.txt", "final-reduct.txt")
    return jsonify({"status": "Final reduce task executed successfully"}), 200


def reduce_full_function(file):

    with open(file, 'r') as f:
        input_data = f.readlines()

    # Initialize an empty dictionary to store results
    result = {}

    # Process each line
    for line in input_data:
        # Split the line into word and value using tab character as delimiter
        parts = line.strip().split(':')
        
        if len(parts) == 2:
            word, value_str = parts
            value = int(value_str)
            
            # Accumulate the value for the word in the dictionary
            if word in result:
                result[word] += value
            else:
                result[word] = value
    output_string = ""
    for word, total_value in result.items():
        output_string += f"{word}: {total_value}\n"

    return output_string.strip()+"\n"

def reduce_function(file):
    with open("1.txt", 'w') as f:
            f.write("lol")
    with open(file, 'r') as f:
        input_data = f.readlines()

    with open("2.txt", 'w') as f:
            f.write("lol")
    # Initialize an empty dictionary to store results
    result = {}

    # Process each line
    for line in input_data:
        # Split the line into word and value using tab character as delimiter
        parts = line.strip().split('\t')
        
        if len(parts) == 2:
            word, value_str = parts
            value = int(value_str)
            
            # Accumulate the value for the word in the dictionary
            if word in result:
                result[word] += value
            else:
                result[word] = value
    with open("3.txt", 'w') as f:
            f.write("lol")
    output_string = ""
    for word, total_value in result.items():
        output_string += f"{word}: {total_value}\n"
    with open("4.txt", 'w') as f:
            f.write("lol")
    return output_string.strip()+"\n"

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


def extract_last_two_digits(string):
    parts = string.split('-')
    if len(parts) > 1:
        number_str = parts[1][-2:]
        return int(number_str)
    return None


def convert_to_two_digit_string(number):
    if 0 <= number <= 9:
        return f'0{number}'
    elif 10 <= number <= 99:
        return str(number)  # If already two digits, just convert to string
    else:
        raise ValueError("Input number must be between 0 and 99 inclusive.")


@app.route('/execute', methods=['POST'])
def execute_task():
    task_type = request.json['task_type']
    input_file_path = request.json['input_file_path']
    pod_name = request.json['pod_name']
    reducers = request.json['reducers']
    mappers = request.json['mappers']
    jobId = request.json["job_id"]

    pod_index = extract_last_two_digits(pod_name)
 
    with open(f'mphke{pod_name}', 'w') as f:
            f.write(task_type)
        

    i = 0

    if task_type == 'map':
        bucket_name = 'input-bucket' 
    else:
        pod_name = pod_name[1:]
        bucket_name = 'map-bucket'
        minio_file = f"map{str(pod_name).replace('-reducer', '')}-shuffle-output_{i}.txt"
        local_input_file = f"{pod_name}-output_{i}.txt"
        
        with open(f'{local_input_file}', 'w') as f:
            f.write("")
        
        for i in range(0, mappers):
            try:
                minio_file = f"mapworker-pod{jobId}{convert_to_two_digit_string(i)}-shuffle-output_{pod_index}.txt"
                local_input_file = f"{pod_name}-output_{i}.txt"
                download_from_minio(bucket_name, minio_file, local_input_file)
                
                
            except Exception as e:
                with open(f'error.txt', 'w') as f:
                    f.write(f"{bucket_name}\n{minio_file}\n{local_input_file}")
                return jsonify({"error": str(e)}), 500
        
        # while download_from_minio(bucket_name, minio_file, local_input_file) != False:
        #     with open(f'ekane-download.txt', 'w') as f:
        #         f.write(f"den skaei error")            
        #     i = i+1
        #     minio_file = f"mapworker-pod1{convert_to_two_digit_string(i)}-shuffle-output_{pod_index}.txt"
        #     local_input_file = f"{pod_name}-output_{i}.txt"

        #     if i==0:
        #         with open(f'debug-error-{pod_name}.txt', 'w') as f:
        #             f.write(f"skaei error\n")
        #             f.write(f"{bucket_name}\n")
        #             f.write(f"{minio_file}\n")
        #         raise Error("why is i still 0")


    with open(f'debug-reduce2-{pod_name}.txt', 'w') as f:
            f.write(f"den skaei error")
    

    
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
        num_shufflers =  reducers
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
            
        return jsonify({"status": "Map task executed and shuffled successfully", "shuffle_output_files": count, "flag": "map"}), 200
    
    elif task_type == "reduce":
        
        output_data = ""

        with open('looptest.txt', 'w') as f:
                    f.write(f"print to for {mappers} me name {pod_name}\n")   
        index = extract_last_two_digits(pod_name)
        for i in range(0,mappers):
            local_input_file = f"{pod_name}-output_{i}.txt"

            output_data += reduce_function(local_input_file)
        
        with open('looptestTelos.txt', 'w') as f:
                    f.write(f"print to for {mappers}\n")   

        local_output_file = f"{pod_name}-reduce-output.txt"

        with open(local_output_file, 'w') as f:
            f.write(output_data)

        # Upload the reduce output to MinIO
        reduce_object_name = f"{pod_name}-reduce-output.txt"

        upload_to_minio("reduce-bucket", reduce_object_name, local_output_file)


        return jsonify({"status": "Reduce task executed successfully", "flag": "reduce"}), 200
    
    return jsonify({"status": "Something bad happened"}), 403


if __name__ == "__main__":
        app.run(host='0.0.0.0', port=5004)