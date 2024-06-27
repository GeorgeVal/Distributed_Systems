# worker.py
from flask import Flask, request, jsonify
import os
from collections import defaultdict

app = Flask(__name__)

def map_function(value, output_file):
    with open(output_file, 'a') as f:
        for word in value.split():
            f.write(f"{word}\t1\n")

def reduce_function(fkey, values, output_file):
    with open(output_file, 'w') as f:
        f.write(f"{fkey}\t{values}\n")

@app.route('/execute', methods=['POST'])
def execute_task():
    task_type = request.json['task_type']
    input_file_path = request.json['input_file_path']
    try:
        fkey = request.json['key']
    except KeyError:
        if task_type == "reduce":
            return jsonify({"error": "Key for reduce function not provided"}), 404
        
    output_file_path = request.json.get('output_file_path', f"{task_type}_output.txt")

    if not os.path.isfile(input_file_path):
        return jsonify({"error": "File not found"}), 404

    with open(input_file_path, 'r') as f:
        input_data = f.readlines()

    if task_type == "map":
        for line in input_data:
            key, value = line.strip().split("\t", 1)
            map_function(value,output_file_path)
    elif task_type == "reduce":
        current_key = fkey
        current_values = 0
        for line in input_data:
            key, value = line.strip().split("\t", 1)
            if current_key == key:
                current_values +=1
            
        
        reduce_function(fkey, current_values,output_file_path)

    return jsonify({"status": "Task executed successfully"}), 200

if __name__ == "__main__":
        app.run(host='0.0.0.0', port=5004)
