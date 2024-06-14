# worker.py
from flask import Flask, request, jsonify
import sys
from collections import defaultdict

app = Flask(__name__)

def map_function(key, value):
    for word in value.split():
        print(f"{word}\t1")

def reduce_function(key, values):
    total = sum(int(v) for v in values)
    print(f"{key}\t{total}")

@app.route('/execute', methods=['POST'])
def execute_task():
    task_type = request.json['task_type']
    input_data = request.json['input_data']

    if task_type == "map":
        for line in input_data:
            key, value = line.strip().split("\t", 1)
            map_function(key, value)
    elif task_type == "reduce":
        current_key = None
        current_values = []
        for line in input_data:
            key, value = line.strip().split("\t", 1)
            if current_key == key:
                current_values.append(value)
            else:
                if current_key:
                    reduce_function(current_key, current_values)
                current_key = key
                current_values = [value]
        if current_key:
            reduce_function(current_key, current_values)

if __name__ == "__main__":
        app.run(host='0.0.0.0', port=5003)
