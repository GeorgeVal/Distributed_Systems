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
    
    token = generate_token(username)

    tokens_collection.insert_one({'token': token, 'username': username})
    
    return jsonify({"token": token}), 200

@app.route('/delete_user', methods=['DELETE'])
def delete_user():
   
    data = request.json
    username = data['username']
    
    if not users_collection.find_one({'username': username}):        
        return jsonify({"error": "User does not exist"}), 400
    users_collection.delete_one({'username': username})

    return jsonify({"message": "User deleted successfully"}), 200
