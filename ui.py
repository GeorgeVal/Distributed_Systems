# ui.py

import sys
import requests

AUTH_URL = 'http://auth-service:5000'
MASTER_URL = 'http://master-service:5000'

def register(token, username, password, role):
    headers = {"Authorization": token}
    response = requests.post(f"{AUTH_URL}/register", headers=headers, json={"username": username, "password": password, "role": role})
    print(response.json())

def login(username, password):
    response = requests.post(f"{AUTH_URL}/login", json={"username": username, "password": password})
    if response.status_code == 200:
        token = response.json().get("token")
        print(f"Login successful, token: {token}")
        return token
    else:
        print(response.json())
        return None

def delete_user(token, username):
    headers = {"Authorization": token}
    response = requests.delete(f"{AUTH_URL}/delete_user", headers=headers, json={"username": username})
    print(response.json())

def submit_job(token, mapper, reducer, input_file):
    headers = {"Authorization": token}
    files = {'input_file': open(input_file, 'rb')}
    response = requests.post(f"{MASTER_URL}/submit_job", headers=headers, files=files, data={"mapper": mapper, "reducer": reducer})
    print(response.json())

def view_jobs(token):
    headers = {"Authorization": token}
    response = requests.get(f"{MASTER_URL}/jobs", headers=headers)
    print(response.json())

if __name__ == "__main__":
    command = sys.argv[1]
    if command == "register":
        token = sys.argv[2]
        username = sys.argv[3]
        password = sys.argv[4]
        role = sys.argv[5]
        register(token, username, password, role)
    elif command == "login":
        username = sys.argv[2]
        password = sys.argv[3]
        login(username, password)
    elif command == "delete_user":
        token = sys.argv[2]
        username = sys.argv[3]
        delete_user(token, username)
    elif command == "submit_job":
        token = sys.argv[2]
        mapper = sys.argv[3]
        reducer = sys.argv[4]
        input_file = sys.argv[5]
        submit_job(token, mapper, reducer, input_file)
    elif command == "view_jobs":
        token = sys.argv[2]
        view_jobs(token)
