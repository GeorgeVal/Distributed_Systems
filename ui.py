from flask import Flask, request, jsonify, render_template, redirect, url_for, flash
import os
import requests

app = Flask(__name__)
app.secret_key = 'supersecretkey'  # Change this in a real application

# Get service URLs from environment variables
AUTH_URL = os.getenv('AUTH_URL', 'http://auth-service:5000')
MANAGER_URL = os.getenv('MANAGER_URL', 'http://manager-service:5001')

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/admin/register', methods=['GET', 'POST'])
def register_user():
    if request.method == 'POST':
        data = request.form
        token = data.get('token')
        username = data.get('username')
        password = data.get('password')
        role = data.get('role')
        headers = {'Authorization': token}
        response = requests.post(f"{AUTH_URL}/register", headers=headers, json={"username": username, "password": password, "role": role})
        flash(response.json())
        return redirect(url_for('register_user'))
    return render_template('register.html')

@app.route('/admin/login', methods=['GET', 'POST'])
def login_user():
    if request.method == 'POST':
        data = request.form
        username = data.get('username')
        password = data.get('password')
        response = requests.post(f"{AUTH_URL}/login", json={"username": username, "password": password})
        flash(response.json())
        if response.status_code == 200:
            token = response.json().get("token")
            flash(f"Login successful, token: {token}")
        return redirect(url_for('login_user'))
    return render_template('login.html')

@app.route('/admin/delete_user', methods=['GET', 'POST'])
def delete_user():
    if request.method == 'POST':
        data = request.form
        token = data.get('token')
        username = data.get('username')
        headers = {'Authorization': token}
        response = requests.delete(f"{AUTH_URL}/delete_user", headers=headers, json={"username": username})
        flash(response.json())
        return redirect(url_for('delete_user'))
    return render_template('delete_user.html')

@app.route('/jobs/submit', methods=['GET', 'POST'])
def submit_job():
    if request.method == 'POST':
        token = request.form['token']
        mapper = request.form['mapper']
        reducer = request.form['reducer']
        input_file = request.files['input_file']
        headers = {'Authorization': token}
        files = {'input_file': input_file}
        data = {'mapper': mapper, 'reducer': reducer}
        response = requests.post(f"{MANAGER_URL}/submit_job", headers=headers, files=files, data=data)
        flash(response.json())
        return redirect(url_for('submit_job'))
    return render_template('submit_job.html')

@app.route('/jobs/view', methods=['GET', 'POST'])
def view_jobs():
    if request.method == 'POST':
        token = request.form['token']
        headers = {'Authorization': token}
        response = requests.get(f"{MANAGER_URL}/jobs", headers=headers)
        flash(response.json())
        return redirect(url_for('view_jobs'))
    return render_template('view_jobs.html')

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5003)
