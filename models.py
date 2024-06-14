# models.py

import hashlib
import uuid

class User:
    def __init__(self, username, password, role):
        self.username = username
        self.password = self.hash_password(password)
        self.role = role

    def hash_password(self, password):
        return hashlib.sha256(password.encode()).hexdigest()

    def verify_password(self, password):
        return self.password == self.hash_password(password)

class Token:
    def __init__(self, username):
        self.token = self.generate_token(username)
        self.username = username

    def generate_token(self, username):
        return hashlib.sha256(f"{username}{uuid.uuid4()}".encode()).hexdigest()