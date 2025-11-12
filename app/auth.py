"""Authentication utilities."""
import os
import json
from pathlib import Path
import hashlib

def get_users_file():
    """Get path to users file."""
    return Path("data/users.json")

def load_users():
    """Load users from file."""
    users_file = get_users_file()
    if not users_file.exists():
        # Create default admin user
        default_users = {
            "admin": {
                "password": hashlib.sha256(os.getenv('ADMIN_PASSWORD', 'admin123').encode()).hexdigest(),
                "role": "admin"
            }
        }
        users_file.parent.mkdir(exist_ok=True)
        with open(users_file, 'w') as f:
            json.dump(default_users, f)
        return default_users
    
    with open(users_file, 'r') as f:
        users = json.load(f)
        # Migrate old format
        for username, data in users.items():
            if isinstance(data, str):
                users[username] = {"password": data, "role": "admin"}
        return users

def save_users(users):
    """Save users to file."""
    users_file = get_users_file()
    users_file.parent.mkdir(exist_ok=True)
    with open(users_file, 'w') as f:
        json.dump(users, f)

def check_password(password: str) -> bool:
    """Check if password matches the admin password (backward compatibility)."""
    return check_user_password('admin', password)

def check_user_password(username: str, password: str) -> bool:
    """Check if username/password combination is valid."""
    users = load_users()
    if username not in users:
        return False
    user_data = users[username]
    password_hash = user_data["password"] if isinstance(user_data, dict) else user_data
    return hashlib.sha256(password.encode()).hexdigest() == password_hash

def create_user(username: str, password: str, role: str = "user") -> bool:
    """Create a new user."""
    users = load_users()
    if username in users:
        return False
    users[username] = {
        "password": hashlib.sha256(password.encode()).hexdigest(),
        "role": role
    }
    save_users(users)
    return True

def delete_user(username: str) -> bool:
    """Delete a user."""
    users = load_users()
    if username not in users:
        return False
    del users[username]
    save_users(users)
    return True

def change_user_password(username: str, old_password: str, new_password: str) -> bool:
    """Change user password."""
    if not check_user_password(username, old_password):
        return False
    users = load_users()
    user_data = users[username]
    if isinstance(user_data, dict):
        user_data["password"] = hashlib.sha256(new_password.encode()).hexdigest()
    else:
        users[username] = {"password": hashlib.sha256(new_password.encode()).hexdigest(), "role": "admin"}
    save_users(users)
    return True

def get_all_users():
    """Get list of all users with roles."""
    users = load_users()
    result = []
    for username, data in users.items():
        if isinstance(data, dict):
            result.append({"username": username, "role": data.get("role", "user")})
        else:
            result.append({"username": username, "role": "admin"})
    return result

def get_user_role(username: str) -> str:
    """Get user role."""
    users = load_users()
    if username not in users:
        return "user"
    user_data = users[username]
    if isinstance(user_data, dict):
        return user_data.get("role", "user")
    return "admin"

def change_user_role(username: str, new_role: str) -> bool:
    """Change user role."""
    users = load_users()
    if username not in users:
        return False
    user_data = users[username]
    if isinstance(user_data, dict):
        user_data["role"] = new_role
    else:
        users[username] = {"password": user_data, "role": new_role}
    save_users(users)
    return True