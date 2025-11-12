"""SQLite database management."""
import sqlite3
import json
from pathlib import Path
from datetime import datetime

class Database:
    """SQLite database manager."""
    
    def __init__(self, db_path):
        self.db_path = db_path
        self.init_db()
    
    def init_db(self):
        """Initialize database tables."""
        with sqlite3.connect(self.db_path) as conn:
            conn.executescript('''
                CREATE TABLE IF NOT EXISTS sessions (
                    id INTEGER PRIMARY KEY,
                    name TEXT UNIQUE NOT NULL,
                    status TEXT DEFAULT 'pending',
                    api_id INTEGER,
                    api_hash TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_used TIMESTAMP
                );
                
                CREATE TABLE IF NOT EXISTS members (
                    id INTEGER PRIMARY KEY,
                    user_id INTEGER UNIQUE,
                    username TEXT,
                    first_name TEXT,
                    last_name TEXT,
                    phone TEXT,
                    access_hash INTEGER,
                    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                
                CREATE TABLE IF NOT EXISTS blacklist (
                    id INTEGER PRIMARY KEY,
                    username TEXT UNIQUE,
                    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                
                CREATE TABLE IF NOT EXISTS phone_numbers (
                    id INTEGER PRIMARY KEY,
                    phone TEXT UNIQUE,
                    status TEXT DEFAULT 'pending',
                    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    processed_at TIMESTAMP
                );
                
                CREATE TABLE IF NOT EXISTS admin_settings (
                    key TEXT PRIMARY KEY,
                    value TEXT,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                
                CREATE TABLE IF NOT EXISTS operations (
                    id TEXT PRIMARY KEY,
                    type TEXT,
                    status TEXT,
                    progress INTEGER DEFAULT 0,
                    data TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                
                CREATE TABLE IF NOT EXISTS admin_session (
                    id INTEGER PRIMARY KEY,
                    session_name TEXT UNIQUE,
                    user_id INTEGER,
                    username TEXT,
                    first_name TEXT,
                    api_id INTEGER,
                    api_hash TEXT,
                    status TEXT DEFAULT 'active',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                
                CREATE TABLE IF NOT EXISTS admin_groups (
                    id INTEGER PRIMARY KEY,
                    group_id INTEGER,
                    title TEXT,
                    username TEXT,
                    participants_count INTEGER DEFAULT 0,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                
                CREATE TABLE IF NOT EXISTS session_limits (
                    id INTEGER PRIMARY KEY,
                    session_name TEXT,
                    date TEXT,
                    users_added INTEGER DEFAULT 0,
                    UNIQUE(session_name, date)
                );
                
                CREATE TABLE IF NOT EXISTS user_preferences (
                    id INTEGER PRIMARY KEY,
                    preference_key TEXT UNIQUE,
                    preference_value TEXT,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            ''')
            
            # Add migration for existing admin_session table
            try:
                conn.execute('ALTER TABLE admin_session ADD COLUMN api_id INTEGER')
                conn.execute('ALTER TABLE admin_session ADD COLUMN api_hash TEXT')
                conn.execute('ALTER TABLE admin_settings ADD COLUMN daily_start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP')
                conn.execute('ALTER TABLE admin_settings ADD COLUMN auto_add_last_run TIMESTAMP DEFAULT CURRENT_TIMESTAMP')
            except sqlite3.OperationalError:
                pass  # Columns already exist

            # Ensure phone_numbers table has processed_at column for older databases
            try:
                conn.execute('ALTER TABLE phone_numbers ADD COLUMN processed_at TIMESTAMP')
            except sqlite3.OperationalError:
                pass  # Column already exists
    
    def get_next_session_name(self):
        """Get next sequential session name."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute('SELECT MAX(CAST(SUBSTR(name, 9) AS INTEGER)) FROM sessions WHERE name LIKE "Session_%"')
            result = cursor.fetchone()[0]
            next_num = (result or 0) + 1
            return f"Session_{next_num:03d}"
    
    def create_session(self, name, api_id, api_hash, status='pending'):
        """Create new session record."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                'INSERT OR REPLACE INTO sessions (name, api_id, api_hash, status) VALUES (?, ?, ?, ?)',
                (name, api_id, api_hash, status)
            )
    
    def update_session_status(self, name, status):
        """Update session status."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                'UPDATE sessions SET status = ?, last_used = CURRENT_TIMESTAMP WHERE name = ?',
                (status, name)
            )
    
    def get_sessions(self, offset=0, limit=None):
        """Get sessions with pagination."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            if limit is not None:
                cursor = conn.execute('SELECT * FROM sessions ORDER BY created_at LIMIT ? OFFSET ?', (limit, offset))
            else:
                cursor = conn.execute('SELECT * FROM sessions ORDER BY created_at')
            return [dict(row) for row in cursor.fetchall()]
    
    def get_sessions_count(self):
        """Get total count of sessions."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute('SELECT COUNT(*) FROM sessions')
            return cursor.fetchone()[0]
    
    def delete_session(self, name):
        """Delete session."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('DELETE FROM sessions WHERE name = ?', (name,))
    
    def delete_all_sessions(self):
        """Delete all sessions."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('DELETE FROM sessions')
    
    def save_members(self, members):
        """Save scraped members."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('DELETE FROM members')  # Clear existing
            for member in members:
                conn.execute('''
                    INSERT OR REPLACE INTO members 
                    (user_id, username, first_name, last_name, phone, access_hash)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (
                    member['id'], member.get('username', ''),
                    member.get('first_name', ''), member.get('last_name', ''),
                    member.get('phone', ''), member.get('access_hash')
                ))
    
    def get_members(self):
        """Get all members."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute('SELECT * FROM members')
            return [dict(row) for row in cursor.fetchall()]
    
    def add_to_blacklist(self, username):
        """Add user to blacklist."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('INSERT OR IGNORE INTO blacklist (username) VALUES (?)', (username,))
    
    def remove_from_blacklist(self, username):
        """Remove user from blacklist."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute('DELETE FROM blacklist WHERE username = ?', (username,))
            return cursor.rowcount > 0
    
    def get_blacklist(self):
        """Get blacklisted users."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute('SELECT username FROM blacklist')
            return [row[0] for row in cursor.fetchall()]
    
    def add_phone_number(self, phone):
        """Add phone number."""
        with sqlite3.connect(self.db_path) as conn:
            try:
                conn.execute('INSERT INTO phone_numbers (phone) VALUES (?)', (phone,))
                return True
            except sqlite3.IntegrityError:
                return False
    
    def remove_phone_number(self, phone):
        """Remove phone number."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute('DELETE FROM phone_numbers WHERE phone = ?', (phone,))
            return cursor.rowcount > 0
    
    def delete_all_phone_numbers(self):
        """Delete all phone numbers."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute('DELETE FROM phone_numbers')
            return cursor.rowcount
    
    def get_phone_numbers(self, offset=0, limit=None):
        """Get phone numbers with pagination."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            if limit is not None:
                cursor = conn.execute('SELECT * FROM phone_numbers ORDER BY added_at LIMIT ? OFFSET ?', (limit, offset))
            else:
                cursor = conn.execute('SELECT * FROM phone_numbers ORDER BY added_at')
            return [dict(row) for row in cursor.fetchall()]
    
    def get_phone_numbers_count(self):
        """Get total count of phone numbers."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute('SELECT COUNT(*) FROM phone_numbers')
            return cursor.fetchone()[0]
    
    def get_pending_phone_numbers(self):
        """Get pending phone numbers."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute('SELECT * FROM phone_numbers WHERE status = "pending" ORDER BY added_at')
            return [dict(row) for row in cursor.fetchall()]
    
    def mark_phone_added(self, phone):
        """Mark phone number as added."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                'UPDATE phone_numbers SET status = "added", processed_at = CURRENT_TIMESTAMP WHERE phone = ?',
                (phone,)
            )
    
    def mark_phone_invited(self, phone):
        """Mark phone number as invited."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                'UPDATE phone_numbers SET status = "invited", processed_at = CURRENT_TIMESTAMP WHERE phone = ?',
                (phone,)
            )
    
    def mark_phone_failed(self, phone):
        """Mark phone number as failed."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                'UPDATE phone_numbers SET status = "failed", processed_at = CURRENT_TIMESTAMP WHERE phone = ?',
                (phone,)
            )
    
    def set_setting(self, key, value):
        """Set admin setting."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                'INSERT OR REPLACE INTO admin_settings (key, value, updated_at) VALUES (?, ?, CURRENT_TIMESTAMP)',
                (key, json.dumps(value))
            )
    
    def get_setting(self, key, default=None):
        """Get admin setting."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute('SELECT value FROM admin_settings WHERE key = ?', (key,))
            result = cursor.fetchone()
            if not result:
                return default
            raw_value = result[0]
            if raw_value is None:
                return default
            try:
                return json.loads(raw_value)
            except (TypeError, json.JSONDecodeError):
                return raw_value
    
    def get_all_settings(self):
        """Get all admin settings."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute('SELECT key, value FROM admin_settings')
            settings = {}
            for row in cursor.fetchall():
                raw_value = row['value']
                if raw_value is None:
                    settings[row['key']] = None
                    continue
                try:
                    settings[row['key']] = json.loads(raw_value)
                except (TypeError, json.JSONDecodeError):
                    settings[row['key']] = raw_value
            return settings
    
    def save_operation(self, operation_id, op_type, status, data=None):
        """Save operation status."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                'INSERT OR REPLACE INTO operations (id, type, status, data) VALUES (?, ?, ?, ?)',
                (operation_id, op_type, status, json.dumps(data) if data else None)
            )
    
    def get_operation(self, operation_id):
        """Get operation status."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute('SELECT * FROM operations WHERE id = ?', (operation_id,))
            result = cursor.fetchone()
            if result:
                data = dict(result)
                data['data'] = json.loads(data['data']) if data['data'] else {}
                return data
            return None
    
    def save_admin_session(self, session_name, user_id, username, first_name, api_id=None, api_hash=None):
        """Save admin session."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                'INSERT OR REPLACE INTO admin_session (session_name, user_id, username, first_name, api_id, api_hash) VALUES (?, ?, ?, ?, ?, ?)',
                (session_name, user_id, username, first_name, api_id, api_hash)
            )
    
    def get_admin_session(self):
        """Get admin session."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute('SELECT * FROM admin_session WHERE status = "active" LIMIT 1')
            result = cursor.fetchone()
            return dict(result) if result else None
    
    def delete_admin_session(self):
        """Delete admin session."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('DELETE FROM admin_session')
    
    def save_admin_groups(self, groups):
        """Save admin groups."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('DELETE FROM admin_groups')  # Clear existing
            for group in groups:
                conn.execute(
                    'INSERT INTO admin_groups (group_id, title, username, participants_count) VALUES (?, ?, ?, ?)',
                    (group['id'], group['title'], group.get('username', ''), group.get('participants_count', 0))
                )
    
    def get_admin_groups(self):
        """Get admin groups."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute('SELECT * FROM admin_groups ORDER BY title')
            return [dict(row) for row in cursor.fetchall()]

    def update_group_member_count(self, group_id, count, title=None, username=None):
        """Update cached member count for a group, creating the record if needed."""
        # Ensure count is not None
        if count is None:
            count = 0
            
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute('SELECT title, username FROM admin_groups WHERE group_id = ?', (group_id,))
            row = cursor.fetchone()
            if row:
                current_title = row['title']
                current_username = row['username']
                conn.execute(
                    '''
                    UPDATE admin_groups
                    SET participants_count = ?,
                        title = ?,
                        username = ?,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE group_id = ?
                    ''',
                    (
                        count,
                        title if title else current_title,
                        username if username else current_username,
                        group_id
                    )
                )
            else:
                resolved_title = title if title else f"Group {group_id}"
                conn.execute(
                    'INSERT INTO admin_groups (group_id, title, username, participants_count) VALUES (?, ?, ?, ?)',
                    (group_id, resolved_title, username or '', count)
                )
            conn.commit()

    def get_member_count(self, group_id=None):
        """Return stored member count for a group. Defaults to configured target group."""
        target_id = group_id if group_id is not None else self.get_setting('target_group_id')
        if not target_id:
            return 0
        try:
            target_id = int(target_id)
        except (TypeError, ValueError):
            return 0
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute('SELECT participants_count FROM admin_groups WHERE group_id = ?', (target_id,)).fetchone()
            count = row[0] if row and row[0] is not None else 0
            
            # Fix any NULL values we encounter
            if row and row[0] is None:
                conn.execute('UPDATE admin_groups SET participants_count = 0 WHERE group_id = ? AND participants_count IS NULL', (target_id,))
                conn.commit()
                count = 0
                
            return count
    
    def set_invite_message(self, message: str):
        """Set custom invite message."""
        self.set_setting('invite_message', message)
    
    def get_invite_message(self):
        """Get custom invite message."""
        default_message = "🎉 You're invited to join our Telegram group! Click the link below to join:\n\n{invite_link}\n\nWelcome aboard!"
        return self.get_setting('invite_message', default_message)
    
    def get_session_daily_limit(self, session_name: str, max_daily: int = 50):
        """Check if session can add more users today."""
        from datetime import date
        today = date.today().isoformat()
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                'SELECT users_added FROM session_limits WHERE session_name = ? AND date = ?',
                (session_name, today)
            )
            result = cursor.fetchone()
            added_today = result[0] if result else 0
            return max_daily - added_today
    
    def increment_session_limit(self, session_name: str):
        """Increment daily user count for session."""
        from datetime import date
        today = date.today().isoformat()
        
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                'INSERT OR IGNORE INTO session_limits (session_name, date, users_added) VALUES (?, ?, 0)',
                (session_name, today)
            )
            conn.execute(
                'UPDATE session_limits SET users_added = users_added + 1 WHERE session_name = ? AND date = ?',
                (session_name, today)
            )
    
    def set_user_preference(self, key, value):
        """Set user preference."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                'INSERT OR REPLACE INTO user_preferences (preference_key, preference_value, updated_at) VALUES (?, ?, CURRENT_TIMESTAMP)',
                (key, json.dumps(value))
            )
    
    def get_user_preference(self, key, default=None):
        """Get user preference."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute('SELECT preference_value FROM user_preferences WHERE preference_key = ?', (key,))
            result = cursor.fetchone()
            if not result:
                return default
            try:
                return json.loads(result[0])
            except (TypeError, json.JSONDecodeError):
                return result[0]
    
    def add_operation(self, operation_type, description, status='completed'):
        """Add a new operation to track user activities."""
        import uuid
        operation_id = str(uuid.uuid4())
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                'INSERT INTO operations (id, type, status, data) VALUES (?, ?, ?, ?)',
                (operation_id, operation_type, status, json.dumps({'description': description}))
            )
        return operation_id
    
    def update_operation_status(self, operation_id, status, data=None):
        """Update operation status and data."""
        with sqlite3.connect(self.db_path) as conn:
            if data:
                conn.execute(
                    'UPDATE operations SET status = ?, data = ? WHERE id = ?',
                    (status, json.dumps(data), operation_id)
                )
            else:
                conn.execute(
                    'UPDATE operations SET status = ? WHERE id = ?',
                    (status, operation_id)
                )
    
    def get_stats(self):
        """Get system statistics."""
        with sqlite3.connect(self.db_path) as conn:
            stats = {}
            stats['total_sessions'] = conn.execute('SELECT COUNT(*) FROM sessions').fetchone()[0]
            stats['total_sessions'] += conn.execute('SELECT COUNT(*) FROM admin_session').fetchone()[0]
            stats['active_sessions'] = conn.execute('SELECT COUNT(*) FROM sessions WHERE status = "active"').fetchone()[0]
            stats['active_sessions'] += conn.execute('SELECT COUNT(*) FROM admin_session WHERE status = "active"').fetchone()[0]
            stats['total_members'] = conn.execute('SELECT COUNT(*) FROM members').fetchone()[0]
            stats['blacklisted_users'] = conn.execute('SELECT COUNT(*) FROM blacklist').fetchone()[0]
            stats['active_operations'] = conn.execute('SELECT COUNT(*) FROM operations WHERE status IN ("running", "pending")').fetchone()[0]
            stats['pending_operations'] = conn.execute('SELECT COUNT(*) FROM operations WHERE status = "pending"').fetchone()[0]
            stats['pending_phones'] = conn.execute('SELECT COUNT(*) FROM phone_numbers WHERE status = "pending"').fetchone()[0]
            stats['added_phones'] = conn.execute('SELECT COUNT(*) FROM phone_numbers WHERE status = "added"').fetchone()[0]
            group_sum_row = conn.execute('SELECT SUM(participants_count) FROM admin_groups').fetchone()
            stats['total_group_members'] = group_sum_row[0] if group_sum_row and group_sum_row[0] is not None else 0
            return stats