"""Session management for Telegram clients."""
import asyncio
import io
import base64
from typing import Dict, Optional
from telethon import TelegramClient
import qrcode
import structlog

logger = structlog.get_logger(__name__)


class SessionManager:
    """Manages Telegram client sessions with web interface support."""
    
    def __init__(self, telegram_config, app_config, db):
        self.telegram_config = telegram_config
        self.app_config = app_config
        self.db = db
        self.sessions: Dict[str, TelegramClient] = {}  # Regular user sessions
        self.admin_sessions: Dict[str, TelegramClient] = {}  # Admin sessions separate
        self.qr_sessions: Dict[str, Dict] = {}
        self.user_sessions: Dict[int, str] = {}  # user_id -> session_name mapping
        self._session_lock = asyncio.Lock()  # For thread safety
        self._generation_semaphore = asyncio.Semaphore(5)  # Limit concurrent QR generations
        self._cleanup_task = None
    
    async def _cleanup_expired_sessions(self):
        """Background task to clean up expired and abandoned sessions."""
        while True:
            try:
                await asyncio.sleep(60)  # Check every minute
                
                from datetime import datetime, timedelta
                now = datetime.now()
                expired_sessions = []
                
                async with self._session_lock:
                    for session_name, data in list(self.qr_sessions.items()):
                        created_at = data.get('created_at')
                        if created_at:
                            created_time = datetime.fromisoformat(created_at)
                            # Clean up sessions older than 10 minutes
                            if (now - created_time).total_seconds() > 600:
                                expired_sessions.append(session_name)
                
                # Clean up expired sessions
                for session_name in expired_sessions:
                    logger.info("Cleaning up expired session", session_name=session_name)
                    async with self._session_lock:
                        if session_name in self.qr_sessions:
                            session_data = self.qr_sessions[session_name]
                            if "client" in session_data:
                                try:
                                    await session_data["client"].disconnect()
                                except Exception:
                                    pass
                            del self.qr_sessions[session_name]
                    
                    # Remove any session files for non-scanned sessions
                    user_path = self.app_config.sessions_dir / "users" / f"{session_name}.session"
                    admin_path = self.app_config.sessions_dir / "admins" / f"{session_name}.session"
                    session_path = user_path if user_path.exists() else admin_path
                    if session_path.exists():
                        # Check both regular sessions and admin session
                        db_sessions = self.db.get_sessions()
                        admin_session = self.db.get_admin_session()
                        is_in_db = any(s['name'] == session_name for s in db_sessions)
                        is_admin = admin_session and admin_session['session_name'] == session_name
                        if not is_in_db and not is_admin:
                            session_path.unlink()
                            logger.info("Removed expired session file", session_name=session_name)
                            
            except Exception as e:
                logger.error("Error in cleanup task", error=str(e))
                await asyncio.sleep(60)
    

    
    def _get_next_session_name(self):
        """Generate unique session name for concurrent users."""
        import time
        import random
        import threading
        
        # Use thread-safe timestamp + random + thread ID for uniqueness
        timestamp = int(time.time() * 1000000) % 1000000
        random_num = random.randint(1000, 9999)
        thread_id = threading.get_ident() % 1000
        return f"Session_{timestamp}_{random_num}_{thread_id}"
    
    def start_cleanup_task(self):
        """Start the cleanup task when event loop is available."""
        if self._cleanup_task is None:
            import asyncio
            self._cleanup_task = asyncio.create_task(self._cleanup_expired_sessions())
            # Also start session health monitoring
            asyncio.create_task(self._periodic_health_check())
    
    async def _periodic_health_check(self):
        """Periodically check session health."""
        while True:
            try:
                await asyncio.sleep(300)  # Check every 5 minutes
                await self._health_check_sessions()
            except Exception as e:
                logger.error("Periodic health check failed", error=str(e))
                await asyncio.sleep(60)  # Wait 1 minute before retry
    
    async def get_active_user_ids(self):
        """Get list of active user IDs to prevent duplicate logins (includes admin)."""
        user_ids = []
        
        # Check regular user sessions
        for session_name, client in self.sessions.items():
            try:
                if client.is_connected() and await client.is_user_authorized():
                    me = await client.get_me()
                    if me:
                        user_ids.append(me.id)
            except Exception as e:
                logger.error("Failed to get user ID", session_name=session_name, error=str(e))
        
        # Check admin session
        for session_name, client in self.admin_sessions.items():
            try:
                if client.is_connected() and await client.is_user_authorized():
                    me = await client.get_me()
                    if me:
                        user_ids.append(me.id)
            except Exception as e:
                logger.error("Failed to get admin user ID", session_name=session_name, error=str(e))
        
        return user_ids
    
    async def create_auto_qr_session(self):
        """Create a new QR session using async - optimized for speed and concurrency."""
        async with self._generation_semaphore:  # Limit concurrent generations
            session_name = self._get_next_session_name()
            
            # Initialize session data immediately
            from datetime import datetime
            async with self._session_lock:
                self.qr_sessions[session_name] = {
                    "status": "generating",
                    "qr_image": None,
                    "created_at": datetime.now().isoformat(),
                    "operation_id": self.db.add_operation("qr_scanning", f"User scanning QR code - {session_name}", "running")
                }
            
            # Generate QR directly for speed
            try:
                result = await self._generate_qr_fast(session_name)
                if result:
                    return {
                        "session_name": session_name,
                        "type": "qr",
                        "qr_image": result,
                        "status": "waiting"
                    }
                elif result is None:  # Already authorized case
                    return {
                        "session_name": session_name,
                        "type": "qr",
                        "status": "success",
                        "message": "Session already authorized"
                    }
            except Exception as e:
                logger.error("Fast QR generation failed", session_name=session_name, error=str(e))
                # Clean up failed session
                async with self._session_lock:
                    self.qr_sessions.pop(session_name, None)
            
            return {"error": "QR generation failed"}
    
    async def verify_2fa_password(self, session_name: str, password: str):
        """Verify 2FA password for QR login."""
        async with self._session_lock:
            if session_name not in self.qr_sessions:
                return {"status": "error", "message": "Session not found"}
            
            session_data = self.qr_sessions[session_name]
            if "client" not in session_data:
                return {"status": "error", "message": "Client not found"}
        
        client = session_data["client"]
        
        try:
            await client.sign_in(password=password)
            logger.info("2FA password verified successfully", session_name=session_name)
            
            # Verify session is actually authorized
            if await client.is_user_authorized():
                await self._save_valid_session(session_name, client)
                async with self._session_lock:
                    if session_name in self.qr_sessions:
                        del self.qr_sessions[session_name]
                return {"status": "success"}
            else:
                await self._cleanup_invalid_session(session_name, client)
                return {"status": "error", "message": "Authorization failed"}
            
        except Exception as e:
            logger.error("2FA password verification failed", session_name=session_name, error=str(e))
            await self._cleanup_invalid_session(session_name, client)
            return {"status": "error", "message": "Invalid password"}
    
    async def verify_admin_2fa_password(self, session_name: str, password: str):
        """Verify 2FA password for admin QR login."""
        async with self._session_lock:
            if session_name not in self.qr_sessions:
                return {"status": "error", "message": "Session not found"}
            
            session_data = self.qr_sessions[session_name]
            if "client" not in session_data:
                return {"status": "error", "message": "Client not found"}
        
        client = session_data["client"]
        
        try:
            await client.sign_in(password=password)
            logger.info("Admin 2FA password verified successfully", session_name=session_name)
            
            # Verify session is actually authorized
            if await client.is_user_authorized():
                await self._save_admin_session(session_name, client)
                async with self._session_lock:
                    if session_name in self.qr_sessions:
                        del self.qr_sessions[session_name]
                return {"status": "success"}
            else:
                await self._cleanup_invalid_session(session_name, client)
                return {"status": "error", "message": "Authorization failed"}
            
        except Exception as e:
            logger.error("Admin 2FA password verification failed", session_name=session_name, error=str(e))
            await self._cleanup_invalid_session(session_name, client)
            return {"status": "error", "message": "Invalid password"}
    
    def check_qr_status(self, session_name: str):
        """Check QR code authentication status."""
        logger.info("Checking QR status", session_name=session_name)
        
        if session_name not in self.qr_sessions:
            # Check if session exists in database (means it was successfully scanned)
            db_sessions = self.db.get_sessions()
            for session in db_sessions:
                if session['name'] == session_name and session['status'] == 'active':
                    logger.info("Session found in database as active", session_name=session_name)
                    return {"status": "success"}
            
            logger.info("Session not found", session_name=session_name)
            return {"status": "not_found"}
        
        session_data = self.qr_sessions[session_name]
        status = session_data["status"]
        logger.info(f"Session status: {status}", session_name=session_name)
        
        if status == "generating":
            return {"status": "waiting"}
        elif status == "scanned":
            # Complete QR scanning operation and clean up
            operation_id = self.qr_sessions[session_name].get("operation_id")
            if operation_id:
                self.db.update_operation_status(operation_id, "completed")
            del self.qr_sessions[session_name]
            return {"status": "success"}
        elif status == "duplicate":
            # Complete QR scanning operation and return duplicate message
            operation_id = session_data.get("operation_id")
            if operation_id:
                self.db.update_operation_status(operation_id, "completed", {"result": "duplicate"})
            message = session_data.get("message", "You already have an active session")
            del self.qr_sessions[session_name]
            return {"status": "duplicate", "message": message}
        elif status == "password_required":
            return {"status": "password_required"}
        elif status in ["expired", "error"]:
            # Complete QR scanning operation as failed and clean up
            operation_id = session_data.get("operation_id")
            if operation_id:
                self.db.update_operation_status(operation_id, "failed", {"reason": status})
            del self.qr_sessions[session_name]
            return {"status": status}
        else:
            return {"status": "waiting"}
    
    def _generate_qr_image(self, url: str) -> str:
        """Generate QR code image as base64 string."""
        qr = qrcode.QRCode(version=1, box_size=10, border=5)
        qr.add_data(url)
        qr.make(fit=True)
        
        img = qr.make_image(fill_color="black", back_color="white")
        
        # Convert to base64
        buffer = io.BytesIO()
        img.save(buffer, format='PNG')
        img_str = base64.b64encode(buffer.getvalue()).decode()
        
        return f"data:image/png;base64,{img_str}"
    

    
    async def _load_all_sessions(self):
        """Load all sessions from database into memory with connection pooling."""
        db_sessions = self.db.get_sessions()
        logger.info("Loading sessions from database", count=len(db_sessions))
        
        # Use semaphore to limit concurrent connections
        semaphore = asyncio.Semaphore(5)
        
        async def load_session(session):
            async with semaphore:
                if session['status'] == 'active' and session['name'] not in self.sessions:
                    session_path = self.app_config.sessions_dir / "users" / f"{session['name']}.session"
                    if session_path.exists():
                        try:
                            client = TelegramClient(
                                str(session_path),
                                session['api_id'],
                                session['api_hash']
                            )
                            await client.connect()
                            if await client.is_user_authorized():
                                async with self._session_lock:
                                    self.sessions[session['name']] = client
                                logger.info("Loaded session", session=session['name'])
                                return True
                            else:
                                await client.disconnect()
                                logger.warning("Session not authorized", session=session['name'])
                                return False
                        except Exception as e:
                            logger.error("Failed to load session", session=session['name'], error=str(e))
                            return False
                    else:
                        logger.warning("Session file missing", session=session['name'], path=str(session_path))
                        return False
                return False
        
        # Load sessions concurrently
        tasks = [load_session(session) for session in db_sessions]
        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            successful = sum(1 for r in results if r is True)
            logger.info(f"Successfully loaded {successful}/{len(tasks)} sessions")

    async def refresh_active_sessions(self):
        """Ensure newly created sessions are available for automation."""
        try:
            await self._load_all_sessions()
            # Run health check after loading
            await self._health_check_sessions()
        except Exception as e:
            logger.error("Failed to refresh active sessions", error=str(e))
        else:
            logger.info("Refresh active sessions completed", active=len(self.sessions))
    
    async def _health_check_sessions(self):
        """Check health of all loaded sessions and remove invalid ones."""
        invalid_sessions = []
        
        for name, client in list(self.sessions.items()):
            try:
                if not client.is_connected():
                    await client.connect()
                
                if not await client.is_user_authorized():
                    invalid_sessions.append(name)
                    continue
                    
                # Test basic functionality
                await client.get_me()
                logger.debug(f"Session {name} health check passed")
                
            except Exception as e:
                logger.warning(f"Session {name} failed health check: {e}")
                invalid_sessions.append(name)
        
        # Remove invalid sessions
        for session_name in invalid_sessions:
            await self._remove_invalid_session(session_name)
        
        if invalid_sessions:
            logger.info(f"Removed {len(invalid_sessions)} invalid sessions during health check")
    
    async def _remove_invalid_session(self, session_name: str):
        """Remove invalid session from memory and database."""
        try:
            # Remove from memory
            async with self._session_lock:
                if session_name in self.sessions:
                    client = self.sessions[session_name]
                    del self.sessions[session_name]
                    try:
                        await client.disconnect()
                    except:
                        pass
            
            # Remove from database
            self.db.delete_session(session_name)
            
            # Remove session file
            session_path = self.app_config.sessions_dir / "users" / f"{session_name}.session"
            if session_path.exists():
                session_path.unlink()
            
            logger.info(f"Removed invalid session: {session_name}")
            
        except Exception as e:
            logger.error(f"Failed to remove invalid session {session_name}: {e}")

    async def refresh_target_group_member_count(self, target_group_id: int):
        """Update cached member counts using any authorized client."""
        if not target_group_id:
            logger.warning("No target group ID provided")
            return None

        try:
            group_id_int = int(target_group_id)
        except (TypeError, ValueError):
            logger.error(f"Invalid group ID: {target_group_id}")
            return None

        checked_clients = []
        
        # Try admin client first
        try:
            admin_client = await self.get_admin_session_client()
            if admin_client:
                checked_clients.append(("admin", admin_client))
                logger.info("Admin client available for member count update")
        except Exception as e:
            logger.warning("Admin session unavailable for member count", error=str(e))

        # Add user sessions as fallback
        for name, client in list(self.sessions.items()):
            checked_clients.append((name, client))

        if not checked_clients:
            logger.error("No clients available for member count update")
            return None

        for source, client in checked_clients:
            try:
                if not client.is_connected():
                    await client.connect()

                group_input = await client.get_input_entity(group_id_int)
                entity = await client.get_entity(group_input)
                
                # Get participant count
                if hasattr(entity, 'participants_count'):
                    total = entity.participants_count
                else:
                    participants = await client.get_participants(entity, limit=0)
                    total = getattr(participants, 'total', len(participants))

                # Update database
                self.db.update_group_member_count(
                    entity.id,
                    total,
                    getattr(entity, 'title', None),
                    getattr(entity, 'username', None)
                )

                logger.info(f"Updated group {entity.id} member count: {total} (source: {source})")
                return {
                    "group_id": entity.id,
                    "count": total,
                    "source": source
                }
            except Exception as e:
                logger.warning(
                    "Failed to refresh group count",
                    group_id=target_group_id,
                    source=source,
                    error=str(e)
                )
                continue

        logger.error(f"All clients failed to update group {target_group_id} member count")
        return None
    
    async def get_session(self, session_name: str) -> Optional[TelegramClient]:
        """Get existing user session with connection validation."""
        logger.info("Getting session", session_name=session_name)
        
        # Check if session exists in memory and is still valid
        if session_name in self.sessions:
            client = self.sessions[session_name]
            try:
                if client.is_connected() and await client.is_user_authorized():
                    logger.info("Session found in memory and valid", session_name=session_name)
                    return client
                else:
                    # Remove invalid session from memory
                    logger.warning("Session in memory but invalid, removing", session_name=session_name)
                    async with self._session_lock:
                        del self.sessions[session_name]
                    try:
                        await client.disconnect()
                    except:
                        pass
            except Exception as e:
                logger.error("Session validation failed", session_name=session_name, error=str(e))
                async with self._session_lock:
                    if session_name in self.sessions:
                        del self.sessions[session_name]
        
        # Try to load existing session from database
        db_sessions = self.db.get_sessions()
        for session in db_sessions:
            if session['name'] == session_name and session['status'] == 'active':
                session_path = self.app_config.sessions_dir / "users" / f"{session_name}.session"
                if session_path.exists():
                    try:
                        client = TelegramClient(
                            str(session_path), 
                            session['api_id'], 
                            session['api_hash']
                        )
                        
                        await client.connect()
                        if await client.is_user_authorized():
                            async with self._session_lock:
                                self.sessions[session_name] = client
                            logger.info("Session loaded from file", session_name=session_name)
                            return client
                        else:
                            await client.disconnect()
                            logger.warning("Session file exists but not authorized", session_name=session_name)
                    except Exception as e:
                        logger.error("Failed to load session from file", session_name=session_name, error=str(e))
                else:
                    logger.warning("Session file missing", session_name=session_name)
        
        return None
    
    async def get_admin_session_client(self) -> Optional[TelegramClient]:
        """Get admin session client."""
        try:
            admin_session = self.db.get_admin_session()
            if not admin_session:
                # Check if user-as-admin is enabled and use first available user session
                use_user_as_admin = self.db.get_setting('use_user_as_admin', False)
                if use_user_as_admin and self.sessions:
                    logger.info("Using user session as admin (user-as-admin enabled)")
                    # Return first connected user session as admin
                    for session_name, client in self.sessions.items():
                        try:
                            if not client.is_connected():
                                await client.connect()
                            if client.is_connected() and await client.is_user_authorized():
                                logger.info(f"Using user session {session_name} as admin")
                                return client
                        except Exception as e:
                            logger.error(f"Failed to check user session {session_name}: {e}")
                logger.warning("No admin session found and user-as-admin not enabled or no user sessions")
                return None
            
            session_name = admin_session['session_name']
            logger.info(f"Loading admin session: {session_name}")
            
            # Check if we already have this admin session in memory
            if session_name in self.admin_sessions:
                client = self.admin_sessions[session_name]
                try:
                    if client.is_connected() and await client.is_user_authorized():
                        logger.info(f"Using cached admin session: {session_name}")
                        return client
                except Exception as e:
                    logger.warning(f"Cached admin session failed: {e}")
                    # Remove failed session from cache
                    del self.admin_sessions[session_name]
            
            # Load admin session from file
            session_path = self.app_config.sessions_dir / "admins" / f"{session_name}.session"
            if not session_path.exists():
                logger.error(f"Admin session file missing: {session_path}")
                return None
            
            api_id = admin_session.get('api_id', self.telegram_config.api_id)
            api_hash = admin_session.get('api_hash', self.telegram_config.api_hash)
            logger.info(f"Loading admin session from file: {session_path}")
            
            client = TelegramClient(session=str(session_path), api_id=api_id, api_hash=api_hash)
            
            try:
                if not client.is_connected():
                    await client.connect()
                
                if await client.is_user_authorized():
                    self.admin_sessions[session_name] = client
                    logger.info(f"Admin session loaded successfully: {session_name}")
                    return client
                else:
                    logger.error(f"Admin session not authorized: {session_name}")
                    await client.disconnect()
                    return None
                    
            except Exception as e:
                logger.error(f"Failed to load admin session {session_name}: {e}")
                try:
                    await client.disconnect()
                except Exception:
                    pass
                return None
                
        except Exception as e:
            logger.error(f"Error in get_admin_session_client: {e}")
            return None
    
    def _get_user_api_credentials(self, user_id):
        """Get user's API credentials from database or use defaults."""
        # Try to get from existing user session first
        db_sessions = self.db.get_sessions()
        for session in db_sessions:
            if session.get('user_id') == user_id:
                return session['api_id'], session['api_hash']
        
        # Use default credentials
        return self.telegram_config.api_id, self.telegram_config.api_hash
    
    def list_sessions(self, offset=0, limit=None):
        """List sessions with pagination support."""
        logger.info("Listing sessions with pagination", offset=offset, limit=limit)
        db_sessions = self.db.get_sessions(offset=offset, limit=limit)
        sessions = []
        
        # Only include database sessions (valid, scanned sessions)
        for session in db_sessions:
            sessions.append({
                "name": session['name'],
                "status": session['status'],
                "created_at": session['created_at']
            })
        
        # Add admin session if exists (only on first page)
        if offset == 0:
            admin_session = self.db.get_admin_session()
            if admin_session:
                sessions.append({
                    "name": admin_session['session_name'],
                    "status": "active",
                    "type": "admin",
                    "username": admin_session['username'],
                    "created_at": admin_session['created_at']
                })
            
            # Add only pending QR sessions that are still being processed (only on first page)
            for name, data in self.qr_sessions.items():
                status = data.get("status", "generating")
                if status in ["generating", "waiting", "password_required"]:
                    session_type = "admin_qr" if data.get("is_admin") else "qr"
                    sessions.append({"name": name, "status": status, "type": session_type})
        
        logger.info(f"Returning {len(sessions)} sessions for page")
        return sessions
    
    def get_sessions_count(self):
        """Get total count of sessions including admin and QR sessions."""
        count = self.db.get_sessions_count()
        
        # Add admin session if exists
        admin_session = self.db.get_admin_session()
        if admin_session:
            count += 1
        
        # Add pending QR sessions
        pending_qr = sum(1 for data in self.qr_sessions.values() 
                        if data.get("status") in ["generating", "waiting", "password_required"])
        count += pending_qr
        
        return count
    
    async def remove_session(self, session_name: str):
        """Remove a session - logs out, disconnects, deletes file, and removes from database."""
        logger.info("Removing session", session_name=session_name)
        success = True
        user_id_to_remove = None
        
        try:
            # 1. Get user ID before logout and disconnect from Telegram if active
            client = None
            if session_name in self.sessions:
                logger.info("Logging out and disconnecting user session", session_name=session_name)
                client = self.sessions[session_name]
                del self.sessions[session_name]
            elif session_name in self.admin_sessions:
                logger.info("Logging out and disconnecting admin session", session_name=session_name)
                client = self.admin_sessions[session_name]
                del self.admin_sessions[session_name]
            
            if client:
                try:
                    # Get user ID before logout
                    me = await client.get_me()
                    if me:
                        user_id_to_remove = me.id
                    
                    await client.log_out()
                    logger.info("Session logged out", session_name=session_name)
                except Exception as logout_error:
                    logger.warning("Logout failed, disconnecting", session_name=session_name, error=str(logout_error))
                    await client.disconnect()
                logger.info("Session removed", session_name=session_name)
            
            # 2. Remove from user sessions mapping
            if user_id_to_remove:
                async with self._session_lock:
                    if user_id_to_remove in self.user_sessions:
                        del self.user_sessions[user_id_to_remove]
                        logger.info("User session mapping removed", user_id=user_id_to_remove)
            
            # 3. Remove from QR sessions and disconnect if needed
            if session_name in self.qr_sessions:
                logger.info("Removing QR session", session_name=session_name)
                if "client" in self.qr_sessions[session_name]:
                    client = self.qr_sessions[session_name]["client"]
                    try:
                        await client.log_out()
                    except Exception:
                        await client.disconnect()
                del self.qr_sessions[session_name]
                logger.info("QR session removed", session_name=session_name)
            
        except Exception as e:
            logger.error("Failed to logout/disconnect session", session_name=session_name, error=str(e))
            success = False
        
        try:
            # 4. Remove session file from both possible locations
            user_path = self.app_config.sessions_dir / "users" / f"{session_name}.session"
            admin_path = self.app_config.sessions_dir / "admins" / f"{session_name}.session"
            
            for session_path in [user_path, admin_path]:
                if session_path.exists():
                    logger.info("Removing session file", session_name=session_name)
                    session_path.unlink()
                    logger.info("Session file removed", session_name=session_name)
                
        except Exception as e:
            logger.error("Failed to remove session file", session_name=session_name, error=str(e))
            success = False
        
        try:
            # 5. Remove from database
            logger.info("Removing session from database", session_name=session_name)
            self.db.delete_session(session_name)
            logger.info("Session removed from database", session_name=session_name)
            
        except Exception as e:
            logger.error("Failed to remove session from database", session_name=session_name, error=str(e))
            success = False
        
        return success
    
    async def create_admin_qr_session(self):
        """Create admin QR session."""
        # Check if admin session already exists
        existing_admin = self.db.get_admin_session()
        if existing_admin:
            # Check if there's already a QR session in progress
            for name, data in self.qr_sessions.items():
                if data.get("is_admin") and data.get("status") in ["generating", "waiting", "password_required"]:
                    return {"error": "Admin QR session already in progress. Please wait or refresh."}
            return {"error": "Admin session already exists. Please remove it first."}
        
        async with self._generation_semaphore:
            session_name = f"Admin_{self._get_next_session_name()}"
            
            from datetime import datetime
            async with self._session_lock:
                self.qr_sessions[session_name] = {
                    "status": "generating",
                    "qr_image": None,
                    "created_at": datetime.now().isoformat(),
                    "is_admin": True,
                    "operation_id": self.db.add_operation("admin_qr_scanning", f"Admin scanning QR code - {session_name}", "running")
                }
            
            try:
                result = await self._generate_admin_qr_fast(session_name)
                if result:
                    return {
                        "session_name": session_name,
                        "type": "admin_qr",
                        "qr_image": result,
                        "status": "waiting"
                    }
            except Exception as e:
                logger.error("Admin QR generation failed", session_name=session_name, error=str(e))
                async with self._session_lock:
                    self.qr_sessions.pop(session_name, None)
            
            return {"error": "Admin QR generation failed"}
    
    async def _generate_admin_qr_fast(self, session_name):
        """Generate admin QR code."""
        import io
        import base64
        import asyncio
        from telethon import TelegramClient
        import qrcode
        
        client = None
        try:
            from telethon.sessions import MemorySession
            client = TelegramClient(
                MemorySession(),
                self.telegram_config.api_id,
                self.telegram_config.api_hash
            )
            
            await client.connect()
            
            if not await client.is_user_authorized():
                qr_login = await client.qr_login()
                
                qr = qrcode.QRCode(version=1, box_size=8, border=3)
                qr.add_data(qr_login.url)
                qr.make(fit=True)
                
                img = qr.make_image(fill_color="black", back_color="white")
                buffer = io.BytesIO()
                img.save(buffer, format='PNG')
                img_str = base64.b64encode(buffer.getvalue()).decode()
                qr_image = f"data:image/png;base64,{img_str}"
                
                async with self._session_lock:
                    self.qr_sessions[session_name]["qr_image"] = qr_image
                    self.qr_sessions[session_name]["status"] = "waiting"
                
                asyncio.create_task(self._wait_for_admin_scan(session_name, client, qr_login))
                return qr_image
            else:
                await self._save_admin_session(session_name, client)
                return None
                
        except Exception as e:
            logger.error("Admin QR generation failed", session_name=session_name, error=str(e))
            async with self._session_lock:
                if session_name in self.qr_sessions:
                    self.qr_sessions[session_name]["status"] = "error"
            if client:
                await client.disconnect()
            return None
    
    async def _wait_for_admin_scan(self, session_name, client, qr_login):
        """Wait for admin QR scan."""
        try:
            import asyncio
            await asyncio.wait_for(qr_login.wait(), timeout=300)
            
            if await client.is_user_authorized():
                await self._save_admin_session(session_name, client)
            else:
                await self._cleanup_invalid_session(session_name, client)
                
        except asyncio.TimeoutError:
            logger.info("Admin QR code expired", session_name=session_name)
            await self._cleanup_invalid_session(session_name, client, "expired")
        except Exception as e:
            if "Two-steps verification" in str(e) or "password is required" in str(e):
                logger.info("Admin 2FA required", session_name=session_name)
                async with self._session_lock:
                    if session_name in self.qr_sessions:
                        self.qr_sessions[session_name]["status"] = "password_required"
                        self.qr_sessions[session_name]["client"] = client
            else:
                logger.error("Admin QR scan failed", session_name=session_name, error=str(e))
                await self._cleanup_invalid_session(session_name, client, "error")
    
    async def _persist_authorized_session(self, session_name, client, me, *, is_admin: bool):
        """Persist an authorized session to disk and in-memory registries."""
        user_api_id, user_api_hash = self._get_user_api_credentials(me.id)
        target_dir = self.app_config.sessions_dir / ("admins" if is_admin else "users")
        target_dir.mkdir(exist_ok=True)
        session_path = target_dir / f"{session_name}.session"

        file_client = TelegramClient(str(session_path), user_api_id, user_api_hash)
        file_client.session.set_dc(
            client.session.dc_id,
            client.session.server_address,
            client.session.port
        )
        file_client.session.auth_key = client.session.auth_key
        file_client.session.save()
        print("Persisted session to file:", session_path)
        if not file_client.session.auth_key:
            raise Exception("Failed to copy session data properly")

        try:
            if is_admin:
                self.db.save_admin_session(
                    session_name,
                    me.id,
                    me.username or '',
                    me.first_name or '',
                    user_api_id,
                    user_api_hash
                )

                async with self._session_lock:
                    self.admin_sessions[session_name] = file_client
                    if session_name in self.qr_sessions:
                        self.qr_sessions[session_name]["status"] = "scanned"
                
                # Add operation tracking for admin session creation
                self.db.add_operation("admin_session_created", f"Admin session {session_name} created for user {me.id}")
            else:
                self.db.create_session(session_name, user_api_id, user_api_hash, 'active')
                async with self._session_lock:
                    self.sessions[session_name] = file_client
                    self.user_sessions[me.id] = session_name
                    if session_name in self.qr_sessions:
                        self.qr_sessions[session_name]["status"] = "scanned"
                
                # Add operation tracking for user session creation
                self.db.add_operation("user_session_created", f"User session {session_name} created for user {me.id}")

            return file_client
        finally:
            try:
                await client.disconnect()
            except Exception:
                pass

    async def _save_admin_session(self, session_name, client):
        """Persist admin session using the shared session flow."""
        try:
            me = await client.get_me()
            if not me:
                raise Exception("Could not get user info")

            file_client = await self._persist_authorized_session(session_name, client, me, is_admin=True)
            # try disconnecting the temp client
            try:
                await file_client.disconnect()
            except Exception:
                pass
            logger.info("Admin session saved", session_name=session_name, user_id=me.id)

        except Exception as e:
            logger.error("Failed to save admin session", session_name=session_name, error=str(e))
            await self._cleanup_invalid_session(session_name, client, "error")
    
    async def _fetch_admin_groups(self, client):
        """Fetch admin groups and save to database."""
        try:
            from telethon.tl.types import Chat, Channel
            
            groups = []
            async for dialog in client.iter_dialogs():
                entity = dialog.entity
                is_channel = isinstance(entity, Channel)
                if is_channel and getattr(entity, 'broadcast', False):
                    continue

                if not (isinstance(entity, Chat) or is_channel):
                    continue
                print("Found group:", entity.participants_count)
                title = getattr(entity, 'title', None) or getattr(entity, 'first_name', None)
                if not title:
                    logger.info(f"Skipping group {entity.id} due to missing title")
                    continue

                groups.append({
                    'id': entity.id,
                    'title': title,
                    'username': getattr(entity, 'username', '') or '',
                    'participants_count': getattr(entity, 'participants_count', 0)
                })
            
            self.db.save_admin_groups(groups)
            logger.info(f"Saved {len(groups)} admin groups")
            
        except Exception as e:
            logger.error("Failed to fetch admin groups", error=str(e))
    
    def get_admin_session(self):
        """Get admin session info."""
        return self.db.get_admin_session()
    
    def get_admin_groups(self):
        """Get admin groups."""
        return self.db.get_admin_groups()
    
    async def remove_admin_session(self):
        """Remove admin session."""
        admin_session = self.db.get_admin_session()
        if admin_session:
            session_name = admin_session['session_name']
            
            # Disconnect admin client first
            if session_name in self.admin_sessions:
                try:
                    await self.admin_sessions[session_name].disconnect()
                    del self.admin_sessions[session_name]
                    logger.info("Admin client disconnected", session_name=session_name)
                except Exception as e:
                    logger.error("Failed to disconnect admin client", session_name=session_name, error=str(e))
            
            # Remove session (includes file deletion)
            await self.remove_session(session_name)
            # Remove from database
            self.db.delete_admin_session()
            return True
        return False
    
    async def remove_all_sessions(self):
        """Remove all user sessions with proper logout (excludes admin session)."""
        try:
            # Get all user sessions (not admin sessions)
            async with self._session_lock:
                all_sessions = list(self.sessions.keys()) + list(self.qr_sessions.keys())
            
            # Remove each user session
            for session_name in all_sessions:
                if not session_name.startswith('Admin_'):
                    await self.remove_session(session_name)
            
            # Clear user session files only
            users_dir = self.app_config.sessions_dir / "users"
            if users_dir.exists():
                for session_file in users_dir.glob("*.session"):
                    try:
                        session_file.unlink()
                        logger.info("Removed orphaned session file", file=str(session_file))
                    except Exception as e:
                        logger.error("Failed to remove session file", file=str(session_file), error=str(e))
            
            # Clear database (only user sessions)
            self.db.delete_all_sessions()
            
            return True
            
        except Exception as e:
            logger.error("Failed to remove all sessions", error=str(e))
            return False
    
    async def get_next_available_session(self):
        """Get next available session for adding members with validation."""
        available_sessions = []
        
        # Check all sessions for availability
        for name, client in list(self.sessions.items()):
            try:
                if client.is_connected() and await client.is_user_authorized():
                    # Check daily limit
                    remaining = self.db.get_session_daily_limit(name)
                    if remaining > 0:
                        available_sessions.append((name, remaining))
            except Exception as e:
                logger.warning(f"Session {name} validation failed: {e}")
                # Remove invalid session
                async with self._session_lock:
                    if name in self.sessions:
                        del self.sessions[name]
        
        if not available_sessions:
            return None
        
        # Sort by remaining quota (highest first) then by name
        available_sessions.sort(key=lambda x: (-x[1], x[0]))
        return available_sessions[0][0]
    
    def add_phone_number(self, phone_number: str):
        """Add phone number to the list."""
        return self.db.add_phone_number(phone_number)
    
    def remove_phone_number(self, phone_number: str):
        """Remove phone number from the list."""
        return self.db.remove_phone_number(phone_number)
    
    def get_phone_numbers(self, offset=0, limit=None):
        """Get phone numbers with pagination."""
        return self.db.get_phone_numbers(offset=offset, limit=limit)
    
    def get_phone_numbers_count(self):
        """Get total count of phone numbers."""
        return self.db.get_phone_numbers_count()
    
    def import_phone_numbers(self, phone_numbers: list):
        """Import multiple phone numbers."""
        added = 0
        for phone in phone_numbers:
            if self.db.add_phone_number(phone.strip()):
                added += 1
        return added
    
    async def list_all_groups(self):
        """List all groups using admin session."""
        logger.info("Starting to list all groups")
        admin_client = await self.get_admin_session_client()
        if not admin_client:
            logger.warning("No admin client available")
            return []
        
        logger.info("Admin client obtained, fetching dialogs")
        groups = []
        try:
            from telethon.tl.types import Chat, Channel

            dialog_count = 0
            async for dialog in admin_client.iter_dialogs():
                dialog_count += 1
                entity = dialog.entity
                entity_type = type(entity).__name__
                try:
                    logger.debug(
                        "Processing dialog",
                        dialog_index=dialog_count,
                        entity_type=entity_type,
                        entity_id=getattr(entity, 'id', None)
                    )
                except UnicodeEncodeError:
                    # Skip logging for entities with problematic Unicode characters
                    pass

                is_channel = isinstance(entity, Channel)
                if is_channel and getattr(entity, 'broadcast', False):
                    try:
                        logger.debug("Skipping broadcast channel", entity_id=entity.id)
                    except UnicodeEncodeError:
                        
                        continue

                if not (isinstance(entity, Chat) or is_channel):
                    try:
                        logger.debug("Skipping non-group dialog", entity_type=entity_type)
                    except UnicodeEncodeError:
                        continue
                
                #ignore if its a normal user not a group
                if isinstance(dialog.entity, (Channel, Chat)) and dialog.is_group:
                    title = getattr(entity, 'title', None)
                    if not title and getattr(entity, 'participants_count', 0)==0:
                        try:
                            logger.debug("Skipping dialog without title", entity_id=entity.id)
                        except UnicodeEncodeError:
                            pass
                            continue
                    
                    group_info = {
                        "id": entity.id,
                        "title": title,
                        "username": getattr(entity, 'username', '') or '',
                        "participants_count": getattr(entity, 'participants_count', 0)
                    }
                    groups.append(group_info)
                    try:
                        logger.debug("Added group", group_id=group_info['id'], title=group_info['title'])
                    except UnicodeEncodeError:
                        # Skip logging for groups with problematic Unicode characters in title
                        pass
            # remove groups with 0 participant or null title
            for group in groups:
                if not group["title"] or group["participants_count"] == 0 or group["title"] == "null" or group["title"] == None:
                    groups.remove(group)
            logger.info(f"Found {len(groups)} groups out of {dialog_count} total dialogs")
            return groups
        except Exception as e:
            if "key is not registered" in str(e):
                logger.warning("Admin session expired during group listing")
                admin_session = self.db.get_admin_session()
                if admin_session:
                    session_name = admin_session['session_name']
                    if session_name in self.admin_sessions:
                        del self.admin_sessions[session_name]
            logger.error("Failed to list groups", error=str(e))
            return []

    async def get_group_by_id(self, group_id: int):
        """Get group information using admin session."""
        admin_client = await self.get_admin_session_client()
        if not admin_client:
            return None
        
        try:
            group = await admin_client.get_entity(group_id)
            return {
                "id": group.id,
                "title": group.title,
                "username": getattr(group, 'username', ''),
                "participants_count": getattr(group, 'participants_count', 0)
            }
        except Exception as e:
            logger.error(f"Failed to get group {group_id}: {e}")
            return None
    
    async def _generate_qr_fast(self, session_name):
        """Fast QR generation with duplicate user prevention."""
        import io
        import base64
        import asyncio
        from telethon import TelegramClient
        import qrcode
        
        client = None
        try:
            # Get active user IDs to prevent duplicates
            ignored_ids = await self.get_active_user_ids()
            
            # Use memory session initially - no file created yet
            from telethon.sessions import MemorySession
            client = TelegramClient(
                MemorySession(),  # Pure memory session
                self.telegram_config.api_id,
                self.telegram_config.api_hash
            )
            
            await client.connect()
            
            if not await client.is_user_authorized():
                # Use ignored_ids to prevent duplicate logins
                qr_login = await client.qr_login(ignored_ids=ignored_ids)
                
                # Generate QR image fast
                qr = qrcode.QRCode(version=1, box_size=8, border=3)
                qr.add_data(qr_login.url)
                qr.make(fit=True)
                
                img = qr.make_image(fill_color="black", back_color="white")
                buffer = io.BytesIO()
                img.save(buffer, format='PNG')
                img_str = base64.b64encode(buffer.getvalue()).decode()
                qr_image = f"data:image/png;base64,{img_str}"
                
                # Update session with QR and start background scan waiter
                async with self._session_lock:
                    self.qr_sessions[session_name]["qr_image"] = qr_image
                    self.qr_sessions[session_name]["status"] = "waiting"
                
                # Start background task to wait for scan
                asyncio.create_task(self._wait_for_scan(session_name, client, qr_login))
                
                return qr_image
            else:
                # Already authorized - save immediately
                await self._save_valid_session(session_name, client)
                return None
                
        except Exception as e:
            logger.error("Fast QR generation failed", session_name=session_name, error=str(e))
            async with self._session_lock:
                if session_name in self.qr_sessions:
                    self.qr_sessions[session_name]["status"] = "error"
            if client:
                await client.disconnect()
            return None
    
    async def _wait_for_scan(self, session_name, client, qr_login):
        """Background task to wait for QR scan."""
        try:
            import asyncio
            await asyncio.wait_for(qr_login.wait(), timeout=300)
            
            # Only save if scan was successful and session is valid
            if await client.is_user_authorized():
                await self._save_valid_session(session_name, client)
                
            else:
                await self._cleanup_invalid_session(session_name, client)
                
        except asyncio.TimeoutError:
            logger.info("QR code expired", session_name=session_name)
            await self._cleanup_invalid_session(session_name, client, "expired")
        except Exception as e:
            if "Two-steps verification" in str(e) or "password is required" in str(e):
                logger.info("2FA required", session_name=session_name)
                async with self._session_lock:
                    if session_name in self.qr_sessions:
                        self.qr_sessions[session_name]["status"] = "password_required"
                        self.qr_sessions[session_name]["client"] = client
            else:
                logger.error("QR scan failed", session_name=session_name, error=str(e))
                await self._cleanup_invalid_session(session_name, client, "error")
    
    async def _save_valid_session(self, session_name, client):
        """Save only valid, authorized sessions to file and database."""
        try:
            me = await client.get_me()
            if not me:
                raise Exception("Could not get user info")

            duplicate_message = "You have already scanned the QR code and created a valid session. Thank you for your support! 🎉"
            async with self._session_lock:
                existing_session = self.user_sessions.get(me.id)
                if existing_session and session_name in self.qr_sessions:
                    self.qr_sessions[session_name]["status"] = "duplicate"
                    self.qr_sessions[session_name]["message"] = duplicate_message

            if existing_session:
                print("User already has session", me.id, existing_session)
                logger.info("User already has session", user_id=me.id, existing_session=existing_session)
                await client.disconnect()
                return

            file_client = await self._persist_authorized_session(session_name, client, me, is_admin=False)
            # try disconnecting the temp client
            try:
                await file_client.disconnect()
            except Exception:
                pass
            
            # Add operation tracking for user session creation
            self.db.add_operation("user_session_created", f"User session {session_name} created for user {me.id}")
            
            logger.info("Valid session saved", session_name=session_name, user_id=me.id)
            print("Valid session saved", session_name, me.id)

        except Exception as e:
            logger.error("Failed to save valid session", session_name, error=str(e))
            print("Failed to save valid session", session_name, str(e))
            logger.error("Failed to save valid session", session_name=session_name, error=str(e))
            await self._cleanup_invalid_session(session_name, client, "error")
    
    async def _cleanup_invalid_session(self, session_name, client, status="error"):
        """Clean up invalid sessions immediately."""
        try:
            # Disconnect client
            await client.disconnect()
            
            # Remove any session file that might have been created
            user_path = self.app_config.sessions_dir / "users" / f"{session_name}.session"
            admin_path = self.app_config.sessions_dir / "admins" / f"{session_name}.session"
            
            for session_path in [user_path, admin_path]:
                if session_path.exists():
                    try:
                        session_path.unlink()
                        logger.info("Removed invalid session file", session_name=session_name)
                    except Exception as file_error:
                        logger.warning("Could not remove session file", session_name=session_name, error=str(file_error))
            
            # Complete QR scanning operation as failed
            async with self._session_lock:
                if session_name in self.qr_sessions:
                    operation_id = self.qr_sessions[session_name].get("operation_id")
                    if operation_id:
                        self.db.update_operation_status(operation_id, "failed", {"reason": status})
                    self.qr_sessions[session_name]["status"] = status
            
            logger.info("Invalid session cleaned up", session_name=session_name, status=status)
            
        except Exception as e:
            logger.error("Failed to cleanup invalid session", session_name=session_name, error=str(e))
    
    async def add_users_to_group(self, group_id: int, delay: int = 30, batch_size: int = 5, max_daily_per_session: int = 80, invite_message: str = None):
        """Add pending phone numbers to group with contact management and invite links fallback."""
        import asyncio
        
        # Create operation tracking
        operation_id = self.db.add_operation("user_adding", f"Adding users to group {group_id}", status="running")
        
        # Update invite message if provided
        if invite_message:
            self.db.set_invite_message(invite_message)
        
        # Get pending phone numbers
        pending_phones = self.db.get_pending_phone_numbers()
        if not pending_phones:
            # Mark operation as completed with no work
            self.db.update_operation_status(operation_id, "completed", {"message": "No pending phone numbers"})
            return {"success": False, "message": "No pending phone numbers"}
        
        # Load all sessions from database first
        await self._load_all_sessions()
        
        # Get available user sessions with remaining daily limits
        available_sessions = []
        print("Checking available sessions for adding users...", len(self.sessions))
        for name, client in self.sessions.items():
            try:
                if not client.is_connected():
                    await client.connect()
                if client.is_connected() and await client.is_user_authorized():
                    remaining = self.db.get_session_daily_limit(name, max_daily_per_session)
                    print(f"Session {name} has {remaining} remaining slots out of {max_daily_per_session}")
                    if remaining > 0:
                        available_sessions.append((name, remaining))
                else:
                    print(f"Session {name} is not authorized")
            except Exception as e:
                logger.error(f"Failed to check session {name}: {e}")
        
        # Check if admin-as-user toggle is enabled
        use_admin_as_user = self.db.get_setting('use_admin_as_user', False)
        if use_admin_as_user:
            admin_client = await self.get_admin_session_client()
            if admin_client:
                admin_session = self.db.get_admin_session()
                if admin_session:
                    admin_name = admin_session['session_name']
                    remaining = self.db.get_session_daily_limit(admin_name, max_daily_per_session)
                    if remaining > 0:
                        available_sessions.append((admin_name, remaining))
                        # Add admin client to sessions temporarily
                        self.sessions[admin_name] = admin_client
        
        if not available_sessions:
            total_sessions = len(self.sessions)
            error_msg = "No user sessions available. Users need to scan QR codes first." if total_sessions == 0 else f"All {total_sessions} sessions have reached their daily limit of {max_daily_per_session} users."
            # Mark operation as failed
            self.db.update_operation_status(operation_id, "failed", {"error": error_msg})
            return {"success": False, "message": error_msg}
        
        # Get admin session for group access
        admin_client = await self.get_admin_session_client()
        if not admin_client:
            admin_session = self.db.get_admin_session()
            use_user_as_admin = self.db.get_setting('use_user_as_admin', False)
            
            if not admin_session and not use_user_as_admin:
                error_msg = "Admin session required. Please create an admin session first."
            elif admin_session and not use_user_as_admin:
                error_msg = f"Admin session exists but failed to connect. Session: {admin_session['session_name']}"
            elif use_user_as_admin and not self.sessions:
                error_msg = "User-as-admin enabled but no user sessions available."
            else:
                error_msg = "Admin session connection failed. Please check session status."
            
            # Mark operation as failed
            self.db.update_operation_status(operation_id, "failed", {"error": error_msg})
            return {"success": False, "message": error_msg}
        
        results = {"added": 0, "failed": 0, "invited": 0, "total": len(pending_phones), "errors": [], "skipped": 0}
        session_index = 0
        
        logger.info(
            "Starting auto-add run",
            pending=len(pending_phones),
            available_sessions=len(available_sessions),
            delay=delay,
            batch_size=batch_size,
            max_daily=max_daily_per_session,
            use_admin_as_user=self.db.get_setting('use_admin_as_user', False)
        )

        if not pending_phones:
            logger.warning("Auto-add run aborted: no pending phone numbers queued")
            return {"success": False, "message": "No pending phone numbers"}

        if not available_sessions:
            logger.warning(
                "Auto-add run aborted: no sessions available with remaining quota",
                total_sessions=len(self.sessions)
            )
            return {"success": False, "message": "No sessions available"}

        logger.info("Sessions with remaining quota", sessions=[name for name, _ in available_sessions])

        try:
            # Get group and ensure sessions are in group
            group_input = await admin_client.get_input_entity(group_id)
            group = await admin_client.get_entity(group_input)
            print(group_input.to_json())
            logger.info(
                "Resolved target group",
                group_id=group.id,
                title=getattr(group, 'title', ''),
                username=getattr(group, 'username', None)
            )
            mega_group = getattr(group,"megagroup", False)
            username=getattr(group, 'username', None)
            group_id = group.id
            # Add user sessions to group if not already members
            await self._ensure_sessions_in_group(admin_client, group, group_input, available_sessions)
            
            # Create invite link for fallback
            invite_link = await self._create_invite_link(admin_client, group, group_input)
            
            # Process phones in batches
            for i in range(0, len(pending_phones), batch_size):
                batch = pending_phones[i:i + batch_size]
                
                for phone_data in batch:
                    phone = phone_data.get('phone') or phone_data.get('phone_number')
                    if not phone:
                        logger.warning("Pending phone entry missing number", entry=phone_data)
                        results["failed"] += 1
                        continue
                    
                    # Find session with remaining limit
                    session_found = False
                    for j in range(len(available_sessions)):
                        session_idx = (session_index + j) % len(available_sessions)
                        session_name, remaining = available_sessions[session_idx]
                        
                        if remaining > 0:
                            client = self.sessions[session_name]
                            session_index = session_idx + 1
                            session_found = True
                            break
                    
                    if not session_found:
                        logger.warning("All sessions reached daily limit")
                        results["skipped"] = len(pending_phones) - (results["added"] + results["failed"] + results["invited"])
                        break
                    
                    logger.info(
                        "Processing pending phone",
                        phone=phone,
                        session=session_name,
                        session_remaining=remaining,
                        batch_index=i,
                        batch_offset=i + batch.index(phone_data) if phone_data in batch else i
                    )

                    phone_set = await self.get_active_contact_lists(client)
                    success = await self._process_phone_number(client, admin_client, phone, group, group_input, session_name, invite_link, phone_set)
                    
                    if success == "added":
                        results["added"] += 1
                        self.db.increment_session_limit(session_name)
                        self.db.mark_phone_added(phone)
                        available_sessions[session_idx] = (session_name, remaining - 1)
                    elif success == "already_member":
                        results["added"] += 1
                        # Don't increment session limit for existing members
                        logger.info(
                            "Phone added successfully",
                            phone=phone,
                            session=session_name,
                            added_total=results['added'],
                            invited_total=results['invited'],
                            failed_total=results['failed']
                        )
                        self.db.mark_phone_added(phone)
                    elif success == "invited":
                        results["invited"] += 1
                        logger.info(
                            "Invite link delivered",
                            phone=phone,
                            session=session_name,
                            invited_total=results['invited']
                        )
                        self.db.mark_phone_invited(phone)
                    else:
                        results["failed"] += 1
                        results["errors"].append(f"{phone}: {success}")
                        logger.warning(
                            "Phone failed to add",
                            phone=phone,
                            session=session_name,
                            reason=success
                        )
                    
                    # Clean up admin session from regular sessions if it was added temporarily
                    admin_session = self.db.get_admin_session()
                    if admin_session and session_name == admin_session['session_name'] and session_name in self.sessions:
                        # Don't remove admin client, just clean up the temporary reference
                        pass
                    
                    # Delay between additions
                    await asyncio.sleep(delay)
                
                # Longer delay between batches
                if i + batch_size < len(pending_phones):
                    logger.info(
                        "Applying inter-batch delay",
                        seconds=delay * 2,
                        processed=results['added'] + results['failed'] + results['invited']
                    )
                    await asyncio.sleep(delay * 2)
                    
        except Exception as e:
            logger.error(f"Group operation failed: {e}")
            # Mark operation as failed
            self.db.update_operation_status(operation_id, "failed", {"error": str(e)})
            return {"success": False, "message": f"Group access failed: {str(e)}"}
        
        logger.info(
            "Auto-add run finished",
            total=results['total'],
            added=results['added'],
            invited=results['invited'],
            failed=results['failed'],
            skipped=results['skipped']
        )
        
        # Mark operation as completed
        self.db.update_operation_status(operation_id, "completed", results)

        return {"success": True, "results": results}
    
    async def _ensure_sessions_in_group(self, admin_client: TelegramClient, group, group_input, available_sessions):
        """Add user sessions to group if not already members using admin privileges."""
        try:
            from telethon.tl.functions.channels import InviteToChannelRequest, JoinChannelRequest
            from telethon.tl.functions.messages import AddChatUserRequest
            from telethon.tl.types import Chat, Channel
            from telethon.errors import UserAlreadyParticipantError, FloodWaitError
            
            for session_name, _ in available_sessions:
                if session_name not in self.sessions:
                    continue
                    
                client = self.sessions[session_name]
                try:
                    me = await client.get_me()
                    if not me:
                        logger.warning(f"Could not get user info for session {session_name}")
                        continue
                    
                    # Check if user is already in the group by looking for clients joined groups
                    is_member = False
                    async for dialog in client.iter_dialogs():
                        entity = dialog.entity
                        

                        is_channel = isinstance(entity, Channel)
                        if is_channel and getattr(entity, 'broadcast', False):
                            try:
                                logger.debug("Skipping broadcast channel", entity_id=entity.id)
                            except UnicodeEncodeError:
                                
                                continue

                        if not (isinstance(entity, Chat) or is_channel):
                            try:
                                logger.debug("Skipping non-group dialog")
                            except UnicodeEncodeError:
                                continue
                    
                        #ignore if its a normal user not a group
                        if isinstance(dialog.entity, (Channel, Chat)) and dialog.is_group:
                            title = getattr(entity, 'title', None)
                            if not title and getattr(entity, 'participants_count', 0)==0:
                                try:
                                    logger.debug("Skipping dialog without title", entity_id=entity.id)
                                except UnicodeEncodeError:
                                    pass
                                    continue
                            if entity.id == group.id:
                                is_member = True
                    if is_member:
                        logger.info(f"Session {session_name} ({me.first_name}) already in group")
                        pass
                    else:
                        
                        try:
                            try:
                                #channel_input = get_input_channel(get_input_peer(group_input))
                                #print("Joining channel", channel_input.access_hash, channel_input.channel_id)
                                await client(JoinChannelRequest(f"https://t.me/{getattr(group, 'username', '')}"))
                                logger.info(f"Session {session_name} self-joined as fallback")
                            except Exception as join_error:
                                print(f"Join fallback failed for {session_name}: {join_error}")
                                raise join_error
                        except:
                            # Try to add user using admin session first (more reliable)
                            try:
                                user_input = await admin_client.get_input_entity(me.id)
                                
                                if isinstance(group, Channel):
                                    # For channels/supergroups, use admin to invite
                                    await admin_client(InviteToChannelRequest(group_input, [user_input]))
                                    logger.info(f"Admin added session {session_name} ({me.first_name}) to channel")
                                elif isinstance(group, Chat):
                                    # For regular groups, use admin to add
                                    await admin_client(AddChatUserRequest(
                                        chat_id=group_input.chat_id,
                                        user_id=user_input,
                                        fwd_limit=0
                                    ))
                                    logger.info(f"Admin added session {session_name} ({me.first_name}) to chat")
                                
                            except UserAlreadyParticipantError:
                                logger.info(f"Session {session_name} already a participant (confirmed)")
                            except FloodWaitError as e:
                                logger.warning(f"Flood wait when adding {session_name}: {e.seconds}s")
                                await asyncio.sleep(e.seconds + 1)
                            except Exception as admin_error:
                                logger.warning(f"unable to add user {session_name} ({me.first_name}) to group: {admin_error}")
                            # Small delay between additions to avoid rate limits
                            await asyncio.sleep(1)
                    
                except Exception as e:
                    logger.error(f"Failed to process session {session_name}: {e}")
                    
        except Exception as e:
            logger.error(f"Failed to ensure sessions in group: {e}")
    
    async def _create_invite_link(self, admin_client, group, group_input):
        """Create invite link for fallback."""
        try:
            from telethon.tl.types import Channel
            if isinstance(group, Channel) and not getattr(group, 'megagroup', False):
                logger.info("Invite link not available for broadcast channels", group_id=group.id)
                return None

            from telethon.tl.functions.messages import ExportChatInviteRequest
            result = await admin_client(ExportChatInviteRequest(group_input))
            return result.link
        except Exception as e:
            logger.error(f"Failed to create invite link: {e}")
            return None

    async def _invite_entity_to_group(self, client: TelegramClient, group, group_input, user_input):
        """Invite a user or session entity to the target group."""
        from telethon.tl.types import Chat, Channel
        from telethon.tl.functions.channels import InviteToChannelRequest
        from telethon.tl.functions.messages import AddChatUserRequest
        from telethon.errors import FloodWaitError, UserPrivacyRestrictedError, UserNotMutualContactError, UserAlreadyParticipantError
        try:
            if isinstance(group, Channel):
                if not getattr(group, 'megagroup', False):
                    return {"result": False, "error": "Cannot add users to broadcast channels", "type": "ValueError"}
                await client(InviteToChannelRequest(group_input, [user_input]))
            elif isinstance(group, Chat):
                await client(AddChatUserRequest(group.id, user_input, fwd_limit=0))
            else:
                return {"result": False, "error": "Invalid group type", "type": "ValueError"}
            return {"result": True, "message": "User invited successfully"}
        except UserAlreadyParticipantError:
            logger.info("User is already a participant", user_id=getattr(user_input, 'user_id', None))
            return {"result": True, "message": "User already a participant"}
        except UserPrivacyRestrictedError as e:
            logger.warning("User privacy settings prevent adding", error=str(e))
            return {"result": False, "error": str(e), "type": "UserPrivacyRestrictedError"}
        except UserNotMutualContactError as e:
            logger.warning("User is not a mutual contact", error=str(e))
            return {"result": False, "error": str(e), "type": "UserNotMutualContactError"}
        except FloodWaitError as e:
            logger.warning("Flood wait encountered", seconds=e.seconds)
            await asyncio.sleep(e.seconds + 5)
            return {"result": False, "error": str(e), "type": "FloodWaitError"}
        except Exception as e:
            logger.error("Failed to invite entity to group", error=str(e))
            return {"result": False, "error": str(e), "type": type(e).__name__}
        finally:
            logger.info("Invite operation completed")
    
    async def check_user_in_contacts(self, client: TelegramClient, active_contacts: set, phone: str):
        """Check if user is already in contacts."""
        if phone in active_contacts:
            logger.info("Phone already in contacts", phone=phone)
            return True
        return False
    
    async def _check_user_in_group(self, admin_client: TelegramClient, group, user_entity):
        """Check if user is already a member of the group using efficient search for large groups."""
        try:
            from telethon.tl.functions.channels import GetParticipantRequest
            from telethon.tl.types import Channel
            from telethon.errors import UserNotParticipantError
            
            if isinstance(group, Channel):
                # For channels/supergroups, use GetParticipantRequest for direct check
                try:
                    participant = await admin_client(GetParticipantRequest(group, user_entity))
                    print("participant",participant)
                    # Check if user is actually active (not left/banned)
                    from telethon.tl.types import ChannelParticipantLeft, ChannelParticipantBanned
                    if isinstance(participant.participant, (ChannelParticipantLeft, ChannelParticipantBanned)):
                        return False
                    if not getattr(participant, 'left', False):
                        return True
                except UserNotParticipantError:
                    return False
            else:
                # For regular chats with 10k+ members, use efficient search
                try:
                    # First try username search if available
                    username = getattr(user_entity, 'username', None)
                    if username:
                        async for participant in admin_client.iter_participants(group, search=username, limit=5):
                            if participant.id == user_entity.id and not getattr(participant, 'left', False):
                                return True
                    
                    # Fallback to ID-based search for large groups
                    user_id_str = str(user_entity.id)
                    async for participant in admin_client.iter_participants(group, search=user_id_str, limit=5):
                        if participant.id == user_entity.id and not getattr(participant, 'left', False):
                            return True
                    
                    return False
                except Exception:
                    # Final fallback for small groups only
                    try:
                        participants = await admin_client.get_participants(group, limit=200)
                        for p in participants:
                            if p.id == user_entity.id and not getattr(p, 'left', False):
                                return True
                        return False
                    except Exception:
                        return False
        except Exception as e:
            logger.debug(f"Error checking membership: {e}")
            return False

    async def _process_phone_number(self, client:TelegramClient, admin_client:TelegramClient, phone, group, group_input, session_name, invite_link, active_contacts):
        """Process single phone number with contact management."""
        try:
            # Step 1: Add to contacts temporarily
            phone_in_contact_list = await self.check_user_in_contacts(client, active_contacts, phone)
            if phone_in_contact_list:
                logger.info("Phone already in contacts", phone=phone)
            else:
                contact_added = await self._add_temp_contact(client, phone)
                logger.info("Temp contact status", phone=phone, contact_added=contact_added)
            group_input = await client.get_input_entity(group.id)
            group = await client.get_entity(group_input)
            try:
                # Step 2: Get user entity
                logger.info("Resolving user entity", phone=phone)
                user_entity = await client.get_entity(phone)
                logger.info(
                    "User entity resolved",
                    phone=phone,
                    user_id=getattr(user_entity, 'id', None),
                    username=getattr(user_entity, 'username', None)
                )
                user_input = await client.get_input_entity(user_entity.id)
                
                # # Step 2.5: Check if user is already in the group
                # is_member = await self._check_user_in_group(admin_client, group, user_entity)
                # if is_member:
                #     logger.info("User already in group", phone=phone, user_id=user_entity.id)
                #     self.db.mark_phone_added(phone)
                #     return "already_member"
                
                # Step 3: Try to add to group using user session
                logger.info(
                    "Attempting to invite user",
                    phone=phone,
                    session=session_name,
                    group_id=getattr(group, 'id', None)
                )
                result = await self._invite_entity_to_group(client, group, group_input, user_input)
                if result.get("result", False):
                    self.db.mark_phone_added(phone)
                    return "added"
                elif result.get("type") == "UserPrivacyRestrictedError":
                    # Try sending invite message for privacy-restricted users
                    if invite_link:
                        success = await self._send_invite_link(client, phone, invite_link)
                        if success:
                            self.db.mark_phone_invited(phone)
                            return "invited"
                    self.db.mark_phone_failed(phone)
                    return "Privacy restricted - invite failed"
                elif result.get("type") == "UserNotMutualContactError":
                    # Try sending invite message for non-mutual contact users
                    if invite_link:
                        success = await self._send_invite_link(client, phone, invite_link)
                        if success:
                            self.db.mark_phone_invited(phone)
                            return "invited"
                    self.db.add_to_blacklist(phone)
                    self.db.mark_phone_failed(phone)
                    return "Not mutual contact - invite failed"
                else:
                    raise Exception(result.get("error", "Unknown error during invite"))
                
            except Exception as add_error:
                # Step 5: If adding fails, try invite link
                if invite_link:
                    try:
                        logger.info(f"Attempting invite link fallback {add_error}", phone=phone)
                        # Send invite link via direct message
                        success = await self._send_invite_link(client, phone, invite_link)
                        if success:
                            self.db.mark_phone_invited(phone)  # Mark as processed
                            logger.info(
                                "Invite link sent",
                                phone=phone,
                                session=session_name,
                                group_id=getattr(group, 'id', None)
                            )
                            return "invited"
                        else:
                            logger.error(
                                "Invite delivery failed",
                                phone=phone,
                                session=session_name
                            )
                    except Exception as invite_error:
                        logger.error(
                            "Invite delivery exception",
                            phone=phone,
                            session=session_name,
                            error=str(invite_error)
                        )
                
                # Mark as failed
                self.db.mark_phone_failed(phone)
                
                # Add to blacklist for non-Telegram users and certain errors
                error_msg = str(add_error).lower()
                if any(err in error_msg for err in ["no user", "not found", "invalid", "not mutual"]):
                    self.db.add_to_blacklist(phone)
                    logger.info("Added to blacklist - non-Telegram user or blocked", phone=phone, error=error_msg)
                
                logger.warning(
                    "Failed to add user, marked as failed",
                    phone=phone,
                    session=session_name,
                    error=str(add_error)
                )

                return str(add_error)
                
            finally:
                # Step 6: Always remove from contacts if it was added temporarily
                if 'contact_added' in locals() and contact_added and not phone_in_contact_list:
                    logger.info("Cleaning up temp contact", phone=phone)
                    await self._remove_temp_contact(client, phone)
                    
        except Exception as e:
            self.db.mark_phone_failed(phone)
            logger.error(
                "Uncaught error while processing phone",
                phone=phone,
                session=session_name,
                error=str(e)
            )
            return str(e)
    
    async def get_active_contact_lists(self, client: TelegramClient):
        """Get active contact lists to prevent duplicate contacts."""
        try:
            phone_set = set()
            async for contact in client.iter_dialogs():
                entity = contact.entity
                if hasattr(entity, 'phone'):
                    if entity.phone:
                        phone_set.add(entity.phone)
            logger.info(f"Fetched {len(phone_set)} active contacts")
            return phone_set
        except Exception as e:
            logger.error(f"Failed to fetch active contacts: {e}")
            return set()
        
    async def _add_temp_contact(self, client: TelegramClient, phone):
        """Add phone to contacts temporarily."""
        try:
            from telethon.tl.functions.contacts import ImportContactsRequest
            from telethon.tl.types import InputPhoneContact
            #first check if the contact is already in contacts
            
            contact = InputPhoneContact(
                client_id=0,
                phone=phone,
                first_name="Temp",
                last_name="Contact"
            )
            
            await client(ImportContactsRequest([contact]))
            logger.info("Temp contact added", phone=phone)
            return True
        except Exception as e:
            logger.error(f"Failed to add temp contact {phone}: {e}")
            return False
    
    async def _remove_temp_contact(self, client, phone):
        """Remove phone from contacts."""
        try:
            from telethon.tl.functions.contacts import DeleteContactsRequest
            
            user = await client.get_input_entity(phone)
            await client(DeleteContactsRequest([user]))
            logger.info(f"Removed temp contact {phone}")
        except Exception as e:
            logger.error(f"Failed to remove temp contact {phone}: {e}")

    async def _send_invite_link(self, client: TelegramClient, phone, invite_link):
        """Send invite link via Telegram direct message."""
        try:
            # Get custom invite message from admin settings
            invite_message = self.db.get_invite_message()
            message = invite_message.format(invite_link=invite_link)
            
            # Get user entity
            logger.info("Resolving entity for invite message", phone=phone)
            user = await client.get_input_entity(phone)
            
            # Send direct message
            logger.info("Sending invite message", phone=phone)
            await client.send_message(user, message)
            logger.info(f"Sent invite message to {phone}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to send invite message to {phone}: {e}")
            return False