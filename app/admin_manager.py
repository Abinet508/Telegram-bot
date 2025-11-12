"""Admin control system for automated operations."""
import json
import asyncio
from datetime import datetime, timedelta
from pathlib import Path
import structlog

logger = structlog.get_logger(__name__)

class AdminManager:
    """Manages admin controls and automated operations."""
    
    def __init__(self, app_config, db):
        self.app_config = app_config
        self.db = db
        self.active_operations = {}
    
    def _get_default_settings(self):
        """Get default settings."""
        return {
            "session_creation_enabled": False,
            "auto_add_enabled": False,
            "target_group_id": None,
            "daily_start_time": None,
            "max_users_per_session": 50,
            "delay_between_adds": 10,
            "batch_size": 3
        }
    
    def update_settings(self, **kwargs):
        """Update admin settings."""
        for key, value in kwargs.items():
            self.db.set_setting(key, value)
    
    def get_settings(self):
        """Get current settings."""
        settings = self._get_default_settings()
        db_settings = self.db.get_all_settings()
        settings.update(db_settings)
        return settings
    
    def is_session_creation_enabled(self):
        """Check if session creation is enabled."""
        return self.db.get_setting("session_creation_enabled", False)
    
    def is_auto_add_enabled(self):
        """Check if auto add is enabled."""
        return self.db.get_setting("auto_add_enabled", False)
    
    def get_next_run_time(self):
        """Calculate next run time based on daily schedule."""
        daily_start_time = self.db.get_setting("daily_start_time")
        if not daily_start_time:
            return None
        
        now = datetime.now()
        start_time = datetime.strptime(daily_start_time, "%H:%M").time()
        next_run = datetime.combine(now.date(), start_time)
        
        if next_run <= now:
            next_run += timedelta(days=1)
        
        return next_run
    
    def should_run_auto_add(self):
        """Check if auto add should run now."""
        if not self.is_auto_add_enabled():
            return False
        
        next_run = self.get_next_run_time()
        if not next_run:
            return False
        
        now = datetime.now()
        return abs((now - next_run).total_seconds()) < 30  # 30 second window
    
    async def update_target_group_count(self, client):
        """Update member count for target group using provided client."""
        try:
            target_group_id = self.db.get_setting('target_group_id')
            if not target_group_id:
                logger.warning("No target group set")
                return None
            
            # Convert to int if needed
            try:
                target_group_id = int(target_group_id)
            except (TypeError, ValueError):
                logger.error(f"Invalid target group ID: {target_group_id}")
                return None
            
            # Check if client is connected and authorized
            if not client.is_connected():
                await client.connect()
            
            if not await client.is_user_authorized():
                logger.error("Client is not authorized")
                return None
            
            # Get group entity and member count
            entity = await client.get_entity(target_group_id)
            
            logger.info("Group entity details", 
                       entity_type=type(entity).__name__,
                       has_participants_count=hasattr(entity, 'participants_count'),
                       participants_count_value=getattr(entity, 'participants_count', 'NOT_FOUND'),
                       entity_id=entity.id,
                       title=getattr(entity, 'title', 'NO_TITLE'))
            
            if hasattr(entity, 'participants_count') and entity.participants_count is not None:
                member_count = entity.participants_count
                logger.info("Using entity participants_count", count=member_count)
            else:
                # Fallback: get actual participants count
                try:
                    logger.info("Fetching participants manually")
                    participants = await client.get_participants(entity, limit=0)
                    member_count = getattr(participants, 'total', len(participants))
                    logger.info("Manual participants fetch result", count=member_count, total_attr=getattr(participants, 'total', 'NO_TOTAL'))
                except Exception as e:
                    logger.warning("Failed to get participants, trying alternative method", error=str(e))
                    # Try getting full chat info
                    try:
                        from telethon.tl.functions.channels import GetFullChannelRequest
                        from telethon.tl.functions.messages import GetFullChatRequest
                        from telethon.tl.types import Channel, Chat
                        
                        if isinstance(entity, Channel):
                            full_info = await client(GetFullChannelRequest(entity))
                            member_count = getattr(full_info.full_chat, 'participants_count', 0)
                        elif isinstance(entity, Chat):
                            full_info = await client(GetFullChatRequest(entity.id))
                            member_count = getattr(full_info.full_chat, 'participants_count', 0)
                        else:
                            member_count = 0
                        
                        logger.info("Alternative method result", count=member_count)
                    except Exception as e2:
                        logger.warning("All methods failed, using 0", error=str(e2))
                        member_count = 0
            
            # Ensure member_count is not None
            if member_count is None:
                logger.warning("Member count is still None, setting to 0")
                member_count = 0
            
            # Update admin_groups table
            self.db.update_group_member_count(
                target_group_id, 
                member_count, 
                getattr(entity, 'title', None),
                getattr(entity, 'username', None)
            )
            
            # Save to admin settings for backward compatibility
            self.db.set_setting('target_group_member_count', member_count)
            self.db.set_setting('target_group_last_updated', datetime.now().isoformat())
            
            logger.info("Updated target group member count", count=member_count, group_id=target_group_id)
            return member_count
            
        except Exception as e:
            logger.error("Failed to update target group count", error=str(e), group_id=target_group_id if 'target_group_id' in locals() else 'unknown')
            # Set a default count of 0 in the database so the UI doesn't break
            try:
                if 'target_group_id' in locals():
                    self.db.update_group_member_count(target_group_id, 0, "Unknown Group", None)
            except Exception:
                pass
            return None