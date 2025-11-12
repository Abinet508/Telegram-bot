"""Background supervisor for auto add automation."""
import asyncio
from datetime import datetime, timedelta, time, timezone
from typing import Optional
import structlog


class AutoAddSupervisor:
    """Coordinates scheduled auto add runs without blocking FastAPI."""

    def __init__(self, session_manager, admin_manager, db, poll_interval: int = 120):
        self.session_manager = session_manager
        self.admin_manager = admin_manager
        self.db = db
        self.poll_interval = max(30, poll_interval)
        self._task: Optional[asyncio.Task] = None
        self._stop_event: asyncio.Event = asyncio.Event()
        self._wake_event: asyncio.Event = asyncio.Event()
        self._lock = asyncio.Lock()
        self._status = {
            "running": False,
            "next_run": None,
        }
        self._logger = structlog.get_logger(__name__)
        # Avoid hammering Telegram or SQLite more frequently than necessary
        self._min_run_interval = timedelta(minutes=5)

    async def start(self):
        """Ensure supervisor loop is running."""
        async with self._lock:
            if self._task and not self._task.done():
                return
            self._stop_event = asyncio.Event()
            self._wake_event = asyncio.Event()
            self._task = asyncio.create_task(self._run_loop(), name="auto-add-supervisor")
            self._logger.info("Auto add supervisor started")

    async def shutdown(self):
        """Stop supervisor loop gracefully."""
        async with self._lock:
            if not self._task:
                return
            self._stop_event.set()
            self._wake_event.set()
            task = self._task
            self._task = None
        try:
            await task
        finally:
            self.db.set_setting('auto_add_running', False)
            self.db.set_setting('auto_add_enabled', False)
            self._logger.info("Auto add supervisor stopped")

    async def wake_up(self, immediate: bool = False):
        """Signal the supervisor to re-evaluate conditions."""
        self._wake_event.set()
        if immediate:
            # Store timestamp for observability; no scheduling logic relies on this flag directly
            self.db.set_setting('auto_add_forced_wakeup', datetime.now(timezone.utc).isoformat())

    def status(self):
        """Return current automation status snapshot."""
        settings = self.admin_manager.get_settings()
        stats = self.db.get_stats()
        return {
            "running": self._status.get("running", False),
            "enabled": settings.get('auto_add_enabled', False),
            "next_run": self._status.get("next_run"),
            "last_run": self.db.get_setting('auto_add_last_run'),
            "pending_phones": stats.get('pending_phones', 0),
            "total_group_members": stats.get('total_group_members', 0),
            "target_group_id": settings.get('target_group_id'),
            "batch_size": settings.get('batch_size'),
            "delay_between_adds": settings.get('delay_between_adds'),
            "max_users_per_session": settings.get('max_users_per_session'),
            "last_result": self.db.get_setting('auto_add_last_result'),
        }

    async def _run_loop(self):
        """Main supervisor loop coordinating scheduled runs."""
        #always update auto_add_last_run on start to current time to prevent immediate run
        self.db.set_setting('auto_add_last_run', datetime.now(timezone.utc).isoformat())
        while not self._stop_event.is_set():
            if self._wake_event.is_set():
                self._wake_event.clear()

            settings = self.admin_manager.get_settings()
            enabled = settings.get('auto_add_enabled', False)
            target_group = settings.get('target_group_id')
            pending_count = self._get_pending_phone_count()
            system_time_zone = timezone.utc
            now_utc = datetime.now(system_time_zone)
            next_run = self._calculate_next_run(settings, now_utc)
            self._status['next_run'] = next_run.isoformat() if next_run else None

            self._logger.info(
                "Supervisor heartbeat",
                enabled=enabled,
                pending=pending_count,
                target_group=target_group,
                next_run=self._status['next_run'],
                running=self._status.get('running', False)
            )

            if not enabled:
                self._logger.info("Automation disabled via settings; sleeping", sleep_seconds=self.poll_interval)
                await self._wait(self.poll_interval)
                continue

            if not target_group:
                self._logger.warning("Auto add skipped: no target group configured")
                await self._wait(self.poll_interval)
                continue

            if pending_count == 0:
                self._logger.info("Auto add idle: no pending phone numbers queued")
                await self._wait(self.poll_interval)
                continue

            if next_run and now_utc < next_run:
                wait_seconds = max(30, (next_run - now_utc).total_seconds())
                self._logger.info(
                    "Waiting for next run window",
                    wait_seconds=wait_seconds,
                    next_run=self._status['next_run']
                )
                await self._wait(min(wait_seconds, self.poll_interval))
                continue

            self._logger.info("Conditions met; launching auto add cycle", target_group=target_group)
            await self._execute_cycle(target_group, settings)

        self._logger.info("Auto add supervisor loop exited")

    async def _execute_cycle(self, target_group_id, settings):
        """Run a single add-to-group cycle."""
        self._status['running'] = True
        self.db.set_setting('auto_add_running', True)
        self._logger.info("Auto add cycle starting", target_group=target_group_id)

        try:
            await self.session_manager.refresh_active_sessions()
            self._logger.info("Active sessions refreshed", active=len(self.session_manager.sessions))

            delay = settings.get('delay_between_adds') or 30
            batch_size = settings.get('batch_size') or 5
            max_daily = settings.get('max_users_per_session') or 80
            invite_message = settings.get('invite_message')

            try:
                group_id = int(target_group_id)
            except (TypeError, ValueError):
                group_id = target_group_id

            result = await self.session_manager.add_users_to_group(
                group_id=group_id,
                delay=delay,
                batch_size=batch_size,
                max_daily_per_session=max_daily,
                invite_message=invite_message,
            )
            self._logger.info("Auto add cycle completed", result=result)
            try:
                await self.session_manager.refresh_target_group_member_count(group_id)
            except Exception as count_error:  # pragma: no cover - defensive logging
                self._logger.warning("Failed to refresh group member count", error=str(count_error))

            if not result.get('success'):
                self._logger.warning("Auto add execution reported failure", result=result)

            self.db.set_setting('auto_add_last_result', result)
            self.db.set_setting('auto_add_last_run', datetime.now(timezone.utc).isoformat())
        except Exception as exc:  # pragma: no cover - defensive logging
            self._logger.error("Auto add cycle failed", error=str(exc))
            self.db.set_setting('auto_add_last_result', {
                "success": False,
                "error": str(exc),
            })
        finally:
            self._status['running'] = False
            self.db.set_setting('auto_add_running', False)
            self._wake_event.clear()
            self._logger.info("Auto add cycle finished")

    def _get_pending_phone_count(self) -> int:
        stats = self.db.get_stats()
        return stats.get('pending_phones', 0)

    def _calculate_next_run(self, settings, now_utc: datetime) -> Optional[datetime]:
        # Check for forced wakeup (immediate execution requested)
        forced_wakeup = self.db.get_setting('auto_add_forced_wakeup')
        if forced_wakeup:
            # Clear the forced wakeup flag and return immediate execution
            self.db.set_setting('auto_add_forced_wakeup', None)
            return now_utc
        
        last_run_iso = self.db.get_setting('auto_add_last_run')
        last_run = None
        if last_run_iso:
            try:
                last_run = datetime.fromisoformat(last_run_iso)
            except ValueError:
                last_run = None

        daily_start = settings.get('daily_start_time')
        if daily_start:
            try:
                hour, minute = [int(part) for part in daily_start.split(':', 1)]
                start_today = datetime.combine(now_utc.date(), time(hour, minute, tzinfo=timezone.utc))
            except (ValueError, TypeError):
                start_today = now_utc

            if last_run:
                if last_run.tzinfo is None:
                    last_run = last_run.replace(tzinfo=timezone.utc)
                if last_run.date() == now_utc.date() and last_run >= start_today:
                    return start_today + timedelta(days=1)

            if now_utc >= start_today:
                return now_utc

            return start_today

        if not last_run:
            return now_utc

        if last_run.tzinfo is None:
            last_run = last_run.replace(tzinfo=timezone.utc)
        earliest_next = last_run + self._min_run_interval
        return earliest_next if earliest_next > now_utc else now_utc

    async def _wait(self, timeout_seconds: float):
        """Wait for timeout, stop signal, or wake-up signal."""
        timeout = max(5.0, float(timeout_seconds))
        stop_task = asyncio.create_task(self._stop_event.wait())
        wake_task = asyncio.create_task(self._wake_event.wait())
        try:
            done, pending = await asyncio.wait(
                {stop_task, wake_task},
                timeout=timeout,
                return_when=asyncio.FIRST_COMPLETED
            )
            if wake_task in done:
                self._wake_event.clear()
        finally:
            for task in (stop_task, wake_task):
                if not task.done():
                    task.cancel()

