"""FastAPI main application for Telegram Bot."""
import asyncio
import uuid
import logging
from logging.handlers import RotatingFileHandler
from fastapi import FastAPI, HTTPException, Depends, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware
from pydantic import BaseModel
from typing import Optional, Dict, Any
from fastapi import UploadFile, File
from contextlib import asynccontextmanager
import structlog
from dotenv import load_dotenv

# Load environment variables first
load_dotenv()

from config.settings import load_config
from app.session_manager import SessionManager
from app.admin_manager import AdminManager
from app.database import Database
from app.auth import check_password, check_user_password
from app.auto_add import AutoAddSupervisor


def configure_logging(log_dir):
    """Configure structlog and standard logging with file persistence."""
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / "automation.log"

    timestamper = structlog.processors.TimeStamper(fmt="iso")
    pre_chain = [
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        timestamper,
    ]

    formatter = logging.Formatter("%(message)s")

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    file_handler = RotatingFileHandler(
        log_path,
        maxBytes=5 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8"
    )
    file_handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.handlers = []
    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)

    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            timestamper,
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

# Initialize configs
telegram_config, app_config = load_config()
configure_logging(app_config.logs_dir)

logger = structlog.get_logger(__name__)

# Initialize database
db_path = app_config.data_dir / "telegram_bot.db"
db = Database(db_path)

# Initialize managers
session_manager = SessionManager(telegram_config, app_config, db)
admin_manager = AdminManager(app_config, db)
auto_add_supervisor = AutoAddSupervisor(session_manager, admin_manager, db)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    session_manager.start_cleanup_task()
    await auto_add_supervisor.start()
    
    # Preload sessions for better performance
    asyncio.create_task(preload_sessions())
    
    # Update member counts on startup
    asyncio.create_task(startup_member_count_update())
    
    yield
    
    # Shutdown
    await auto_add_supervisor.shutdown()

# FastAPI app with optimizations for concurrent requests
app = FastAPI(
    title="Telegram Bot Manager", 
    version="1.0.0",
    docs_url=None,  # Disable docs for performance
    redoc_url=None,  # Disable redoc for performance
    lifespan=lifespan
)
app.add_middleware(SessionMiddleware, secret_key="your-secret-key-here", max_age=3600)

# Add CORS for better performance
from fastapi.middleware.cors import CORSMiddleware

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

async def preload_sessions():
    """Preload all sessions on startup for better performance"""
    try:
        logger.info("Preloading sessions on startup...")
        await session_manager.refresh_active_sessions()
        logger.info(f"Preloaded {len(session_manager.sessions)} user sessions")
    except Exception as e:
        logger.error("Failed to preload sessions on startup", error=str(e))

async def startup_member_count_update():
    """Update member counts on application startup."""
    try:
        # Check if target group is set first
        target_group_id = db.get_setting('target_group_id')
        if not target_group_id:
            logger.info("No target group set, skipping startup member count update")
            return
            
        admin_session = db.get_admin_session()
        if admin_session:
            client = await session_manager.get_session(admin_session['session_name'])
            if client:
                await admin_manager.update_target_group_count(client)
                logger.info("Target group count updated on startup")
        else:
            logger.info("No admin session available for startup member count update")
    except Exception as e:
        logger.error("Failed to update member counts on startup", error=str(e))

# Static files and templates with cache control
from fastapi.staticfiles import StaticFiles

class NoCacheStaticFiles(StaticFiles):
    def file_response(self, *args, **kwargs):
        response = super().file_response(*args, **kwargs)
        response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
        return response

app.mount("/static", NoCacheStaticFiles(directory="app/static"), name="static")
templates = Jinja2Templates(directory="app/templates")

# Pydantic models
class QRSessionResponse(BaseModel):
    success: bool
    data: Optional[Dict[str, Any]] = None
    error: Optional[str] = None

class StatusResponse(BaseModel):
    success: bool
    data: Optional[Dict[str, Any]] = None
    error: Optional[str] = None

# Auth dependency
async def require_admin(request: Request):
    if not request.session.get('admin_logged_in'):
        raise HTTPException(status_code=401, detail="Admin login required")
    return True

def is_authenticated(request: Request) -> bool:
    return request.session.get('admin_logged_in', False)

# Routes
@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("dashboard.html", {"request": request})

@app.get("/qr", response_class=HTMLResponse)
async def qr_page(request: Request):
    if not admin_manager.is_session_creation_enabled():
        return templates.TemplateResponse("disabled.html", {
            "request": request, 
            "message": "Session creation is currently disabled"
        })
    return templates.TemplateResponse("qr.html", {"request": request})

@app.middleware("http")
async def redirect_unauthenticated(request: Request, call_next):
    # Skip middleware for static files and public endpoints
    if (request.url.path.startswith("/static") or 
        request.url.path in ["/", "/qr", "/login"] or
        request.url.path.startswith("/api/sessions/qr") or
        request.url.path.startswith("/api/sessions/") and "qr-status" in request.url.path or
        request.url.path.startswith("/api/sessions/") and "verify-2fa" in request.url.path or
        request.url.path == "/api/stats"):
        return await call_next(request)
    
    # Check if accessing admin pages without authentication
    if (request.url.path.startswith("/admin") or 
        request.url.path.startswith("/phones") or
        (request.url.path.startswith("/api/") and "admin" in request.url.path)):
        # Check if session exists in scope (SessionMiddleware loaded)
        if "session" in request.scope and not request.session.get('admin_logged_in'):
            if request.url.path.startswith("/api/"):
                from fastapi.responses import JSONResponse
                return JSONResponse({"error": "Authentication required"}, status_code=401)
            # Show access denied page for admin pages, redirect to login for phones
            if request.url.path.startswith("/admin"):
                return templates.TemplateResponse("access_denied.html", {
                    "request": request,
                    "message": "You are not in the allowed list to access this page. Please log in first.",
                    "redirect_url": "/login",
                    "redirect_text": "Go to Login"
                })
            return RedirectResponse(url="/login", status_code=302)
    
    return await call_next(request)

@app.get("/admin", response_class=HTMLResponse)
async def admin_page(request: Request):
    # Check if user is logged in
    if not request.session.get('admin_logged_in'):
        return templates.TemplateResponse("access_denied.html", {
            "request": request,
            "message": "You are not in the allowed list to access this page. Please log in first.",
            "redirect_url": "/login",
            "redirect_text": "Go to Login"
        })
    
    # Check if user has admin role
    user_role = request.session.get('user_role', 'user')
    if user_role != 'admin':
        return templates.TemplateResponse("access_denied.html", {
            "request": request,
            "message": "You are not in the allowed list to access this page.",
            "redirect_url": "/phones",
            "redirect_text": "Return to Phone Management"
        })
    return templates.TemplateResponse("admin.html", {"request": request})

@app.get("/admin/auth", response_class=HTMLResponse)
async def admin_auth_page(request: Request):
    # Check if user is logged in
    if not request.session.get('admin_logged_in'):
        return templates.TemplateResponse("access_denied.html", {
            "request": request,
            "message": "You are not in the allowed list to access this page. Please log in first.",
            "redirect_url": "/login",
            "redirect_text": "Go to Login"
        })
    
    # Check if user has admin role
    user_role = request.session.get('user_role', 'user')
    if user_role != 'admin':
        return templates.TemplateResponse("access_denied.html", {
            "request": request,
            "message": "You are not in the allowed list to access this page.",
            "redirect_url": "/phones",
            "redirect_text": "Return to Phone Management"
        })
    return templates.TemplateResponse("admin_auth.html", {"request": request})



@app.get("/phones", response_class=HTMLResponse)
async def phones_page(request: Request, _: bool = Depends(require_admin)):
    return templates.TemplateResponse("phones.html", {"request": request})

@app.get("/admin/qr", response_class=HTMLResponse)
async def admin_qr_page(request: Request):
    if not request.session.get('admin_logged_in'):
        return RedirectResponse(url="/login", status_code=302)
    # Check if user has admin role
    user_role = request.session.get('user_role', 'user')
    if user_role != 'admin':
        return templates.TemplateResponse("access_denied.html", {
            "request": request,
            "message": "You are not in the allowed list to access this page.",
            "redirect_url": "/phones",
            "redirect_text": "Return to Phone Management"
        })
    return templates.TemplateResponse("admin_qr.html", {"request": request})

@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    if request.session.get('admin_logged_in'):
        return RedirectResponse(url="/admin", status_code=302)
    return templates.TemplateResponse("login.html", {"request": request})

@app.post("/login")
async def login(request: Request, username: str = Form(...), password: str = Form(...)):
    if check_user_password(username, password):
        from app.auth import get_user_role
        request.session['admin_logged_in'] = True
        request.session['admin_username'] = username
        request.session['user_role'] = get_user_role(username)
        return RedirectResponse(url="/admin", status_code=302)
    return templates.TemplateResponse("login.html", {
        "request": request, 
        "error": "Invalid credentials"
    })

@app.get("/logout")
async def logout(request: Request):
    request.session.pop('admin_logged_in', None)
    return RedirectResponse(url="/", status_code=302)

@app.post("/api/sessions/qr", response_model=QRSessionResponse)
async def create_qr_session():
    if not admin_manager.is_session_creation_enabled():
        return QRSessionResponse(success=False, error="Session creation is disabled")
    
    try:
        # Use asyncio timeout for faster response
        import asyncio
        result = await asyncio.wait_for(
            session_manager.create_auto_qr_session(), 
            timeout=8.0  # 8 second timeout
        )
        
        if "error" in result:
            return QRSessionResponse(success=False, error=result["error"])
        
        return QRSessionResponse(success=True, data=result)
        
    except asyncio.TimeoutError:
        logger.error("QR session creation timeout")
        return QRSessionResponse(success=False, error="QR generation timeout - please try again")
    except Exception as e:
        logger.error("Failed to create session", error=str(e))
        return QRSessionResponse(success=False, error=str(e))

@app.get("/api/sessions/{session_name}/qr-status", response_model=StatusResponse)
async def qr_status(session_name: str):
    try:
        # Fast status check with timeout
        import asyncio
        result = await asyncio.wait_for(
            asyncio.to_thread(session_manager.check_qr_status, session_name),
            timeout=3.0
        )
        return StatusResponse(success=True, data=result)
    except asyncio.TimeoutError:
        return StatusResponse(success=False, error="Status check timeout")
    except Exception as e:
        return StatusResponse(success=False, error=str(e))

@app.get("/api/admin/{session_name}/qr-status", response_model=StatusResponse)
async def admin_qr_status(session_name: str, _: bool = Depends(require_admin)):
    try:
        import asyncio
        result = await asyncio.wait_for(
            asyncio.to_thread(session_manager.check_qr_status, session_name),
            timeout=3.0
        )
        return StatusResponse(success=True, data=result)
    except asyncio.TimeoutError:
        return StatusResponse(success=False, error="Status check timeout")
    except Exception as e:
        return StatusResponse(success=False, error=str(e))

@app.post("/api/sessions/{session_name}/verify-2fa", response_model=StatusResponse)
async def verify_2fa(session_name: str, password: str = Form(...)):
    try:
        result = await session_manager.verify_2fa_password(session_name, password)
        return StatusResponse(success=True, data=result)
    except Exception as e:
        return StatusResponse(success=False, error=str(e))

@app.post("/api/admin/{session_name}/verify-2fa", response_model=StatusResponse)
async def verify_admin_2fa(session_name: str, password: str = Form(...), _: bool = Depends(require_admin)):
    try:
        result = await session_manager.verify_admin_2fa_password(session_name, password)
        return StatusResponse(success=True, data=result)
    except Exception as e:
        return StatusResponse(success=False, error=str(e))

@app.get("/api/sessions")
async def list_sessions(offset: int = 0, limit: int = 10, _: bool = Depends(require_admin)):
    sessions = session_manager.list_sessions(offset=offset, limit=limit)
    total = session_manager.get_sessions_count()
    return {"success": True, "sessions": sessions, "total": total, "offset": offset, "limit": limit}

@app.get("/api/stats")
async def get_stats():
    """Get basic stats for dashboard - no auth required"""
    sessions = session_manager.list_sessions()
    active_sessions = [s for s in sessions if s.get('status') == 'active']
    db_stats = db.get_stats()
    
    # Get target group member count from admin_groups table
    target_group_count = db.get_member_count()
    
    # If no count available, try to get from settings as fallback
    if target_group_count == 0:
        target_group_count = db.get_setting('target_group_member_count', 0)

    return {
        "success": True,
        "stats": {
            "total_sessions": db_stats.get('total_sessions', len(sessions)),
            "active_sessions": db_stats.get('active_sessions', len(active_sessions)),
            "pending_sessions": len([s for s in sessions if s.get('status') in ['generating', 'waiting']]),
            "members_added": db_stats.get('added_phones', 0),
            "total_group_members": target_group_count,
            "pending_phones": db_stats.get('pending_phones', 0),
            "active_operations": db_stats.get('active_operations', 0)
        }
    }

@app.delete("/api/sessions/{session_name}")
async def delete_session(session_name: str, _: bool = Depends(require_admin)):
    try:
        success = await session_manager.remove_session(session_name)
        return {"success": success}
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.delete("/api/sessions")
async def delete_all_sessions(_: bool = Depends(require_admin)):
    try:
        success = await session_manager.remove_all_sessions()
        return {"success": success}
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.get("/api/admin/settings")
async def get_admin_settings(_: bool = Depends(require_admin)):
    return {"success": True, "settings": admin_manager.get_settings()}

@app.post("/api/admin/settings")
async def update_admin_settings(settings: Dict[str, Any], _: bool = Depends(require_admin)):
    admin_manager.update_settings(**settings)
    return {"success": True}

@app.get("/api/admin/users")
async def get_admin_users(_: bool = Depends(require_admin)):
    from app.auth import get_all_users
    users = get_all_users()
    return {"success": True, "users": users}

@app.post("/api/admin/users")
async def create_admin_user(request: Request, _: bool = Depends(require_admin)):
    if request.session.get('user_role') != 'admin':
        return {"success": False, "error": "Admin role required"}
    from app.auth import create_user
    data = await request.json()
    username = data.get('username')
    password = data.get('password')
    role = data.get('role', 'user')
    if not username or not password:
        return {"success": False, "error": "Username and password required"}
    success = create_user(username, password, role)
    return {"success": success, "error": None if success else "User already exists"}

@app.delete("/api/admin/users/{username}")
async def delete_admin_user(username: str, request: Request, _: bool = Depends(require_admin)):
    if request.session.get('user_role') != 'admin':
        return {"success": False, "error": "Admin role required"}
    from app.auth import delete_user
    success = delete_user(username)
    return {"success": success}

@app.post("/api/admin/users/{username}/role")
async def change_user_role_endpoint(username: str, request: Request, _: bool = Depends(require_admin)):
    if request.session.get('user_role') != 'admin':
        return {"success": False, "error": "Admin role required"}
    from app.auth import change_user_role
    data = await request.json()
    new_role = data.get('role')
    if not new_role or new_role not in ['admin', 'user']:
        return {"success": False, "error": "Valid role required"}
    success = change_user_role(username, new_role)
    return {"success": success}

@app.post("/api/admin/change-password")
async def change_password(request: Request, _: bool = Depends(require_admin)):
    from app.auth import change_user_password
    data = await request.json()
    current_username = request.session.get('admin_username', 'admin')
    old_password = data.get('old_password')
    new_password = data.get('new_password')
    if not old_password or not new_password:
        return {"success": False, "error": "Old and new passwords required"}
    success = change_user_password(current_username, old_password, new_password)
    return {"success": success, "error": None if success else "Invalid current password"}

@app.get("/api/phones")
async def get_phone_numbers(offset: int = 0, limit: int = 25, _: bool = Depends(require_admin)):
    phones = session_manager.get_phone_numbers(offset=offset, limit=limit)
    total = session_manager.get_phone_numbers_count()
    return {"success": True, "phones": phones, "total": total, "offset": offset, "limit": limit}

@app.post("/api/phones/upload")
async def upload_phone_numbers(file: UploadFile = File(None), text: str = Form(None), _: bool = Depends(require_admin)):
    try:
        phone_numbers = []
        
        if file:
            # Handle Excel/CSV file
            content = await file.read()
            if file.filename.endswith('.xlsx'):
                import pandas as pd
                import io
                df = pd.read_excel(io.BytesIO(content))
                # Look for phone numbers in any column
                for col in df.columns:
                    for val in df[col].dropna():
                        val_str = str(val).strip()
                        if val_str.startswith('+') and len(val_str) > 10:
                            phone_numbers.append(val_str)
            else:
                # Handle text file
                lines = content.decode('utf-8').strip().split('\n')
                for line in lines:
                    line = line.strip()
                    if line and line.startswith('+'):
                        phone_numbers.append(line)
        elif text:
            # Handle text input
            lines = text.strip().split('\n')
            for line in lines:
                line = line.strip()
                if line and line.startswith('+'):
                    phone_numbers.append(line)
        
        if not phone_numbers:
            return {"success": False, "error": "No valid phone numbers found"}
        
        added = session_manager.import_phone_numbers(phone_numbers)
        return {"success": True, "added": added, "total": len(phone_numbers)}
        
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.delete("/api/phones/{phone}")
async def delete_phone_number(phone: str, _: bool = Depends(require_admin)):
    success = session_manager.remove_phone_number(phone)
    return {"success": success}

@app.delete("/api/phones")
async def delete_all_phone_numbers(_: bool = Depends(require_admin)):
    try:
        count = db.delete_all_phone_numbers()
        return {"success": True, "deleted_count": count}
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.post("/api/phones/automation")
async def start_phone_automation(
    group_id: int = Form(...), 
    delay: int = Form(30), 
    batch_size: int = Form(5), 
    max_daily: int = Form(80),
    invite_message: str = Form(None),
    _: bool = Depends(require_admin)
):
    try:
        result = await session_manager.add_users_to_group(group_id, delay, batch_size, max_daily, invite_message)
        return result
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.get("/api/admin/invite-message")
async def get_invite_message(_: bool = Depends(require_admin)):
    try:
        message = session_manager.db.get_invite_message()
        return {"success": True, "message": message}
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.post("/api/admin/invite-message")
async def set_invite_message(message: str = Form(...), _: bool = Depends(require_admin)):
    try:
        session_manager.db.set_invite_message(message)
        return {"success": True}
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.get("/api/sessions/limits")
async def get_session_limits(_: bool = Depends(require_admin)):
    try:
        limits = {}
        for session_name in session_manager.sessions.keys():
            remaining = session_manager.db.get_session_daily_limit(session_name)
            limits[session_name] = remaining
        return {"success": True, "limits": limits}
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.get("/api/groups/{group_id}")
async def get_group_info(group_id: int, _: bool = Depends(require_admin)):
    try:
        group_info = await session_manager.get_group_by_id(group_id)
        if group_info:
            return {"success": True, "group": group_info}
        else:
            return {"success": False, "error": "Group not found"}
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.post("/api/admin/qr", response_model=QRSessionResponse)
async def create_admin_qr_session(_: bool = Depends(require_admin)):
    try:
        import asyncio
        result = await asyncio.wait_for(
            session_manager.create_admin_qr_session(),
            timeout=8.0
        )
        
        if "error" in result:
            return QRSessionResponse(success=False, error=result["error"])
        
        return QRSessionResponse(success=True, data=result)
        
    except asyncio.TimeoutError:
        return QRSessionResponse(success=False, error="Admin QR generation timeout")
    except Exception as e:
        logger.error("Failed to create admin session", error=str(e))
        return QRSessionResponse(success=False, error=str(e))

@app.get("/api/admin/session")
async def get_admin_session(_: bool = Depends(require_admin)):
    try:
        admin_session = db.get_admin_session()
        return {"success": True, "admin_session": admin_session}
    except Exception as e:
        logger.error("Failed to get admin session", error=str(e))
        return {"success": False, "error": str(e), "admin_session": None}

@app.delete("/api/admin/session")
async def delete_admin_session(_: bool = Depends(require_admin)):
    try:
        success = await session_manager.remove_admin_session()
        return {"success": success}
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.get("/api/admin/groups")
async def get_admin_groups(_: bool = Depends(require_admin)):
    groups = session_manager.get_admin_groups()
    return {"success": True, "groups": groups}

@app.get("/api/admin/all-groups")
async def get_all_groups(_: bool = Depends(require_admin)):
    groups = await session_manager.list_all_groups()
    return {"success": True, "groups": groups}

@app.post("/api/admin/target-group")
async def set_target_group(group_id: int = Form(...), _: bool = Depends(require_admin)):
    session_manager.db.set_setting('target_group_id', group_id)
    return {"success": True}

@app.get("/api/admin/target-group")
async def get_target_group(_: bool = Depends(require_admin)):
    group_id = session_manager.db.get_setting('target_group_id')
    return {"success": True, "group_id": group_id}

@app.post("/api/admin/use-admin-as-user")
async def set_use_admin_as_user(enabled: bool = Form(...), _: bool = Depends(require_admin)):
    session_manager.db.set_setting('use_admin_as_user', enabled)
    return {"success": True}

@app.get("/api/admin/use-admin-as-user")
async def get_use_admin_as_user(_: bool = Depends(require_admin)):
    enabled = session_manager.db.get_setting('use_admin_as_user', False)
    return {"success": True, "enabled": enabled}

@app.post("/api/admin/use-user-as-admin")
async def set_use_user_as_admin(enabled: bool = Form(...), _: bool = Depends(require_admin)):
    session_manager.db.set_setting('use_user_as_admin', enabled)
    return {"success": True}

@app.get("/api/admin/use-user-as-admin")
async def get_use_user_as_admin(_: bool = Depends(require_admin)):
    enabled = session_manager.db.get_setting('use_user_as_admin', False)
    return {"success": True, "enabled": enabled}

@app.post("/api/admin/start-auto-add")
async def start_auto_add(
    target_group_id: int = Form(...),
    start_time: str = Form(...),
    _: bool = Depends(require_admin)
):
    """Start automated user addition process."""
    try:
        # Get current settings
        settings = admin_manager.get_settings()
        delay = settings.get('delay_between_adds', 30)
        batch_size = settings.get('batch_size', 5)
        max_daily = settings.get('max_users_per_session', 80)
        
        # Start the automation
        result = await session_manager.add_users_to_group(
            target_group_id, delay, batch_size, max_daily
        )
        return result
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.post("/api/admin/auto-add/start")
async def start_auto_add_supervisor(request: Request, _: bool = Depends(require_admin)):
    """Enable and immediately trigger the background auto add supervisor."""
    if request.session.get('user_role') != 'admin':
        return {"success": False, "error": "Admin role required"}
    admin_manager.update_settings(auto_add_enabled=True)
    # Clear last run time to force immediate execution
    session_manager.db.set_setting('auto_add_last_run', None)
    logger.info("Auto add supervisor start requested")
    await auto_add_supervisor.start()
    await auto_add_supervisor.wake_up(immediate=True)
    return {"success": True, "state": auto_add_supervisor.status()}


@app.post("/api/admin/auto-add/stop")
async def stop_auto_add_supervisor(request: Request, _: bool = Depends(require_admin)):
    """Disable scheduled runs. In-flight runs finish gracefully."""
    if request.session.get('user_role') != 'admin':
        return {"success": False, "error": "Admin role required"}
    admin_manager.update_settings(auto_add_enabled=False)
    logger.info("Auto add supervisor stop requested")
    await auto_add_supervisor.wake_up()
    return {"success": True, "state": auto_add_supervisor.status()}


@app.get("/api/admin/auto-add/status")
async def auto_add_status(_: bool = Depends(require_admin)):
    """Expose the current automation status for UI polling."""
    logger.debug("Auto add status queried")
    return {"success": True, "state": auto_add_supervisor.status()}

@app.post("/api/admin/ensure-sessions-in-group")
async def ensure_sessions_in_group(_: bool = Depends(require_admin)):
    """Ensure all user sessions are members of the target group."""
    try:
        result = await session_manager.ensure_all_sessions_in_target_group()
        return result
    except Exception as e:
        logger.error("Failed to ensure sessions in group", error=str(e))
        return {"success": False, "error": str(e)}

@app.post("/api/groups/update-counts")
async def update_group_counts(_: bool = Depends(require_admin)):
    try:
        # Check if target group is set
        target_group_id = session_manager.db.get_setting('target_group_id')
        if not target_group_id:
            return {"success": False, "error": "No target group set. Please select a target group first."}
        
        # First try to get admin session client
        client = await session_manager.get_admin_session_client()
        
        if not client:
            # Check if we have any user sessions and user-as-admin is enabled
            use_user_as_admin = session_manager.db.get_setting('use_user_as_admin', False)
            if use_user_as_admin and session_manager.sessions:
                # Use first available user session
                for session_name, user_client in session_manager.sessions.items():
                    try:
                        if user_client.is_connected() and await user_client.is_user_authorized():
                            client = user_client
                            logger.info(f"Using user session {session_name} as admin for group count update")
                            break
                    except Exception as e:
                        logger.warning(f"Failed to check user session {session_name}: {e}")
                        continue
        
        if not client:
            return {"success": False, "error": "No admin session or user sessions available for group count update"}
        
        count = await admin_manager.update_target_group_count(client)
        if count is not None:
            return {"success": True, "count": count}
        else:
            return {"success": False, "error": "Failed to update target group count"}
    except Exception as e:
        logger.error("Group count update failed", error=str(e))
        return {"success": False, "error": str(e)}

@app.post("/api/preferences")
async def save_preference(request: Request, _: bool = Depends(require_admin)):
    try:
        request_data = await request.json()
        key = request_data.get('key')
        value = request_data.get('value')
        
        if not key:
            return {"success": False, "error": "Key required"}
        
        db.set_user_preference(key, value)
        return {"success": True}
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.get("/api/preferences/{key}")
async def get_preference(key: str, _: bool = Depends(require_admin)):
    value = db.get_user_preference(key)
    return {"success": True, "value": value}

@app.get("/api/admin/debug-status")
async def debug_admin_status(_: bool = Depends(require_admin)):
    """Debug endpoint to check admin session status."""
    try:
        admin_session = session_manager.db.get_admin_session()
        use_user_as_admin = session_manager.db.get_setting('use_user_as_admin', False)
        use_admin_as_user = session_manager.db.get_setting('use_admin_as_user', False)
        
        # Check admin client
        admin_client = await session_manager.get_admin_session_client()
        
        # Check user sessions
        user_sessions = list(session_manager.sessions.keys())
        active_user_sessions = []
        for name, client in session_manager.sessions.items():
            try:
                if client.is_connected() and await client.is_user_authorized():
                    active_user_sessions.append(name)
            except:
                pass
        
        return {
            "success": True,
            "debug_info": {
                "admin_session_in_db": admin_session is not None,
                "admin_session_name": admin_session['session_name'] if admin_session else None,
                "admin_client_available": admin_client is not None,
                "use_user_as_admin": use_user_as_admin,
                "use_admin_as_user": use_admin_as_user,
                "total_user_sessions": len(user_sessions),
                "active_user_sessions": len(active_user_sessions),
                "user_session_names": user_sessions,
                "active_user_session_names": active_user_sessions
            }
        }
    except Exception as e:
        return {"success": False, "error": str(e)}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=5002, log_level="info")