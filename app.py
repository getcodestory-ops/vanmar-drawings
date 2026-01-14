import os
import uuid
import time
import asyncio
import json
import base64
from datetime import datetime
from typing import List, Optional, Dict, Any
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, BackgroundTasks, Depends, Cookie, HTTPException, status
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, validator
from sqlalchemy import create_engine, Column, String, DateTime, Integer, Text, Boolean, text
from sqlalchemy.orm import sessionmaker, declarative_base, Session
from cryptography.fernet import Fernet
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
import requests
import aiohttp

from apscheduler.schedulers.background import BackgroundScheduler
import pytz

from config import settings
from logging_config import setup_logging, get_logger
import core_engine

# Setup logging
setup_logging(settings.ENVIRONMENT, "INFO" if settings.is_production else "DEBUG")
logger = get_logger(__name__)

# Database setup
# For PostgreSQL, add SSL mode if not specified in URL
connect_args = {}
if settings.DATABASE_URL.startswith("sqlite"):
    connect_args["check_same_thread"] = False
elif settings.DATABASE_URL.startswith("postgresql"):
    # Render.com PostgreSQL requires SSL
    if "sslmode" not in settings.DATABASE_URL:
        connect_args["sslmode"] = "require"

engine = create_engine(
    settings.DATABASE_URL,
    connect_args=connect_args,
    pool_pre_ping=True,
    pool_recycle=3600,
    echo=False  # Set to True for SQL query logging (debugging)
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Encryption setup
cipher_suite = Fernet(settings.get_encryption_key())

# Rate limiting
limiter = Limiter(key_func=get_remote_address)

# Job cancellation tracking
# Maps job_id -> cancellation event
job_cancellation_events: Dict[str, asyncio.Event] = {}


# ---------------------------------------------------------
# DATABASE MODELS
# ---------------------------------------------------------

class Job(Base):
    __tablename__ = "jobs"
    id = Column(String, primary_key=True, index=True)
    status = Column(String)  # queued, processing, uploading, completed, failed, cancelled
    project_id = Column(String)
    project_name = Column(String)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    result_message = Column(String, nullable=True)
    progress = Column(Integer, default=0)
    error_details = Column(Text, nullable=True)
    total_drawings = Column(Integer, nullable=True)  # Total drawings to process
    processed_drawings = Column(Integer, default=0)  # Drawings processed so far


class TokenStore(Base):
    __tablename__ = "token_store"
    id = Column(Integer, primary_key=True, index=True)
    refresh_token_encrypted = Column(Text)  # Encrypted
    updated_at = Column(DateTime, default=datetime.utcnow)


class ScheduledJob(Base):
    __tablename__ = "scheduled_jobs"
    id = Column(String, primary_key=True, index=True)
    project_id = Column(String)
    project_name = Column(String)
    disciplines_json = Column(String)
    day_of_week = Column(String)
    hour = Column(Integer)
    minute = Column(Integer)
    timezone = Column(String)
    active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)


# Tables will be created on startup via lifespan function
# This prevents module-level errors if database isn't ready
try:
    Base.metadata.create_all(bind=engine)
except Exception as e:
    logger.warning(f"Could not create tables at import time (will retry on startup): {e}")


# ---------------------------------------------------------
# PYDANTIC MODELS (Request/Response validation)
# ---------------------------------------------------------

class SingleJobConfig(BaseModel):
    project_id: int
    project_name: str
    disciplines: List[str]
    drawing_ids: Optional[List[int]] = None  # Optional: specific drawing IDs to process

    @validator('project_id')
    def validate_project_id(cls, v):
        if v <= 0:
            raise ValueError('project_id must be positive')
        return v

    @validator('disciplines')
    def validate_disciplines(cls, v):
        if not v:
            raise ValueError('at least one discipline is required')
        return v


class BatchRequest(BaseModel):
    queue: List[SingleJobConfig]

    @validator('queue')
    def validate_queue(cls, v):
        if not v:
            raise ValueError('queue cannot be empty')
        if len(v) > 10:
            raise ValueError('queue cannot exceed 10 projects')
        return v


class ScheduleRequest(BaseModel):
    queue: List[SingleJobConfig]
    day: str
    time: str
    timezone: str

    @validator('day')
    def validate_day(cls, v):
        valid_days = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
        if v not in valid_days:
            raise ValueError(f'day must be one of {valid_days}')
        return v

    @validator('time')
    def validate_time(cls, v):
        try:
            h, m = map(int, v.split(':'))
            if not (0 <= h < 24 and 0 <= m < 60):
                raise ValueError
        except:
            raise ValueError('time must be in HH:MM format')
        return v


class HealthResponse(BaseModel):
    status: str
    database: str
    disk_space_mb: int
    environment: str
    version: str = "1.0.0"
    redirect_uri: Optional[str] = None  # Show redirect URI for OAuth debugging


# ---------------------------------------------------------
# LIFESPAN CONTEXT (Startup/Shutdown)
# ---------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage app lifecycle."""
    logger.info(f"Starting Procore PDF Merger - Environment: {settings.ENVIRONMENT}")
    logger.info(f"Database URL: {settings.DATABASE_URL[:20]}..." if settings.DATABASE_URL else "DATABASE_URL not set")
    
    # Ensure database tables exist
    try:
        Base.metadata.create_all(bind=engine)
        logger.info("Database tables verified/created successfully")
        
        # Migrate existing database: Add new columns if they don't exist
        with engine.begin() as conn:  # Use begin() for automatic transaction handling
            # Check if we're using SQLite
            if settings.DATABASE_URL.startswith("sqlite"):
                # Check if columns exist and add them if missing
                try:
                    # Try to query the columns - if they don't exist, this will fail
                    conn.execute(text("SELECT total_drawings, processed_drawings FROM jobs LIMIT 1"))
                    logger.info("Database columns already exist")
                except Exception:
                    # Columns don't exist, add them
                    logger.info("Adding new columns to jobs table...")
                    try:
                        conn.execute(text("ALTER TABLE jobs ADD COLUMN total_drawings INTEGER"))
                        logger.info("Added total_drawings column")
                    except Exception as e:
                        logger.warning(f"Could not add total_drawings column (may already exist): {e}")
                    
                    try:
                        conn.execute(text("ALTER TABLE jobs ADD COLUMN processed_drawings INTEGER DEFAULT 0"))
                        logger.info("Added processed_drawings column")
                    except Exception as e:
                        logger.warning(f"Could not add processed_drawings column (may already exist): {e}")
                    
                    logger.info("Database migration completed successfully")
            else:
                # For PostgreSQL, use IF NOT EXISTS equivalent
                try:
                    conn.execute(text("""
                        DO $$ 
                        BEGIN
                            IF NOT EXISTS (
                                SELECT 1 FROM information_schema.columns 
                                WHERE table_name='jobs' AND column_name='total_drawings'
                            ) THEN
                                ALTER TABLE jobs ADD COLUMN total_drawings INTEGER;
                            END IF;
                            
                            IF NOT EXISTS (
                                SELECT 1 FROM information_schema.columns 
                                WHERE table_name='jobs' AND column_name='processed_drawings'
                            ) THEN
                                ALTER TABLE jobs ADD COLUMN processed_drawings INTEGER DEFAULT 0;
                            END IF;
                        END $$;
                    """))
                    logger.info("Database migration completed successfully (PostgreSQL)")
                except Exception as e:
                    logger.warning(f"Database migration check failed (columns may already exist): {e}")
        
        # Test database connection
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
            logger.info("Database connection test successful")
    except Exception as e:
        logger.error(f"Database initialization failed: {e}", exc_info=True)
        # Don't crash the app, but log the error clearly
        logger.warning("App will continue but database operations may fail")
    
    # Start scheduler
    scheduler.start()
    logger.info("Background scheduler started")
    
    # Create output directory
    os.makedirs("output", exist_ok=True)
    
    yield
    
    # Shutdown
    scheduler.shutdown()
    logger.info("Application shutting down")


# ---------------------------------------------------------
# FASTAPI APP
# ---------------------------------------------------------

app = FastAPI(
    title="Procore PDF Merger",
    description="Automated PDF merging and archiving for Procore",
    version="1.0.0",
    lifespan=lifespan
)

# Add rate limiter
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# Mount static files and templates
templates = Jinja2Templates(directory="templates")
if not os.path.exists("output"):
    os.makedirs("output")
if not os.path.exists("static"):
    os.makedirs("static")
app.mount("/static", StaticFiles(directory="static"), name="static")
app.mount("/output", StaticFiles(directory="output"), name="output")


# ---------------------------------------------------------
# DEPENDENCIES
# ---------------------------------------------------------

def get_db():
    """Database session dependency."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def encrypt_token(token: str) -> str:
    """Encrypt a token."""
    return cipher_suite.encrypt(token.encode()).decode()


def decrypt_token(encrypted_token: str) -> str:
    """Decrypt a token."""
    return cipher_suite.decrypt(encrypted_token.encode()).decode()


# ---------------------------------------------------------
# AUTH FUNCTIONS
# ---------------------------------------------------------

def get_fresh_access_token() -> Optional[str]:
    """Get a fresh access token using refresh token."""
    db = SessionLocal()
    try:
        token_entry = db.query(TokenStore).first()
        if not token_entry or not token_entry.refresh_token_encrypted:
            logger.error("No refresh token found in database")
            return None

        refresh_token = decrypt_token(token_entry.refresh_token_encrypted)
        
        payload = {
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": settings.PROCORE_CLIENT_ID,
            "client_secret": settings.PROCORE_CLIENT_SECRET,
            "redirect_uri": settings.redirect_uri
        }
        
        try:
            r = requests.post(settings.token_url, data=payload, timeout=30)
            r.raise_for_status()
            tokens = r.json()
            
            # Update refresh token
            new_refresh = tokens['refresh_token']
            token_entry.refresh_token_encrypted = encrypt_token(new_refresh)
            token_entry.updated_at = datetime.utcnow()
            db.commit()
            
            logger.info("Successfully refreshed access token")
            return tokens['access_token']
        except requests.exceptions.RequestException as e:
            logger.error(f"Token refresh failed: {e}")
            return None
    finally:
        db.close()


# ---------------------------------------------------------
# WORKER FUNCTIONS
# ---------------------------------------------------------

def process_batch_sequence(job_list: List[dict], access_token: str):
    """Process a batch of jobs sequentially."""
    batch_start_time = time.time()
    db = SessionLocal()
    
    logger.info(f"═══════════════════════════════════════════════════════════")
    logger.info(f"📋 STARTING BATCH PROCESSING")
    logger.info(f"   Total jobs: {len(job_list)}")
    logger.info(f"   Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"═══════════════════════════════════════════════════════════")
    
    for job_idx, job_meta in enumerate(job_list, start=1):
        job_id = job_meta["id"]
        job_start_time = time.time()
        
        # Create cancellation event for this job
        cancel_event = asyncio.Event()
        job_cancellation_events[job_id] = cancel_event
        
        job_record = db.query(Job).filter(Job.id == job_id).first()
        if not job_record:
            logger.warning(f"[{job_idx}/{len(job_list)}] Job {job_id} not found in database, skipping")
            if job_id in job_cancellation_events:
                del job_cancellation_events[job_id]
            continue
        
        # Check if job was cancelled before starting
        if job_record.status == "cancelled":
            logger.info(f"[{job_idx}/{len(job_list)}] Job {job_id} was cancelled, skipping")
            if job_id in job_cancellation_events:
                del job_cancellation_events[job_id]
            continue
        
        job_record.status = "processing"
        job_record.progress = 0
        job_record.updated_at = datetime.utcnow()
        db.commit()
        
        logger.info(f"═══════════════════════════════════════════════════════════")
        logger.info(f"[JOB {job_idx}/{len(job_list)}] Starting job {job_id}")
        logger.info(f"   Project ID: {job_meta['project_id']}")
        logger.info(f"   Project Name: {job_meta.get('project_name', 'Unknown')}")
        logger.info(f"   Disciplines: {', '.join(job_meta.get('disciplines', []))}")
        if job_meta.get('drawing_ids'):
            logger.info(f"   Selected Drawings: {len(job_meta['drawing_ids'])} specific drawing(s)")
        logger.info(f"   Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"═══════════════════════════════════════════════════════════")
        
        # Define progress callback
        def update_db_progress(percent, processed=0, total=0):
            try:
                sub_db = SessionLocal()
                jr = sub_db.query(Job).filter(Job.id == job_id).first()
                if jr and jr.status != "cancelled":
                    jr.progress = percent
                    if total > 0:
                        jr.total_drawings = total
                    if processed >= 0:
                        jr.processed_drawings = processed
                    jr.updated_at = datetime.utcnow()
                    sub_db.commit()
                    logger.debug(f"   Progress updated: {percent}% ({processed}/{total} drawings)")
                sub_db.close()
            except Exception as e:
                logger.warning(f"   Could not update progress: {e}")

        try:
            max_retries = 3
            retry_count = 0
            local_path = None
            
            while retry_count < max_retries:
                try:
                    # Check for cancellation before starting PDF generation
                    if cancel_event.is_set() or job_record.status == "cancelled":
                        raise Exception("Job was cancelled")
                    
                    # Run PDF generation
                    if retry_count > 0:
                        logger.info(f"   [RETRY {retry_count}/{max_retries-1}] Regenerating PDF...")
                    else:
                        logger.info(f"   [PHASE 1/2] Generating PDF...")
                    
                    pdf_start = time.time()
                    local_path = core_engine.run_job_api(
                        job_meta["project_id"],
                        job_meta["disciplines"],
                        access_token,
                        progress_callback=update_db_progress,
                        drawing_ids=job_meta.get("drawing_ids")
                    )
                    pdf_time = time.time() - pdf_start
                    
                    # Check for cancellation after PDF generation
                    if cancel_event.is_set():
                        raise Exception("Job was cancelled during PDF generation")
                    
                    logger.info(f"   ✓ PDF generation complete (took {pdf_time:.2f}s)")
                    break  # Success, exit retry loop
                    
                except Exception as pdf_error:
                    retry_count += 1
                    error_msg = str(pdf_error)
                    
                    # Check if it's a validation error (page count mismatch)
                    is_validation_error = "PDF validation failed" in error_msg or ("Expected" in error_msg and "pages" in error_msg)
                    
                    if is_validation_error and retry_count < max_retries:
                        logger.warning(f"   ⚠ PDF validation failed (attempt {retry_count}/{max_retries}): {error_msg}")
                        logger.info(f"   🔄 Restarting process...")
                        # Clean up any partial files
                        if local_path and os.path.exists(local_path):
                            try:
                                os.remove(local_path)
                                logger.debug(f"   Removed invalid PDF file")
                            except:
                                pass
                        # Wait a bit before retrying
                        time.sleep(2)
                        continue  # Retry
                    else:
                        # Not a validation error, or max retries reached
                        raise  # Re-raise the exception
            
            if not local_path:
                raise Exception(f"PDF generation failed after {max_retries} attempts")
            
            # Upload phase
            logger.info(f"   [PHASE 2/2] Uploading to Procore...")
            update_db_progress(95, job_record.total_drawings or 0, job_record.total_drawings or 0)
            
            # Check for cancellation before upload
            if cancel_event.is_set() or job_record.status == "cancelled":
                raise Exception("Job was cancelled before upload")
            
            job_record.status = "uploading"
            job_record.updated_at = datetime.utcnow()
            db.commit()
            
            upload_start = time.time()
            # Upload to Procore
            upload_result = asyncio.run(
                core_engine.handle_procore_upload(
                    job_meta["project_id"],
                    local_path,
                    access_token
                )
            )
            upload_time = time.time() - upload_start
            
            # Final cancellation check
            if cancel_event.is_set():
                raise Exception("Job was cancelled during upload")
            
            job_record.status = "completed"
            job_record.progress = 100
            job_record.updated_at = datetime.utcnow()
            job_record.result_message = os.path.join("output", os.path.basename(local_path))
            
            if upload_result != "Success":
                job_record.result_message += f"||{upload_result}"
                logger.warning(f"   ⚠ Upload result: {upload_result}")
            
            job_time = time.time() - job_start_time
            logger.info(f"   ✓ Upload complete (took {upload_time:.2f}s)")
            logger.info(f"═══════════════════════════════════════════════════════════")
            logger.info(f"✅ JOB {job_idx}/{len(job_list)} COMPLETED")
            logger.info(f"   Job ID: {job_id}")
            logger.info(f"   Total time: {job_time:.2f}s ({job_time/60:.1f} minutes)")
            logger.info(f"   Output: {os.path.basename(local_path)}")
            logger.info(f"═══════════════════════════════════════════════════════════")

        except Exception as e:
            job_time = time.time() - job_start_time
            
            # Check if it was a cancellation
            if cancel_event.is_set() or job_record.status == "cancelled":
                job_record.status = "cancelled"
                job_record.error_details = "Job was cancelled by user"
                logger.info(f"═══════════════════════════════════════════════════════════")
                logger.info(f"🚫 JOB {job_idx}/{len(job_list)} CANCELLED after {job_time:.2f}s")
                logger.info(f"   Job ID: {job_id}")
                logger.info(f"═══════════════════════════════════════════════════════════")
            else:
                job_record.status = "failed"
                job_record.error_details = str(e)
                logger.error(f"═══════════════════════════════════════════════════════════")
                logger.error(f"❌ JOB {job_idx}/{len(job_list)} FAILED after {job_time:.2f}s")
                logger.error(f"   Job ID: {job_id}")
                logger.error(f"   Project ID: {job_meta['project_id']}")
                logger.error(f"   Error: {type(e).__name__}: {str(e)}")
                logger.error(f"═══════════════════════════════════════════════════════════", exc_info=True)
        
        db.commit()
        
        # Clean up cancellation event
        if job_id in job_cancellation_events:
            del job_cancellation_events[job_id]
        
        # Rate limiting between jobs
        if job_idx < len(job_list):
            logger.info(f"   Waiting 5s before next job...")
        time.sleep(5)

    batch_time = time.time() - batch_start_time
    logger.info(f"═══════════════════════════════════════════════════════════")
    logger.info(f"📋 BATCH PROCESSING COMPLETE")
    logger.info(f"   Processed: {len(job_list)} job(s)")
    logger.info(f"   Total time: {batch_time:.2f}s ({batch_time/60:.1f} minutes)")
    logger.info(f"   Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"═══════════════════════════════════════════════════════════")

    db.close()


# ---------------------------------------------------------
# SCHEDULER
# ---------------------------------------------------------

def scheduler_tick():
    """Check and run scheduled jobs."""
    db = SessionLocal()
    try:
        active_jobs = db.query(ScheduledJob).filter(ScheduledJob.active == True).all()
        
        if active_jobs:
            logger.debug(f"Scheduler tick: Checking {len(active_jobs)} active schedule(s)")
        
        jobs_to_run = []
        
        for s_job in active_jobs:
            try:
                user_tz = pytz.timezone(s_job.timezone)
                now_in_user_tz = datetime.now(user_tz)
                current_day = now_in_user_tz.strftime("%a")
                
                logger.debug(f"  Schedule '{s_job.project_name}': "
                           f"Now={current_day} {now_in_user_tz.hour:02d}:{now_in_user_tz.minute:02d}, "
                           f"Target={s_job.day_of_week} {s_job.hour:02d}:{s_job.minute:02d}")
                
                if (current_day == s_job.day_of_week and
                    now_in_user_tz.hour == s_job.hour and
                    now_in_user_tz.minute == s_job.minute):
                    
                    logger.info(f"✅ Triggering scheduled job: {s_job.project_name} (Timezone: {s_job.timezone})")
                    jobs_to_run.append(s_job)
                    
            except Exception as e:
                logger.error(f"Error checking schedule for {s_job.project_name}: {e}")

        if not jobs_to_run:
            return

        # Get fresh access token
        access_token = get_fresh_access_token()
        if not access_token:
            logger.error("Could not get access token for scheduled jobs")
            return

        # Create job entries
        batch_jobs = []
        for s_job in jobs_to_run:
            job_id = str(uuid.uuid4())
            new_job = Job(
                id=job_id,
                status="queued",
                project_id=s_job.project_id,
                project_name=f"[AUTO] {s_job.project_name}"
            )
            db.add(new_job)
            discs = json.loads(s_job.disciplines_json)
            batch_jobs.append({
                "id": job_id,
                "project_id": int(s_job.project_id),
                "disciplines": discs
            })

        db.commit()
        
        # Run jobs
        process_batch_sequence(batch_jobs, access_token)
        
    except Exception as e:
        logger.error(f"Scheduler tick error: {e}", exc_info=True)
    finally:
        db.close()


# Initialize scheduler
scheduler = BackgroundScheduler()
scheduler.add_job(scheduler_tick, 'interval', seconds=60)


# ---------------------------------------------------------
# ROUTES
# ---------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
async def home(request: Request, access_token: str = Cookie(None)):
    """Home page - dashboard or login."""
    if not access_token:
        return templates.TemplateResponse("login.html", {"request": request})
    return templates.TemplateResponse("dashboard.html", {"request": request})


@app.get("/login")
async def login():
    """Redirect to Procore OAuth login."""
    from urllib.parse import quote_plus
    # URL encode the redirect_uri to ensure proper encoding
    redirect_uri_encoded = quote_plus(settings.redirect_uri)
    url = f"{settings.login_url}?response_type=code&client_id={settings.PROCORE_CLIENT_ID}&redirect_uri={redirect_uri_encoded}"
    logger.info(f"Redirecting to Procore OAuth. Redirect URI (raw): {settings.redirect_uri}")
    return RedirectResponse(url)


@app.get("/callback")
async def callback(request: Request, code: Optional[str] = None, error: Optional[str] = None, db: Session = Depends(get_db)):
    """OAuth callback handler."""
    # Check for OAuth errors from Procore
    if error:
        error_desc = request.query_params.get("error_description", error)
        logger.error(f"OAuth error from Procore: {error} - {error_desc}")
        return HTMLResponse(
            f"""
            <html>
                <body style="font-family: Arial; padding: 40px; max-width: 600px; margin: 0 auto;">
                    <h1>❌ Login Failed</h1>
                    <p><strong>Error:</strong> {error}</p>
                    <p><strong>Description:</strong> {error_desc}</p>
                    <h3>Common Issues:</h3>
                    <ul>
                        <li>Redirect URI mismatch - Check your Procore Developer Portal</li>
                        <li>Invalid client ID or secret - Verify your .env file</li>
                        <li>App not authorized - Try authorizing again</li>
                    </ul>
                    <p><a href="/login">Try Again</a> | <a href="/">Home</a></p>
                </body>
            </html>
            """,
            status_code=400
        )
    
    # Check if authorization code is present
    if not code:
        logger.error("OAuth callback missing authorization code")
        return HTMLResponse(
            """
            <html>
                <body style="font-family: Arial; padding: 40px; max-width: 600px; margin: 0 auto;">
                    <h1>❌ Login Failed</h1>
                    <p>No authorization code received from Procore.</p>
                    <p>This usually means:</p>
                    <ul>
                        <li>The OAuth flow was cancelled</li>
                        <li>There's a redirect URI mismatch</li>
                    </ul>
                    <p><a href="/login">Try Again</a> | <a href="/">Home</a></p>
                </body>
            </html>
            """,
            status_code=400
        )
    
    # Validate configuration
    if not settings.PROCORE_CLIENT_ID or not settings.PROCORE_CLIENT_SECRET:
        logger.error("Missing Procore credentials in configuration")
        return HTMLResponse(
            """
            <html>
                <body style="font-family: Arial; padding: 40px; max-width: 600px; margin: 0 auto;">
                    <h1>❌ Configuration Error</h1>
                    <p>PROCORE_CLIENT_ID or PROCORE_CLIENT_SECRET not set in environment.</p>
                    <p>Please check your .env file or environment variables.</p>
                    <p><a href="/">Home</a></p>
                </body>
            </html>
            """,
            status_code=500
        )
    
    payload = {
        "grant_type": "authorization_code",
        "code": code,
        "client_id": settings.PROCORE_CLIENT_ID,
        "client_secret": settings.PROCORE_CLIENT_SECRET,
        "redirect_uri": settings.redirect_uri
    }
    
    try:
        logger.info(f"Exchanging code for token. Redirect URI: {settings.redirect_uri}")
        r = requests.post(settings.token_url, data=payload, timeout=30)
        
        # Check for API errors
        if r.status_code != 200:
            error_text = r.text
            logger.error(f"Procore API error {r.status_code}: {error_text}")
            
            # Parse error response
            try:
                error_data = r.json()
                error_msg = error_data.get("error_description", error_data.get("error", error_text))
            except:
                error_msg = error_text
            
            return HTMLResponse(
                f"""
                <html>
                    <body style="font-family: Arial; padding: 40px; max-width: 600px; margin: 0 auto;">
                        <h1>❌ Login Failed</h1>
                        <p><strong>HTTP {r.status_code} Error:</strong></p>
                        <pre style="background: #f5f5f5; padding: 15px; border-radius: 4px;">{error_msg}</pre>
                        <h3>Troubleshooting:</h3>
                        <ul>
                            <li><strong>Invalid redirect_uri:</strong> Make sure your Procore app has <code>{settings.redirect_uri}</code> configured</li>
                            <li><strong>Invalid client credentials:</strong> Check PROCORE_CLIENT_ID and PROCORE_CLIENT_SECRET in your .env file</li>
                            <li><strong>Code expired:</strong> Authorization codes expire quickly, try again</li>
                        </ul>
                        <p>Check your server logs for more details.</p>
                        <p><a href="/login">Try Again</a> | <a href="/debug/config">Check Configuration</a></p>
                    </body>
                </html>
                """,
                status_code=r.status_code
            )
        
        tokens = r.json()
        
        # Validate SECRET_KEY before encrypting
        try:
            settings.get_encryption_key()
        except ValueError as e:
            logger.error(f"SECRET_KEY not configured: {e}")
            return HTMLResponse(
                f"""
                <html>
                    <body style="font-family: Arial; padding: 40px; max-width: 600px; margin: 0 auto;">
                        <h1>❌ Configuration Error</h1>
                        <p>SECRET_KEY is not set. This is required for token encryption.</p>
                        <p>Add SECRET_KEY to your .env file and restart the server.</p>
                        <p><strong>Error:</strong> {str(e)}</p>
                        <p><a href="/">Home</a></p>
                    </body>
                </html>
                """,
                status_code=500
            )
        
        # Store encrypted refresh token
        ref = tokens.get("refresh_token")
        if ref:
            try:
                encrypted_ref = encrypt_token(ref)
                existing = db.query(TokenStore).first()
                if existing:
                    existing.refresh_token_encrypted = encrypted_ref
                    existing.updated_at = datetime.utcnow()
                else:
                    db.add(TokenStore(refresh_token_encrypted=encrypted_ref))
                db.commit()
                logger.info("OAuth tokens stored successfully")
            except Exception as enc_error:
                logger.error(f"Failed to encrypt/store token: {enc_error}", exc_info=True)
                return HTMLResponse(
                    f"""
                    <html>
                        <body style="font-family: Arial; padding: 40px; max-width: 600px; margin: 0 auto;">
                            <h1>❌ Token Storage Failed</h1>
                            <p>Failed to store authentication token.</p>
                            <p><strong>Error:</strong> {str(enc_error)}</p>
                            <p>Check server logs for details.</p>
                            <p><a href="/login">Try Again</a></p>
                        </body>
                    </html>
                    """,
                    status_code=500
                )

        response = RedirectResponse(url="/")
        response.set_cookie(
            key="access_token",
            value=tokens["access_token"],
            httponly=True,
            secure=settings.is_production,
            samesite="lax",
            max_age=3600  # 1 hour
        )
        return response
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Network error during OAuth callback: {e}", exc_info=True)
        return HTMLResponse(
            f"""
            <html>
                <body style="font-family: Arial; padding: 40px; max-width: 600px; margin: 0 auto;">
                    <h1>❌ Network Error</h1>
                    <p>Could not connect to Procore API.</p>
                    <p><strong>Error:</strong> {str(e)}</p>
                    <p>Please check your internet connection and try again.</p>
                    <p><a href="/login">Try Again</a></p>
                </body>
            </html>
            """,
            status_code=500
        )
    except Exception as e:
        logger.error(f"Unexpected error in OAuth callback: {e}", exc_info=True)
        return HTMLResponse(
            f"""
            <html>
                <body style="font-family: Arial; padding: 40px; max-width: 600px; margin: 0 auto;">
                    <h1>❌ Login Failed</h1>
                    <p>An unexpected error occurred.</p>
                    <p><strong>Error:</strong> {str(e)}</p>
                    <p>Check server logs for more details.</p>
                    <p><a href="/login">Try Again</a> | <a href="/debug/config">Check Configuration</a></p>
                </body>
            </html>
            """,
            status_code=500
        )


@app.get("/api/projects")
@limiter.limit("30/minute")
async def get_projects(request: Request, access_token: str = Cookie(None)):
    """Get list of Procore projects."""
    if not access_token:
        raise HTTPException(status_code=401, detail="Not authenticated")
    
    try:
        # Use async version directly instead of sync wrapper
        from core_engine import AsyncProcoreClient
        from config import settings
        
        client = AsyncProcoreClient(access_token, settings.PROCORE_COMPANY_ID)
        try:
            data = await client.get("/rest/v1.0/projects", params={"company_id": settings.PROCORE_COMPANY_ID})
            projects = data if isinstance(data, list) else []
            projects.sort(key=lambda x: x.get('name', ''))
            return projects
        except aiohttp.ClientResponseError as api_error:
            logger.error(f"Procore API HTTP error: {api_error.status} - {api_error.message}")
            if api_error.status == 401:
                raise HTTPException(
                    status_code=401, 
                    detail="Token expired or invalid. Please login again."
                )
            elif api_error.status == 403:
                raise HTTPException(
                    status_code=403,
                    detail=f"Access forbidden: {api_error.message}"
                )
            else:
                raise HTTPException(
                    status_code=api_error.status,
                    detail=f"Procore API error: {api_error.message}"
                )
        except Exception as api_error:
            logger.error(f"Procore API error: {api_error}", exc_info=True)
            # Re-raise to be caught by outer handler
            raise
        finally:
            await client.close()
            
    except HTTPException:
        # Re-raise HTTP exceptions
        raise
    except Exception as e:
        logger.error(f"Error fetching projects: {e}", exc_info=True)
        error_msg = str(e)
        # Provide more helpful error messages
        if "401" in error_msg or "Unauthorized" in error_msg:
            raise HTTPException(
                status_code=401, 
                detail="Authentication failed. Token may be expired. Please login again."
            )
        elif "403" in error_msg or "Forbidden" in error_msg:
            raise HTTPException(
                status_code=403,
                detail="Access forbidden. Check your Procore permissions."
            )
        else:
            raise HTTPException(
                status_code=500, 
                detail=f"Failed to fetch projects: {error_msg}"
            )


@app.get("/api/disciplines")
@limiter.limit("30/minute")
async def get_disciplines(request: Request, project_id: int, access_token: str = Cookie(None)):
    """Get disciplines for a project."""
    if not access_token:
        raise HTTPException(status_code=401, detail="Not authenticated")
    
    try:
        # Use async version directly
        from core_engine import AsyncProcoreClient
        from config import settings
        
        client = AsyncProcoreClient(access_token, settings.PROCORE_COMPANY_ID)
        try:
            r_area = await client.get(f"/rest/v1.1/projects/{project_id}/drawing_areas")
            area_id = None
            for a in r_area:
                if a['name'].strip().upper() == "IFC":
                    area_id = a['id']
                    break
            
            if not area_id:
                return {"error": "No 'IFC' Drawing Area found."}

            endpoint = f"/rest/v1.1/drawing_areas/{area_id}/drawings"
            all_drawings = []
            page = 1
            
            while True:
                data = await client.get(endpoint, params={"project_id": project_id, "page": page, "per_page": 100})
                if not data:
                    break
                all_drawings.extend(data)
                if len(data) < 100:
                    break
                page += 1

            # Count drawings per discipline
            discipline_counts = {}
            for dwg in all_drawings:
                d_name = "Unknown"
                if dwg.get("drawing_discipline"):
                    d_name = dwg["drawing_discipline"].get("name", "Unknown")
                elif dwg.get("discipline"):
                    d_name = dwg["discipline"]
                discipline_counts[d_name] = discipline_counts.get(d_name, 0) + 1
            
            # Format disciplines with counts
            disciplines_with_counts = [
                {
                    "name": disc,
                    "count": discipline_counts[disc]
                }
                for disc in sorted(discipline_counts.keys())
            ]
            
            return {"area_id": area_id, "disciplines": disciplines_with_counts}
        except Exception as api_error:
            logger.error(f"Procore API error fetching disciplines: {api_error}", exc_info=True)
            if hasattr(api_error, 'status') and api_error.status == 401:
                raise HTTPException(status_code=401, detail="Token expired or invalid. Please login again.")
            raise
        finally:
            await client.close()
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching disciplines: {e}", exc_info=True)
        error_msg = str(e)
        if "401" in error_msg or "Unauthorized" in error_msg:
            raise HTTPException(
                status_code=401,
                detail="Authentication failed. Please login again."
            )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch disciplines: {error_msg}"
        )


@app.get("/api/drawings")
@limiter.limit("30/minute")
async def get_drawings(request: Request, project_id: int, discipline: str, access_token: str = Cookie(None)):
    """Get drawings for a specific discipline."""
    if not access_token:
        raise HTTPException(status_code=401, detail="Not authenticated")
    
    try:
        from core_engine import AsyncProcoreClient
        from config import settings
        
        client = AsyncProcoreClient(access_token, settings.PROCORE_COMPANY_ID)
        try:
            r_area = await client.get(f"/rest/v1.1/projects/{project_id}/drawing_areas")
            area_id = None
            for a in r_area:
                if a['name'].strip().upper() == "IFC":
                    area_id = a['id']
                    break
            
            if not area_id:
                return {"error": "No 'IFC' Drawing Area found."}

            endpoint = f"/rest/v1.1/drawing_areas/{area_id}/drawings"
            all_drawings = []
            page = 1
            
            while True:
                data = await client.get(endpoint, params={"project_id": project_id, "page": page, "per_page": 100})
                if not data:
                    break
                all_drawings.extend(data)
                if len(data) < 100:
                    break
                page += 1

            # Filter drawings by discipline
            filtered_drawings = []
            for dwg in all_drawings:
                d_name = "Unknown"
                if dwg.get("drawing_discipline"):
                    d_name = dwg["drawing_discipline"].get("name", "Unknown")
                elif dwg.get("discipline"):
                    d_name = dwg["discipline"]
                
                if d_name == discipline:
                    filtered_drawings.append({
                        "id": dwg.get("id"),
                        "number": dwg.get("number", "Unknown"),
                        "title": dwg.get("title", ""),
                        "revision": dwg.get("current_revision", {}).get("revision_number", "")
                    })
            
            return {"drawings": filtered_drawings}
        except Exception as api_error:
            logger.error(f"Procore API error fetching drawings: {api_error}", exc_info=True)
            if hasattr(api_error, 'status') and api_error.status == 401:
                raise HTTPException(status_code=401, detail="Token expired or invalid. Please login again.")
            raise
        finally:
            await client.close()
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching drawings: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to fetch drawings: {str(e)}")


@app.get("/api/recent_jobs")
async def get_recent_jobs(
    page: int = 1,
    per_page: int = 6,
    db: Session = Depends(get_db)
):
    """Get recent job history with pagination."""
    # Calculate offset
    offset = (page - 1) * per_page
    
    # Get total count
    total_count = db.query(Job).count()
    
    # Get paginated jobs
    jobs = db.query(Job).order_by(Job.created_at.desc()).offset(offset).limit(per_page).all()
    
    # Calculate total pages
    total_pages = (total_count + per_page - 1) // per_page if total_count > 0 else 1
    
    return {
        "jobs": jobs,
        "pagination": {
            "page": page,
            "per_page": per_page,
            "total": total_count,
            "total_pages": total_pages,
            "has_next": page < total_pages,
            "has_prev": page > 1
        }
    }


@app.post("/api/cancel_job/{job_id}")
@limiter.limit("10/minute")
async def cancel_job(
    request: Request,
    job_id: str,
    db: Session = Depends(get_db)
):
    """Cancel a queued or processing job."""
    job_record = db.query(Job).filter(Job.id == job_id).first()
    
    if not job_record:
        raise HTTPException(status_code=404, detail="Job not found")
    
    # Only allow cancelling queued or processing jobs
    if job_record.status not in ["queued", "processing", "uploading"]:
        raise HTTPException(
            status_code=400, 
            detail=f"Cannot cancel job with status '{job_record.status}'. Only queued, processing, or uploading jobs can be cancelled."
        )
    
    # Set cancellation flag
    job_record.status = "cancelled"
    job_record.error_details = "Job was cancelled by user"
    job_record.updated_at = datetime.utcnow()
    db.commit()
    
    # Signal cancellation event if job is running
    if job_id in job_cancellation_events:
        job_cancellation_events[job_id].set()
        logger.info(f"Job {job_id} cancellation signal sent")
    
    logger.info(f"Job {job_id} cancelled by user")
    return {"status": "cancelled", "job_id": job_id}


@app.post("/api/start_job")
@limiter.limit("5/minute")
async def start_job(
    request: Request,
    batch_request: BatchRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
    access_token: str = Cookie(None)
):
    """Start a batch of jobs."""
    if not access_token:
        raise HTTPException(status_code=401, detail="Not authenticated")
    
    batch_jobs = []
    for item in batch_request.queue:
        job_id = str(uuid.uuid4())
        new_job = Job(
            id=job_id,
            status="queued",
            project_id=str(item.project_id),
            project_name=item.project_name
        )
        db.add(new_job)
        batch_jobs.append({
            "id": job_id,
            "project_id": item.project_id,
            "disciplines": item.disciplines,
            "drawing_ids": item.drawing_ids if hasattr(item, 'drawing_ids') else None
        })
    
    db.commit()
    
    # Run in background
    background_tasks.add_task(process_batch_sequence, batch_jobs, access_token)
    
    logger.info(f"Queued {len(batch_jobs)} jobs")
    return {"status": "queued", "count": len(batch_jobs)}


@app.get("/api/schedules")
async def get_schedules(db: Session = Depends(get_db)):
    """Get all scheduled jobs."""
    schedules = db.query(ScheduledJob).filter(ScheduledJob.active == True).all()
    return schedules


@app.post("/api/schedule_batch")
@limiter.limit("10/minute")
async def create_schedule(
    request: Request,
    schedule_request: ScheduleRequest,
    db: Session = Depends(get_db)
):
    """Create scheduled jobs."""
    h, m = map(int, schedule_request.time.split(":"))
    
    saved_count = 0
    for item in schedule_request.queue:
        s_job = ScheduledJob(
            id=str(uuid.uuid4()),
            project_id=str(item.project_id),
            project_name=item.project_name,
            disciplines_json=json.dumps(item.disciplines),
            day_of_week=schedule_request.day,
            hour=h,
            minute=m,
            timezone=schedule_request.timezone
        )
        db.add(s_job)
        saved_count += 1
    
    db.commit()
    
    logger.info(f"Created {saved_count} scheduled jobs")
    return {"status": "scheduled", "count": saved_count}


@app.delete("/api/schedule/{job_id}")
async def delete_schedule(job_id: str, db: Session = Depends(get_db)):
    """Delete a scheduled job."""
    deleted = db.query(ScheduledJob).filter(ScheduledJob.id == job_id).delete()
    db.commit()
    
    if deleted:
        logger.info(f"Deleted schedule {job_id}")
        return {"status": "deleted"}
    else:
        raise HTTPException(status_code=404, detail="Schedule not found")


@app.get("/health", response_model=HealthResponse)
async def health_check(db: Session = Depends(get_db)):
    """Health check endpoint."""
    # Check database
    db_status = "healthy"
    try:
        # Proper SQLAlchemy syntax for executing raw SQL
        db.execute(text("SELECT 1"))
        db.commit()
    except Exception as e:
        logger.error(f"Database health check failed: {e}", exc_info=True)
        db_status = "unhealthy"
    
    # Check disk space
    disk_usage = 0
    try:
        stat = os.statvfs(".")
        disk_usage = (stat.f_bavail * stat.f_frsize) // (1024 * 1024)  # MB
    except:
        pass
    
    return HealthResponse(
        status="healthy" if db_status == "healthy" else "degraded",
        database=db_status,
        disk_space_mb=disk_usage,
        environment=settings.ENVIRONMENT,
        redirect_uri=settings.redirect_uri  # Include redirect URI for debugging
    )


@app.get("/debug/redirect-uri")
async def debug_redirect_uri():
    """Production-safe endpoint to show the exact redirect URI being used."""
    return JSONResponse({
        "redirect_uri": settings.redirect_uri,
        "base_url": settings.BASE_URL,
        "render_external_url": os.getenv("RENDER_EXTERNAL_URL"),
        "environment": settings.ENVIRONMENT,
        "instructions": {
            "step_1": "Copy the redirect_uri value above",
            "step_2": "Go to https://developers.procore.com/",
            "step_3": "Select your app and go to Redirect URIs",
            "step_4": "Add the EXACT redirect_uri (must match character-for-character)",
            "step_5": "Common mistakes: trailing slash, http vs https, wrong domain"
        }
    })


@app.get("/debug/config")
async def debug_config():
    """Debug endpoint to check OAuth configuration (development only)."""
    if settings.is_production:
        raise HTTPException(status_code=404, detail="Not found")
    
    # Check configuration
    config_status = {
        "client_id_set": bool(settings.PROCORE_CLIENT_ID),
        "client_secret_set": bool(settings.PROCORE_CLIENT_SECRET),
        "secret_key_set": bool(settings.SECRET_KEY),
        "company_id": settings.PROCORE_COMPANY_ID,
        "redirect_uri": settings.redirect_uri,
        "token_url": settings.token_url,
        "login_url": settings.login_url,
        "environment": settings.ENVIRONMENT,
    }
    
    # Validate SECRET_KEY
    try:
        settings.get_encryption_key()
        config_status["secret_key_valid"] = True
    except Exception as e:
        config_status["secret_key_valid"] = False
        config_status["secret_key_error"] = str(e)
    
    # Check database
    try:
        db = SessionLocal()
        token_count = db.query(TokenStore).count()
        db.close()
        config_status["database_connected"] = True
        config_status["stored_tokens"] = token_count
    except Exception as e:
        config_status["database_connected"] = False
        config_status["database_error"] = str(e)
    
    # Generate HTML response with styling
    issues = []
    if not config_status["client_id_set"]:
        issues.append("❌ PROCORE_CLIENT_ID is not set")
    if not config_status["client_secret_set"]:
        issues.append("❌ PROCORE_CLIENT_SECRET is not set")
    if not config_status["secret_key_set"]:
        issues.append("❌ SECRET_KEY is not set (required for token encryption)")
    elif not config_status.get("secret_key_valid"):
        issues.append(f"❌ SECRET_KEY is invalid: {config_status.get('secret_key_error')}")
    
    status_html = "<ul>"
    for key, value in config_status.items():
        if key == "client_secret_set" or key == "secret_key_set":
            # Don't show actual secret values, just if they're set
            status_html += f"<li><strong>{key}:</strong> {'✅ Set' if value else '❌ Not set'}</li>"
        elif key not in ["secret_key_error", "database_error"]:
            status_html += f"<li><strong>{key}:</strong> {value}</li>"
    status_html += "</ul>"
    
    return HTMLResponse(
        f"""
        <html>
            <head><title>Configuration Debug</title></head>
            <body style="font-family: Arial; padding: 40px; max-width: 800px; margin: 0 auto;">
                <h1>🔧 Configuration Debug</h1>
                <p><strong>This page is only available in development mode.</strong></p>
                
                <h2>Configuration Status</h2>
                {status_html}
                
                <h2>Issues Found</h2>
                {"<ul>" + "".join(f"<li>{issue}</li>" for issue in issues) + "</ul>" if issues else "<p>✅ No issues found!</p>"}
                
                <h2>Next Steps</h2>
                <ol>
                    <li>Ensure all required values are set in your <code>.env</code> file</li>
                    <li>Verify redirect URI in Procore matches: <code>{settings.redirect_uri}</code></li>
                    <li>Restart the server after changing .env file</li>
                    <li>Try <a href="/login">logging in again</a></li>
                </ol>
                
                <h2>Procore Developer Portal Checklist</h2>
                <ul>
                    <li>✅ Redirect URI added: <code>{settings.redirect_uri}</code></li>
                    <li>✅ Client ID matches PROCORE_CLIENT_ID in .env</li>
                    <li>✅ Client Secret matches PROCORE_CLIENT_SECRET in .env</li>
                    <li>✅ App has appropriate permissions enabled</li>
                </ul>
                
                <p><a href="/">← Back to Home</a> | <a href="/login">Try Login</a></p>
            </body>
        </html>
        """
    )


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """Global exception handler."""
    logger.error(f"Unhandled exception: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={"detail": "An internal error occurred"}
    )
