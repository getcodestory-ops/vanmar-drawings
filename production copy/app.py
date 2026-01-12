import os
import uuid
import time
import requests
import json
from datetime import datetime
from typing import List

from fastapi import FastAPI, Request, BackgroundTasks, Depends, Cookie, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
from sqlalchemy import create_engine, Column, String, DateTime, Integer, Text, Boolean
from sqlalchemy.orm import sessionmaker, declarative_base, Session
from dotenv import load_dotenv

from apscheduler.schedulers.background import BackgroundScheduler
import pytz

import core_engine

load_dotenv()
app = FastAPI()

# --- CONFIG ---
CLIENT_ID = os.getenv("PROCORE_CLIENT_ID")
CLIENT_SECRET = os.getenv("PROCORE_CLIENT_SECRET")
REDIRECT_URI = "http://localhost:8000/callback" 
LOGIN_URL = "https://login.procore.com/oauth/authorize"
TOKEN_URL = "https://login.procore.com/oauth/token"

SQLALCHEMY_DATABASE_URL = "sqlite:///./jobs.db"
engine = create_engine(SQLALCHEMY_DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# --- DB MODELS ---
class Job(Base):
    __tablename__ = "jobs"
    id = Column(String, primary_key=True, index=True)
    status = Column(String) 
    project_id = Column(String)
    project_name = Column(String)
    created_at = Column(DateTime, default=datetime.utcnow)
    result_message = Column(String, nullable=True)
    progress = Column(Integer, default=0)  # <--- NEW COLUMN
class TokenStore(Base):
    __tablename__ = "token_store"
    id = Column(Integer, primary_key=True, index=True)
    refresh_token = Column(Text)
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
    timezone = Column(String)   # <--- NEW: Stores "America/Mexico_City"
    active = Column(Boolean, default=True)

Base.metadata.create_all(bind=engine)

# --- API MODELS ---
class SingleJobConfig(BaseModel):
    project_id: int
    project_name: str
    disciplines: List[str]

class BatchRequest(BaseModel):
    queue: List[SingleJobConfig]

class ScheduleRequest(BaseModel):
    queue: List[SingleJobConfig]
    day: str   
    time: str  
    timezone: str # <--- NEW

templates = Jinja2Templates(directory="templates")
if not os.path.exists("output"): os.makedirs("output")
app.mount("/output", StaticFiles(directory="output"), name="output")

def get_db():
    db = SessionLocal()
    try: yield db
    finally: db.close()

# --- AUTH ---
def get_fresh_access_token():
    db = SessionLocal()
    token_entry = db.query(TokenStore).first()
    if not token_entry or not token_entry.refresh_token:
        db.close()
        print("❌ Scheduler Error: No refresh token found.")
        return None

    payload = {"grant_type": "refresh_token", "refresh_token": token_entry.refresh_token, "client_id": CLIENT_ID, "client_secret": CLIENT_SECRET, "redirect_uri": REDIRECT_URI}
    try:
        r = requests.post(TOKEN_URL, data=payload)
        if r.status_code == 200:
            tokens = r.json()
            token_entry.refresh_token = tokens['refresh_token']
            token_entry.updated_at = datetime.utcnow()
            db.commit()
            db.close()
            return tokens['access_token']
    except Exception as e:
        print(f"Token refresh failed: {e}")
    db.close()
    return None

# --- WORKER ---
def process_batch_sequence(job_list: List[dict], access_token: str):
    db = SessionLocal()
    
    for job_meta in job_list:
        job_id = job_meta["id"]
        
        job_record = db.query(Job).filter(Job.id == job_id).first()
        if not job_record: continue
        
        job_record.status = "processing"
        job_record.progress = 0 # Start at 0
        db.commit()
        
        # Define the callback function
        def update_db_progress(percent):
            # We re-query inside callback to avoid stale sessions
            # Using a new session for status updates ensures thread safety
            try:
                sub_db = SessionLocal()
                jr = sub_db.query(Job).filter(Job.id == job_id).first()
                if jr:
                    jr.progress = percent
                    sub_db.commit()
                sub_db.close()
            except: pass

        try:
            print(f"--- RUNNING: {job_meta['project_id']} ---")
            
            # Pass the callback here!
            local_path = core_engine.run_job_api(
                job_meta["project_id"], 
                job_meta["disciplines"], 
                access_token,
                progress_callback=update_db_progress # <--- CONNECTED
            )
            
            # Upload Phase (95%)
            update_db_progress(95)
            job_record.status = "uploading"
            db.commit()
            
            res = core_engine.handle_procore_upload(job_meta["project_id"], local_path, access_token)
            
            job_record.status = "completed"
            job_record.progress = 100 # Done
            job_record.result_message = os.path.join("output", os.path.basename(local_path)) + ("||" + res if res != "Success" else "")

        except Exception as e:
            job_record.status = "failed"
            job_record.result_message = str(e)
        
        db.commit()
        time.sleep(5)

    db.close()

# ---------------------------------------------------------
# TIMEZONE-AWARE WATCHDOG
# ---------------------------------------------------------
def scheduler_tick():
    """
    Checks EVERY active job to see if it matches the current time 
    in THAT JOB'S specific timezone.
    """
    db = SessionLocal()
    active_jobs = db.query(ScheduledJob).filter(ScheduledJob.active == True).all()
    
    jobs_to_run = []
    
    for s_job in active_jobs:
        try:
            # 1. Get current time in the user's specific timezone
            user_tz = pytz.timezone(s_job.timezone)
            now_in_user_tz = datetime.now(user_tz)
            
            # 2. Check match
            current_day = now_in_user_tz.strftime("%a") # "Fri"
            
            if (current_day == s_job.day_of_week and 
                now_in_user_tz.hour == s_job.hour and 
                now_in_user_tz.minute == s_job.minute):
                
                print(f"⏰ Time Match! Triggering {s_job.project_name} (Timezone: {s_job.timezone})")
                jobs_to_run.append(s_job)
                
        except Exception as e:
            print(f"Error checking schedule for {s_job.project_name}: {e}")

    if not jobs_to_run:
        db.close()
        return

    # 3. Execution Logic
    access_token = get_fresh_access_token()
    if not access_token: 
        db.close()
        return

    batch_jobs = []
    for s_job in jobs_to_run:
        job_id = str(uuid.uuid4())
        new_job = Job(id=job_id, status="queued", project_id=s_job.project_id, project_name=f"[AUTO] {s_job.project_name}")
        db.add(new_job)
        discs = json.loads(s_job.disciplines_json)
        batch_jobs.append({"id": job_id, "project_id": int(s_job.project_id), "disciplines": discs})

    db.commit()
    db.close()
    
    process_batch_sequence(batch_jobs, access_token)

# Run every 60 seconds
scheduler = BackgroundScheduler()
scheduler.add_job(scheduler_tick, 'interval', seconds=60)
scheduler.start()

# --- ROUTES ---
@app.get("/", response_class=HTMLResponse)
async def home(request: Request, access_token: str = Cookie(None)):
    if not access_token: return templates.TemplateResponse("login.html", {"request": request})
    return templates.TemplateResponse("dashboard.html", {"request": request})

@app.get("/login")
async def login():
    url = f"{LOGIN_URL}?response_type=code&client_id={CLIENT_ID}&redirect_uri={REDIRECT_URI}"
    return RedirectResponse(url)

@app.get("/callback")
async def callback(code: str, db: Session = Depends(get_db)):
    payload = {"grant_type": "authorization_code", "code": code, "client_id": CLIENT_ID, "client_secret": CLIENT_SECRET, "redirect_uri": REDIRECT_URI}
    try:
        r = requests.post(TOKEN_URL, data=payload)
        r.raise_for_status()
        tokens = r.json()
        
        ref = tokens.get("refresh_token")
        if ref:
            existing = db.query(TokenStore).first()
            if existing: existing.refresh_token = ref
            else: db.add(TokenStore(refresh_token=ref))
            db.commit()

        response = RedirectResponse(url="/")
        response.set_cookie(key="access_token", value=tokens["access_token"], httponly=True)
        return response
    except: return HTMLResponse("Login Failed")

@app.get("/api/projects")
def get_projects(access_token: str = Cookie(None)):
    if not access_token: raise HTTPException(status_code=401)
    p = core_engine.api_get_projects(access_token)
    if p is None: raise HTTPException(status_code=401)
    return p

@app.get("/api/disciplines")
def get_disciplines(project_id: int, access_token: str = Cookie(None)):
    return core_engine.api_get_disciplines(project_id, access_token)

@app.get("/api/recent_jobs")
def get_recent_jobs(db: Session = Depends(get_db)):
    return db.query(Job).order_by(Job.created_at.desc()).limit(20).all()

@app.post("/api/start_job")
def start_job(request: BatchRequest, background_tasks: BackgroundTasks, db: Session = Depends(get_db), access_token: str = Cookie(None)):
    if not access_token: raise HTTPException(status_code=401)
    batch_jobs = []
    for item in request.queue:
        job_id = str(uuid.uuid4())
        new_job = Job(id=job_id, status="queued", project_id=str(item.project_id), project_name=item.project_name)
        db.add(new_job)
        batch_jobs.append({"id": job_id, "project_id": item.project_id, "disciplines": item.disciplines})
    db.commit()
    background_tasks.add_task(process_batch_sequence, batch_jobs, access_token)
    return {"status": "queued", "count": len(batch_jobs)}

@app.get("/api/schedules")
def get_schedules(db: Session = Depends(get_db)):
    return db.query(ScheduledJob).all()

@app.post("/api/schedule_batch")
def create_schedule(request: ScheduleRequest, db: Session = Depends(get_db)):
    h, m = map(int, request.time.split(":"))
    
    saved_count = 0
    for item in request.queue:
        s_job = ScheduledJob(
            id=str(uuid.uuid4()),
            project_id=str(item.project_id),
            project_name=item.project_name,
            disciplines_json=json.dumps(item.disciplines),
            day_of_week=request.day,
            hour=h,
            minute=m,
            timezone=request.timezone # <--- SAVING TIMEZONE
        )
        db.add(s_job)
        saved_count += 1
    
    db.commit()
    return {"status": "scheduled", "count": saved_count}

@app.delete("/api/schedule/{job_id}")
def delete_schedule(job_id: str, db: Session = Depends(get_db)):
    db.query(ScheduledJob).filter(ScheduledJob.id == job_id).delete()
    db.commit()
    return {"status": "deleted"}