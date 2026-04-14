import os
import uuid
import traceback
from datetime import datetime
from typing import Dict, Any

from fastapi import FastAPI, BackgroundTasks, HTTPException, Request
from test_endpoint import router as debug_router

from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sqlalchemy import create_engine, Column, String, DateTime
from sqlalchemy.orm import declarative_base, sessionmaker

# --- DQS Imports ---
import sys
# Add parent dir to path so we can import dqs directly
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dqs.scanner import scan_from_file
from dqs.reporter import render

# --- Database Setup (SQLite) ---
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "jobs.db")
engine = create_engine(f"sqlite:///{DB_PATH}", connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class ScanJob(Base):
    __tablename__ = "scan_jobs"

    id = Column(String, primary_key=True, index=True)
    config_path = Column(String, nullable=False)
    status = Column(String, default="PENDING")  # PENDING, RUNNING, COMPLETED, FAILED
    start_time = Column(DateTime, default=datetime.utcnow)
    end_time = Column(DateTime, nullable=True)
    report_file_path = Column(String, nullable=True)
    error_message = Column(String, nullable=True)

Base.metadata.create_all(bind=engine)

# --- FastAPI App ---
app = FastAPI(title="DQS Orchestrator API")
app.include_router(debug_router)


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # For dev, allow all origins. Configure properly for prod.
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Models ---
class TriggerScanRequest(BaseModel):
    config_path: str = "tpcds_full_scan.yaml"  # Default test config

class JobResponse(BaseModel):
    job_id: str
    status: str
    message: str

class StatusResponse(BaseModel):
    job_id: str
    config_path: str
    status: str
    start_time: str
    end_time: str | None
    error_message: str | None
    duration_seconds: float | None

# --- Background Task Logic ---
def execute_scan_task(job_id: str, config_path: str):
    db = SessionLocal()
    job = db.query(ScanJob).filter(ScanJob.id == job_id).first()
    if not job:
        db.close()
        return

    job.status = "RUNNING"
    db.commit()

    try:
        # Resolve config path relative to the backend directory or absolute
        abs_config_path = config_path
        if not os.path.isabs(config_path):
            abs_config_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), config_path)
            
        print(f"[{job_id}] Starting scan for: {abs_config_path}")

        # Run DQS locally in this background thread
        report = scan_from_file(
            config_path=abs_config_path,
            mode="live" # Can also expose mode as a param
        )

        reports_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "reports")
        os.makedirs(reports_dir, exist_ok=True)
        
        output_html_path = os.path.join(reports_dir, f"{job_id}_report.html")
        
        json_output_path = os.path.join(reports_dir, f"{job_id}_result.json")
        render(report, output_format="json", output_path=json_output_path)
        
        # Now convert the json to HTML report format requested, importing build_html from generate_report
        import json
        from generate_report import build_html
        with open(json_output_path, 'r') as f:
            data = json.load(f)
            
        # We might need logo base64 if it's there
        logo_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "refined_logo.png")
        logo_b64 = ""
        if os.path.exists(logo_path):
            import base64
            with open(logo_path, "rb") as lf:
                logo_b64 = base64.b64encode(lf.read()).decode()
                
        html_report = build_html(data, logo_b64)
        
        with open(output_html_path, "w", encoding="utf-8") as f:
            f.write(html_report)

        job.status = "COMPLETED"
        job.report_file_path = output_html_path
        print(f"[{job_id}] Scan completed successfully. Report saved to {output_html_path}")

    except Exception as e:
        job.status = "FAILED"
        job.error_message = str(e)
        print(f"[{job_id}] Scan failed: {str(e)}")
        print(traceback.format_exc())

    finally:
        job.end_time = datetime.utcnow()
        db.commit()
        db.close()


# --- API Endpoints ---

@app.post("/api/jobs", response_model=JobResponse)
async def trigger_scan(req: TriggerScanRequest, background_tasks: BackgroundTasks):
    job_id = str(uuid.uuid4())
    
    db = SessionLocal()
    job = ScanJob(id=job_id, config_path=req.config_path, status="PENDING")
    db.add(job)
    db.commit()
    db.close()
    
    background_tasks.add_task(execute_scan_task, job_id, req.config_path)
    
    return JobResponse(job_id=job_id, status="PENDING", message="Scan job submitted successfully.")

@app.get("/api/jobs/{job_id}", response_model=StatusResponse)
def get_job_status(job_id: str):
    db = SessionLocal()
    try:
        job = db.query(ScanJob).filter(ScanJob.id == job_id).first()
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")
        
        duration = None
        if job.end_time and job.start_time:
            duration = (job.end_time - job.start_time).total_seconds()
        elif job.status == "RUNNING" and job.start_time:
            duration = (datetime.utcnow() - job.start_time).total_seconds()

        return StatusResponse(
            job_id=job.id,
            config_path=job.config_path,
            status=job.status,
            start_time=job.start_time.isoformat() + "Z" if job.start_time else "",
            end_time=job.end_time.isoformat() + "Z" if job.end_time else None,
            error_message=job.error_message,
            duration_seconds=duration
        )
    finally:
        db.close()

@app.get("/api/reports/{job_id}", response_class=HTMLResponse)
def get_report(job_id: str):
    db = SessionLocal()
    try:
        job = db.query(ScanJob).filter(ScanJob.id == job_id).first()
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")
            
        if job.status != "COMPLETED":
             raise HTTPException(status_code=400, detail=f"Report not ready. Current status: {job.status}")
             
        if not job.report_file_path or not os.path.exists(job.report_file_path):
            raise HTTPException(status_code=500, detail="Report file missing")
            
        with open(job.report_file_path, "r", encoding="utf-8") as f:
            return f.read()
    finally:
        db.close()

@app.get("/logo")
def get_logo():
    from fastapi.responses import FileResponse
    logo_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "refined_logo.png")
    if not os.path.exists(logo_path):
        raise HTTPException(status_code=404, detail="Logo not found")
    return FileResponse(logo_path, media_type="image/png")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)
