from fastapi import APIRouter
import os
import pathlib

router = APIRouter()

@router.get("/debug")
def debug_info():
    try:
        config = pathlib.Path("../tpcds_full_scan.yaml").read_text()
    except Exception as e:
        config = str(e)
    try:
        files = os.listdir("../test")
    except Exception as e:
        files = str(e)
    return {"cwd": os.getcwd(), "test_dir": files, "config": config}
