import os
import requests
from fastapi import FastAPI

app = FastAPI(title="FastAPI HDFS API")

HDFS_URL = os.getenv("HDFS_URL", "http://namenode:9870/webhdfs/v1")
HDFS_USER = os.getenv("HDFS_USER", "hdfs")


@app.post("/write")
def write_file(path: str, data: str):
    # Step 1: Request redirect URL

    create_url = f"{HDFS_URL}{path}?op=CREATE&user.name={HDFS_USER}"
    r = requests.put(create_url, allow_redirects=False)

    if "Location" not in r.headers:
        return {"error": "Failed to create file", "details": r.text, "status": r.status_code}

    # Step 2: Upload data
    upload_url = r.headers["Location"]
    upload = requests.put(upload_url, data=data.encode())

    if upload.status_code not in (200, 201):
        return {"error": "Upload failed", "details": upload.text}

    return {"message": f"File '{path}' written successfully"}


@app.get("/read")
def read_file(path: str):
    open_url = f"{HDFS_URL}{path}?op=OPEN&user.name={HDFS_USER}"
    r = requests.get(open_url)

    if r.status_code != 200:
        return {"error": "Cannot read file", "details": r.text, "status": r.status_code}

    return {"content": r.text}
