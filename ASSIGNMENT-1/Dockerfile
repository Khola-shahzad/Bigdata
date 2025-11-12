# ---------------- Dockerfile ----------------
FROM python:3.9-slim

WORKDIR /app

COPY . /app

# Install required Python packages
RUN pip install hdfs python-dotenv

CMD ["python", "main.py"]






#docker exec -it hdfs_crud_app bash