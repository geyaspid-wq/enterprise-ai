FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir "redis>=5.0.1"
COPY orchestrator.py /app/orchestrator.py
CMD ["python", "orchestrator.py"]
