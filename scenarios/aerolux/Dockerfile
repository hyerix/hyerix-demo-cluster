FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY generate.py .

# -u forces unbuffered stdout so `docker compose logs -f` shows live output.
ENTRYPOINT ["python", "-u", "generate.py"]
