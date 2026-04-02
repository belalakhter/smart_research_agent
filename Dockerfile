FROM python:3.12-slim-bullseye

ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

RUN apt-get update && \
    apt-get install -y gcc build-essential python3-dev && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .


RUN --mount=type=cache,target=/root/.cache/pip \
    pip install --upgrade pip && \
    pip install -r requirements.txt gunicorn

COPY . .

EXPOSE 4000

CMD ["gunicorn", \
    "--workers=4", \
    "--threads=8", \
    "--worker-class=gthread", \
    "--timeout=120", \
    "--preload", \
    "--config=gunicorn.conf.py", \
    "--bind=0.0.0.0:4000", \
    "app.main:app"]