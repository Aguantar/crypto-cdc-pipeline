FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY collector.py .

RUN useradd -m collector
USER collector

CMD ["python", "-u", "collector.py"]
