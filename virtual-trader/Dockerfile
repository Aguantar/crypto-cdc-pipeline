FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY trader.py .
RUN useradd -m trader
USER trader
CMD ["python", "-u", "trader.py"]
