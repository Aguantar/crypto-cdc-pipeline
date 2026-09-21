FROM python:3.11-slim

WORKDIR /app

# 의존성 설치 (캐시 활용)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY producer.py .

# 비root 유저로 실행
RUN useradd -m producer
USER producer

CMD ["python", "-u", "producer.py"]
