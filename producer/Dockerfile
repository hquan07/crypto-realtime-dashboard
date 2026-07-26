FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY crypto_producer.py .

CMD ["python", "-u", "crypto_producer.py"]
