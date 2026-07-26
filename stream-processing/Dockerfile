FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY alert_consumer.py .
COPY indicator_consumer.py .
COPY redis_sink_crypto.py .

# Runner: three background consumers, forwarding SIGTERM to all of them
RUN cat > run_consumers.sh <<'EOF'
#!/bin/bash
set -e
python -u alert_consumer.py &
python -u indicator_consumer.py &
python -u redis_sink_crypto.py &
wait -n
exit $?
EOF
RUN chmod +x run_consumers.sh

CMD ["./run_consumers.sh"]