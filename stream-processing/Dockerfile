FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY alert_consumer.py .
COPY indicator_consumer.py .

# Create a runner script
RUN echo '#!/bin/bash\npython -u alert_consumer.py &\npython -u indicator_consumer.py' > run_consumers.sh
RUN chmod +x run_consumers.sh

CMD ["./run_consumers.sh"]
