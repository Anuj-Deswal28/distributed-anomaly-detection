FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY simulator ./simulator

CMD ["python", "simulator/sensor_simulator.py", "--anomaly-rate", "0.05", "--interval", "1.0"]
