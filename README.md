# Distributed Real-Time Anomaly Detection

A simulated distributed IoT platform where multiple sensor nodes stream real-time
data (temperature, traffic, network throughput) over MQTT into a FastAPI backend.
A Redis pub/sub layer decouples ingestion from processing, an Isolation Forest /
z-score AI engine flags anomalies as readings arrive, and a live Chart.js dashboard
visualizes every stream with anomalies highlighted.

Built as a practical training capstone — MDU Rohtak, B.Tech CSE, 2026.

## Architecture

```
Sensor simulators  →  MQTT (Mosquitto)  →  FastAPI  →  Redis pub/sub
                                                            │
                                                            ▼
                                                        AI engine
                                              (z-score + Isolation Forest)
                                                            │
                                                            ▼
                                                     Redis pub/sub
                                                            │
                                                            ▼
                                         FastAPI WebSocket  →  Chart.js dashboard
```

Every arrow above is a real network hop through Redis or MQTT, not a function
call — ingestion, AI processing, and the dashboard are three independently
swappable processes that never talk to each other directly. That's what makes
this a distributed system rather than one monolithic script.

## Features

- **Three simulated sensors** (temperature, traffic, network throughput), each
  with a realistic baseline pattern and a configurable rate of labeled,
  injected anomalies (spikes, bursts, flatlines, drops)
- **MQTT ingestion** via Mosquitto, decoupled from processing through Redis pub/sub
- **Two independent anomaly detectors** running side by side:
  - **Z-score** — fast, no training required, strong on single-variable spikes
  - **Isolation Forest** — retrains periodically on detrended residuals so
    slow seasonal drift isn't mistaken for anomalous spread
- **Self-scoring against ground truth** — since the simulator labels every
  reading, the AI engine computes live precision/recall per detector instead
  of relying on visual inspection
- **Live dashboard** (FastAPI WebSocket + Chart.js) with anomalies highlighted
  in real time, plus running stats (readings, detected, missed)
- **Fully Dockerized** — one command starts all five services

## Tech stack

| Layer | Technology |
|---|---|
| Backend | FastAPI, Uvicorn (async) |
| Messaging | Redis 7 (pub/sub), Mosquitto (MQTT) |
| AI / ML | scikit-learn (Isolation Forest), NumPy (z-score) |
| Frontend | Chart.js, vanilla JS/HTML/CSS |
| Infra | Docker & Docker Compose |
| Language | Python 3.11+ |

## Getting started

### Option 1 — Docker Compose (recommended)

```bash
docker compose up --build -d
docker ps          # confirm all 5 containers are Up
```

Open the dashboard at **http://localhost:8000/dashboard**.

Check backend health at `http://localhost:8000/health`.

### Option 2 — Run manually (useful for development)

```bash
# 1. Start infra only
docker compose up -d redis mosquitto

# 2. Python environment
python -m venv anomaly-env
source anomaly-env/bin/activate      # Windows: anomaly-env\Scripts\Activate.ps1
pip install -r requirements.txt

# 3. In separate terminals:
uvicorn backend.app.main:app --reload --port 8000
python ai_engine/detector.py
python simulator/sensor_simulator.py --anomaly-rate 0.05 --interval 1.0
```

## Project structure

```
anomaly-detection/
├── simulator/          # Sensor simulators, publish over MQTT
├── backend/app/         # FastAPI: MQTT→Redis relay + WebSocket to dashboard
├── ai_engine/           # z-score + Isolation Forest detectors, self-scoring
├── dashboard/static/    # Chart.js live dashboard (served by FastAPI)
├── mosquitto/config/    # MQTT broker config
├── docker-compose.yml   # Full 5-service stack
└── requirements.txt
```

## Evaluation

The AI engine scores both detectors against the simulator's ground-truth labels
as data streams in, printing a running summary every 100 readings. Example
results from a synthetic run with a sine-wave temperature baseline:

| Detector | Precision | Recall |
|---|---|---|
| Z-score | 1.00 | 0.96–0.98 |
| Isolation Forest (solo) | 0.32–0.54 | 0.75–0.96 |
| Consensus (both agree) | ~1.00 | ~0.96 |

**Key finding:** z-score comfortably outperforms Isolation Forest on this
single-variable, synthetic anomaly data — a legitimate and expected result,
not a shortcoming. Isolation Forest's advantage is multivariate anomaly
detection, which this project doesn't currently exercise (each sensor is
scored independently). Requiring both detectors to agree before flagging an
anomaly nearly eliminates false positives at a small cost to recall.

## Known limitations

- Single-machine setup can't replicate true distributed failure modes
- No persistence layer — Isolation Forest retrains from scratch on every restart
- MQTT/Redis use fire-and-forget delivery (no durability guarantee), a
  deliberate CAP-theorem tradeoff favoring availability over consistency
- Isolation Forest is only exercised on single-variable data

## Future scope

Apache Kafka, LSTM/Transformer models, InfluxDB/TimescaleDB persistence,
Kubernetes deployment, adaptive thresholds, federated edge learning, security
hardening.

## License

MIT — see [LICENSE](LICENSE).
