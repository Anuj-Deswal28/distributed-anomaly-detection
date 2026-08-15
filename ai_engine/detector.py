import json
import os
from collections import deque

import numpy as np
import redis
from sklearn.ensemble import IsolationForest

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_CHANNEL = "sensor_readings"
ANALYZED_CHANNEL = "sensor_analyzed"

WINDOW_SIZE = 200
MIN_TRAIN_SIZE = 50
RETRAIN_EVERY = 50
Z_SCORE_THRESHOLD = 3.0
SUMMARY_EVERY = 100
IFOREST_CONTAMINATION = 0.05
LOCAL_SPAN = 10


def causal_moving_average(arr: np.ndarray, span: int) -> np.ndarray:
    ma = np.empty(len(arr))
    for i in range(len(arr)):
        start = max(0, i - span + 1)
        ma[i] = np.mean(arr[start:i + 1])
    return ma


class Scoreboard:
    def __init__(self, name: str):
        self.name = name
        self.tp = self.fp = self.tn = self.fn = 0

    def update(self, predicted: bool, actual: bool):
        if predicted and actual:
            self.tp += 1
        elif predicted and not actual:
            self.fp += 1
        elif not predicted and actual:
            self.fn += 1
        else:
            self.tn += 1

    def precision(self) -> float:
        denom = self.tp + self.fp
        return self.tp / denom if denom else 0.0

    def recall(self) -> float:
        denom = self.tp + self.fn
        return self.tp / denom if denom else 0.0

    def summary(self) -> str:
        return (f"{self.name}: precision={self.precision():.2f} "
                f"recall={self.recall():.2f} "
                f"(tp={self.tp} fp={self.fp} fn={self.fn} tn={self.tn})")


class SensorDetector:
    def __init__(self, sensor_type: str):
        self.sensor_type = sensor_type
        self.window = deque(maxlen=WINDOW_SIZE)
        self.model: IsolationForest | None = None
        self.readings_since_retrain = 0
        self.zscore_scoreboard = Scoreboard(f"{sensor_type} z-score")
        self.iforest_scoreboard = Scoreboard(f"{sensor_type} isolation-forest")

    def zscore_flag(self, value: float) -> bool:
        if len(self.window) < 10:
            return False
        mean = np.mean(self.window)
        std = np.std(self.window)
        if std == 0:
            return False
        z = abs((value - mean) / std)
        return z > Z_SCORE_THRESHOLD

    def maybe_retrain_iforest(self):
        if len(self.window) < MIN_TRAIN_SIZE:
            return
        if self.readings_since_retrain < RETRAIN_EVERY and self.model is not None:
            return

        window_arr = np.array(self.window)
        local_trend = causal_moving_average(window_arr, LOCAL_SPAN)
        residuals = window_arr - local_trend

        mean, std = np.mean(residuals), np.std(residuals)
        if std > 0:
            z_scores = np.abs((residuals - mean) / std)
            clean = residuals[z_scores < Z_SCORE_THRESHOLD]
        else:
            clean = residuals

        if len(clean) < MIN_TRAIN_SIZE // 2:
            return

        X = clean.reshape(-1, 1)
        self.model = IsolationForest(
            contamination=IFOREST_CONTAMINATION, random_state=42
        )
        self.model.fit(X)
        self.readings_since_retrain = 0

    def local_residual(self, value: float) -> float:
        if len(self.window) == 0:
            return 0.0
        recent = list(self.window)[-LOCAL_SPAN:]
        local_mean = np.mean(recent)
        return value - local_mean

    def iforest_flag(self, value: float) -> bool:
        if self.model is None:
            return False
        residual = self.local_residual(value)
        prediction = self.model.predict([[residual]])
        return prediction[0] == -1

    def process(self, value: float, actual_anomaly: bool):
        zscore_pred = self.zscore_flag(value)
        self.maybe_retrain_iforest()
        iforest_pred = self.iforest_flag(value)

        self.zscore_scoreboard.update(zscore_pred, actual_anomaly)
        self.iforest_scoreboard.update(iforest_pred, actual_anomaly)

        self.window.append(value)
        self.readings_since_retrain += 1

        return zscore_pred, iforest_pred


detectors: dict[str, SensorDetector] = {}
total_readings = 0


def get_detector(sensor_type: str) -> SensorDetector:
    if sensor_type not in detectors:
        detectors[sensor_type] = SensorDetector(sensor_type)
    return detectors[sensor_type]


def print_summary():
    print(f"\n--- summary after {total_readings} readings ---")
    for detector in detectors.values():
        print("  " + detector.zscore_scoreboard.summary())
        print("  " + detector.iforest_scoreboard.summary())
    print("-" * 40)


def handle_reading(raw_payload: str, publisher: redis.Redis):
    global total_readings
    try:
        reading = json.loads(raw_payload)
    except json.JSONDecodeError:
        print(f"[warn] could not parse payload: {raw_payload!r}")
        return

    sensor_type = reading["sensor_type"]
    value = reading["value"]
    actual_anomaly = reading["is_anomaly"]

    detector = get_detector(sensor_type)
    zscore_pred, iforest_pred = detector.process(value, actual_anomaly)

    analyzed = {
        **reading,
        "zscore_anomaly": bool(zscore_pred),
        "iforest_anomaly": bool(iforest_pred),
    }
    publisher.publish(ANALYZED_CHANNEL, json.dumps(analyzed))

    total_readings += 1
    if zscore_pred or iforest_pred or actual_anomaly:
        flags = []
        if zscore_pred: flags.append("Z")
        if iforest_pred: flags.append("IF")
        if actual_anomaly: flags.append("TRUE")
        print(f"[{sensor_type}] value={value} flags={','.join(flags) or '-'}")

    if total_readings % SUMMARY_EVERY == 0:
        print_summary()


def main():
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    pubsub = r.pubsub()
    pubsub.subscribe(REDIS_CHANNEL)
    print(f"[ai_engine] subscribed to '{REDIS_CHANNEL}', publishing results to '{ANALYZED_CHANNEL}'...")

    for message in pubsub.listen():
        if message["type"] != "message":
            continue
        handle_reading(message["data"], r)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print_summary()
        print("\nStopped.")
