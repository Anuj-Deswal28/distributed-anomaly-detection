"""
Day 2 sensor simulator.

Generates synthetic readings for three sensor types (temperature, traffic,
network throughput) on a fixed interval, with a configurable chance of
injecting a labeled anomaly into each reading.

This version prints JSON lines to stdout. MQTT publishing gets wired in
next (Week 1.5-2) -- keeping this stage isolated lets you verify the data
generation logic looks right before adding networking on top of it.

Run:
    python sensor_simulator.py --anomaly-rate 0.02 --interval 1.0
"""

import argparse
import asyncio
import json
import math
import random
import time
from dataclasses import dataclass, asdict


@dataclass
class Reading:
    sensor_id: str
    sensor_type: str
    value: float
    timestamp: float
    is_anomaly: bool  # ground truth label, for later precision/recall scoring


class SensorSimulator:
    """Base class -- each sensor type overrides generate_value()."""

    def __init__(self, sensor_id: str, sensor_type: str, anomaly_rate: float):
        self.sensor_id = sensor_id
        self.sensor_type = sensor_type
        self.anomaly_rate = anomaly_rate
        self.start_time = time.time()

    def generate_value(self) -> tuple[float, bool]:
        raise NotImplementedError

    def next_reading(self) -> Reading:
        value, is_anomaly = self.generate_value()
        return Reading(
            sensor_id=self.sensor_id,
            sensor_type=self.sensor_type,
            value=round(value, 2),
            timestamp=time.time(),
            is_anomaly=is_anomaly,
        )


class TemperatureSensor(SensorSimulator):
    """Slow day/night sine wave + noise. Anomaly = sudden spike or drop."""

    def generate_value(self) -> tuple[float, bool]:
        elapsed = time.time() - self.start_time
        baseline = 22 + 5 * math.sin(elapsed / 60)  # ~1 min cycle for demo speed
        noise = random.gauss(0, 0.3)
        value = baseline + noise

        if random.random() < self.anomaly_rate:
            value += random.choice([-1, 1]) * random.uniform(15, 25)
            return value, True
        return value, False


class TrafficSensor(SensorSimulator):
    """Requests/sec with rush-hour-style peaks. Anomaly = burst or flatline."""

    def generate_value(self) -> tuple[float, bool]:
        elapsed = time.time() - self.start_time
        baseline = 50 + 30 * max(0, math.sin(elapsed / 45))
        noise = random.gauss(0, 4)
        value = max(0, baseline + noise)

        if random.random() < self.anomaly_rate:
            if random.random() < 0.5:
                return value * random.uniform(4, 8), True   # burst
            return 0.0, True                                 # flatline
        return value, False


class ThroughputSensor(SensorSimulator):
    """Steady baseline with occasional dips. Anomaly = sharp drop."""

    def generate_value(self) -> tuple[float, bool]:
        baseline = 100
        noise = random.gauss(0, 2)
        value = baseline + noise

        if random.random() < self.anomaly_rate:
            value *= random.uniform(0.05, 0.2)
            return value, True
        return value, False


async def run_sensor(sensor: SensorSimulator, interval: float):
    while True:
        reading = sensor.next_reading()
        print(json.dumps(asdict(reading)))
        await asyncio.sleep(interval)


async def main(anomaly_rate: float, interval: float):
    sensors = [
        TemperatureSensor("temp-01", "temperature", anomaly_rate),
        TrafficSensor("traffic-01", "traffic", anomaly_rate),
        ThroughputSensor("throughput-01", "network_throughput", anomaly_rate),
    ]
    await asyncio.gather(*(run_sensor(s, interval) for s in sensors))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sensor simulator")
    parser.add_argument("--anomaly-rate", type=float, default=0.02,
                         help="Probability [0-1] a reading is anomalous")
    parser.add_argument("--interval", type=float, default=1.0,
                         help="Seconds between readings per sensor")
    args = parser.parse_args()

    try:
        asyncio.run(main(args.anomaly_rate, args.interval))
    except KeyboardInterrupt:
        print("\nStopped.")
