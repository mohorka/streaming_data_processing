import argparse
import json
import logging
import time
from multiprocessing.pool import ThreadPool as Pool
from typing import List

import yaml

from kafka_task.config import (
    KAFKA_HOSTS,
    distribution_mapping
)
from kafka import KafkaProducer


class Sensor:
    def __init__(
        self,
        name: str,
        type: str,
        delay: int,
        working_time: int,
        distribution: str,
        topic: str,
    ) -> None:
        self.name = name
        self.type = type
        self.delay = delay
        self.working_time = working_time
        self.topic = topic
        try:
            distribution = distribution_mapping[distribution]
        except KeyError:
            raise ValueError(
                f"Wrong distribution set for sensor {name}.\n"
                "Availible distributions are"
                f"{list(distribution_mapping.keys())}."
            )
        self.distribution = distribution


def _parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-p",
        "--config_path",
        type=str,
        default="kafka_task/sensors_config.yaml",
        help="Path to sensors config. Must be .yaml",
    )
    parser.add_argument(
        "-t",
        "--topic",
        type=str,
        default="test_topic",
        help="Name of kafka topic",
    )
    args = parser.parse_args()
    return args


def get_measurement(sensor: Sensor) -> None:
    """Simulate measurement and send result to kafka.

    Args:
        sensor (Sensor): object for sensor simulation.
    """
    producer = KafkaProducer(bootstrap_servers=KAFKA_HOSTS)
    started = time.time()
    while time.time() < started + sensor.working_time:
        measurement = str(sensor.distribution(size=None))
        report_time = time.strftime(
            "%Y-%m-%d %H:%M:%S",
            time.gmtime(time.time())
            )
        content = {
            "name": sensor.name,
            "type": sensor.type,
            "measurement": measurement,
            "report_time": report_time,
        }
        content = json.dumps(content).encode("utf-8")
        producer.send(
            topic=sensor.topic,
            value=content,
        )
        time.sleep(sensor.delay)

    producer.close()


def get_sensors(config_path: str, topic: str) -> List[Sensor]:
    """Get sensors from config.

    Args:
        config_path (str): Path to sensors config. Config must be yaml.
        topic (str): Kafka topic for sending messages.

    Returns:
        List[Sensor]: List of sensors.
    """
    with open(config_path) as f:
        sensors_config = yaml.safe_load(f)

    sensors: List[Sensor] = []
    for sensor_config in sensors_config:
        sensor = Sensor(
            name=sensor_config["sensor_name"],
            type=sensor_config["type"],
            delay=sensor_config["delay"],
            working_time=sensor_config["working_time"],
            distribution=sensor_config["distribution"],
            topic=topic,
        )
        sensors.append(sensor)
    return sensors


def main():
    logging.basicConfig(level=logging.INFO)
    args = _parse_args()
    sensors = get_sensors(args.config_path, args.topic)
    with Pool() as pool:
        pool.map(get_measurement, sensors)


if __name__ == "__main__":
    main()
