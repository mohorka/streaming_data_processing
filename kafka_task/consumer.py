import argparse
import json
import logging
import time
from dataclasses import dataclass
from multiprocessing.pool import ThreadPool as Pool

import numpy as np
import pandas as pd

from kafka import KafkaConsumer
from kafka_task.config import KAFKA_HOSTS


@dataclass
class ConsumerConfig:
    topic: str
    aggregate_by_type: bool
    consume_delay: int


def _parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-p",
        "--config_path",
        type=str,
        default="sensors_config.yaml",
        help="Path to sensors config. Must be .yaml",
    )
    parser.add_argument(
        "-t",
        "--topic",
        type=str,
        default="test_topic",
        help="Name of kafka topic",
    )
    parser.add_argument(
        "-d",
        "--delay",
        type=int,
        default=20,
        help="Consumer delay. Must be in seconds.",
    )
    args = parser.parse_args()
    return args


def consume_content(consumer_config: ConsumerConfig) -> None:
    """
    Consume content from kafka topic and calculate means.
    Args:
        consumer_config (ConsumerConfig): config with kafka topic,
        delay for calculating statistics
        and aggregation type (may aggregate by sensor's type or name).
    """
    consumer_type = "typed" if consumer_config.aggregate_by_type else "named"
    consumer = KafkaConsumer(
        consumer_config.topic,
        group_id=f"my-group-{consumer_type}",
        bootstrap_servers=KAFKA_HOSTS,
    )
    start_time = time.time()
    measurements = {}
    for event in consumer:
        value = event.value.decode("utf-8")
        value = json.loads(value)
        measurement = value["measurement"]
        if consumer_config.aggregate_by_type:
            key = value["type"]
        else:
            key = value["name"]

        if key not in measurements:
            measurements[key] = []

        measurements[key].append(float(measurement))

        delta = time.time() - start_time
        if delta >= consumer_config.consume_delay:
            for k, v in measurements.items():
                mean = np.average(v)
                measurements[k] = []
                measurements[k].append(mean)
            df = pd.DataFrame.from_dict(measurements, orient="index")
            logging.info(
                f"Aggregated for {consumer_type}:\n{df.to_markdown()}\n"
                )
            start_time = time.time()


def main():
    logging.basicConfig(level=logging.INFO)
    args = _parse_args()
    consumers_config = [
        ConsumerConfig(
            topic=args.topic, consume_delay=args.delay, aggregate_by_type=True
        ),
        ConsumerConfig(
            topic=args.topic, consume_delay=args.delay, aggregate_by_type=False
        ),
    ]

    with Pool() as pool:
        pool.map(consume_content, consumers_config)


if __name__ == "__main__":
    main()
