# Streaming Data Processing
Repo with homeworks for Streaming Data Processing 2023 course
## Prerequisites
Make sure you have `poetry` installed and `docker` daemon running on your host.
## Task1: Kafka Consumers/Producers
### How to install?
Run following after cloning repo:
1. Create any virtual enviroment (setup docker container or simply run `python3 -m venv .venv`).
2. Inside virtual enviroment: `poetry install`.
### How to use?
- *Optional*: Configure sensors simulation via `kafka_task/sensors_config.yaml`. You may add *new sensor*, set its *name*, *type*, *writing delay* and *distribution* from which measurements will be sampled. Make sure that chosen distribution is available in `kafka_task/config.py`. Optionally you may add to this config any distribution you like.
- From repo root run `docker compose up` to setup Kafka.
- `poetry run producers -t your_kafka_topic_name -p path_to_sensors_config` for start simulation of sending sensors measurements.
- `poetry run consumers -t your_kafka_topic_name -p path_to_sensors_config` for start aggregate measurements from kafka. You may specify time in seconds to delay printing next statistics report by add flag `-d seconds_for_delay_between_statistics_printing`. (its 20 seconds by default).