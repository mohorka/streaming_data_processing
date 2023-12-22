import argparse
import logging
import re
import time
from dataclasses import dataclass
from multiprocessing.pool import ThreadPool as Pool
from pathlib import Path
from typing import List, Optional, Tuple

import pandas as pd
import requests
import yaml
from bs4 import BeautifulSoup


@dataclass
class CrawlerConfig:
    channel_name: str
    output_dir: str
    delay: int
    working_time: int
    verbose: bool


@dataclass
class Content:
    text: str
    has_media: bool
    timestamp: str


class DummyContentCrawler:
    def __init__(self) -> None:
        self.current_text = ""
        self.current_media = False
        self.current_media_amount = -1
        self.current_timestamp = ""

    def get_HTML_content(
        self, html: str, channel_name: str
    ) -> Tuple[Optional[Content], float]:
        soup = BeautifulSoup(html, "html.parser")

        time_content = soup.find_all("time", {"class": "time"})
        time_content = time_content[-1].attrs["datetime"]
        if time_content == self.current_timestamp:
            logging.info(f"Still no updates for {channel_name}")
            return None, -1
        self.current_timestamp = time_content

        text_content = soup.find_all(
            "div", {"class": "tgme_widget_message_text js-message_text"}
        )
        text_content = text_content[-1].text
        text_content = text_content.lower()
        self.current_text = text_content

        video_content_len = len(soup.find_all(
            "video", {"class": "tgme_widget_message_video js-message_video"}
        ))
        poll_content_len = len(soup.find_all(
            "div", {"class": "tgme_widget_message_poll_type"}
        ))
        photo_content_len = len(soup.find_all("div", {"class": "tgme_widget_message_photo"}))

        current_media_sum = video_content_len + poll_content_len + photo_content_len
        if current_media_sum > self.current_media_amount:
            self.current_media_amount = current_media_sum
            self.current_media = True
            content = Content(
                text=text_content,
                has_media=True,
                timestamp=time_content,
            )
        else:
            self.current_media = False
            content = Content(
                text=text_content,
                has_media=False,
                timestamp=time_content,
            )

        return content, time.time()


def _save_content_parquet(
    content: Content, channel_name: str, path_to_data: Path, timestamp: float, verbose: bool = False
):
    data = {
        "timestamp": content.timestamp,
        "source": channel_name,
        "text": content.text,
        "has_media": content.has_media,
    }
    if verbose:
        logging.info(f"Ready to write message: {data}\n")

    df = pd.DataFrame([data])
    path_to_file = path_to_data / f"{channel_name}_{str(timestamp)}.gzip"
    df.to_parquet(path_to_file, compression="gzip")


def get_new_message(crawler_config: CrawlerConfig):
    dummy_crawler = DummyContentCrawler()
    url = f"https://t.me/s/{crawler_config.channel_name}?before=-1"
    start_time = time.time()
    while time.time() - start_time <= crawler_config.working_time:
        response = requests.get(url)
        html = response.text
        content, getting_time = dummy_crawler.get_HTML_content(
            html=html,
            channel_name=crawler_config.channel_name,
        )
        if content is not None:
            time_diff = getting_time - start_time
            _save_content_parquet(
                content=content,
                channel_name=crawler_config.channel_name,
                path_to_data=crawler_config.output_dir,
                timestamp=time_diff,
                verbose=crawler_config.verbose,
            )
        time.sleep(crawler_config.delay)


def get_crawlers_configs(config_path: str, verbose: bool = False) -> List[CrawlerConfig]:
    """Get crawlers configs from global config.

    Args:
        config_path (str): Path to crowlers config. Config must be yaml.
        verbose (bool): Add more logs during crawling. Default: false. 

    Returns:
        List[CrawlerConfig]: List of configs.
    """
    with open(config_path) as f:
        crawlers_config = yaml.safe_load(f)

    configs: List[CrawlerConfig] = []
    for config in crawlers_config:
        output_dir = Path(config["output_dir"])
        output_dir.mkdir(parents=True, exist_ok=True)
        crawler_config = CrawlerConfig(
            channel_name=config["channel_name"],
            output_dir=output_dir,
            delay=config["delay"],
            working_time=config["working_time"],
            verbose=verbose,
        )
        configs.append(crawler_config)
    return configs


def _parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-p",
        "--config_path",
        type=str,
        default="spark_task/crawling_config.yaml",
        help="Path to crawlers config. Must be .yaml",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action='store_true',
        help="Flag for show additional logs.",
    )
    args = parser.parse_args()
    return args


def main():
    logging.basicConfig(level=logging.INFO)
    args = _parse_args()
    crawlers_config = get_crawlers_configs(args.config_path, args.verbose)
    with Pool() as pool:
        pool.map(get_new_message, crawlers_config)


if __name__ == "__main__":
    main()
