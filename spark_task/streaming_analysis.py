import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def _parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-t",
        "--topic",
        type=str,
        required=True,
        help="Topic to search in data stream.",
    )
    parser.add_argument(
        "-o",
        "--output_dir",
        type=str,
        default="/output/",
        help="Path to directory with parquet files. Default: %(default)s",
    )
    args = parser.parse_args()
    return args


def main() -> None:
    """Run Spark Streaming and find given topic in messages."""
    args = _parse_args()
    spark = SparkSession.builder.appName("StreamingParquet").getOrCreate()

    df = spark.read.format("parquet").load(args.output_dir)
    streaming_df = (
        spark.readStream.format("parquet").schema(df.schema).load(args.output_dir)
    )
    topic = args.topic.lower()
    filtered_stream = streaming_df.filter(col("text").like(f"%{topic}%"))
    query = filtered_stream.writeStream.outputMode("append").format("console").start()
    query.awaitTermination()
