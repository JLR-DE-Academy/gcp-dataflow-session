import apache_beam as beam
import argparse
import json
import logging
from apache_beam.options.pipeline_options import PipelineOptions


# TODO (change to yours)
PROJECT_ID = "jlr-dl-cat-training"
BIGQUERY_DATASET_NAME = "2022_temp_jlr_de_dataflow_<your-name>"
PUBSUB_SUBSCRIPTION_ID = "temp-2022-jlr-de-subscription-dataflow-<your-name>"

INPUT_SUBSCRIPTION = (
    f"projects/jlr-dl-cat-training/subscriptions/{PUBSUB_SUBSCRIPTION_ID}"
)
OUTPUT_TABLE = f"{PROJECT_ID}:{BIGQUERY_DATASET_NAME}.bike_trips_streaming"

parser = argparse.ArgumentParser()
args, beam_args = parser.parse_known_args()
beam_options = PipelineOptions(beam_args, streaming=True)


def run():
    with beam.Pipeline(options=beam_options) as p:
        (
            p
            | "Read from Pub/Sub"
            >> beam.io.ReadFromPubSub(subscription=INPUT_SUBSCRIPTION)
            | "Decode" >> beam.Map(lambda x: x.decode("utf-8"))
            | "Parse JSON" >> beam.Map(json.loads)
            | "Write to Table"
            >> beam.io.WriteToBigQuery(
                OUTPUT_TABLE,
                schema="trip_id:STRING,start_date:TIMESTAMP,start_station_id:STRING,bike_number:STRING,duration_sec:INTEGER",
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            )
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
