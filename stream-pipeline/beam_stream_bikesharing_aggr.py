import apache_beam as beam
import argparse
import json
import logging
from apache_beam.options.pipeline_options import PipelineOptions


# TODO (change to yours)
PROJECT_ID = "jlr-dl-cat-training"
BIGQUERY_DATASET_NAME = "2022_temp_jlr_de_dataflow_ben"
PUBSUB_SUBSCRIPTION_ID = "temp-2022-jlr-de-subscription-ben-aggr"

INPUT_SUBSCRIPTION= 'projects/jlr-dl-cat-training/subscriptions/temp-2022-jlr-de-subscription-ben-aggr'
OUTPUT_TABLE = f'{PROJECT_ID}:{BIGQUERY_DATASET_NAME}.bike_trips_streaming_sum_aggr'

parser = argparse.ArgumentParser()
args, beam_args = parser.parse_known_args()
beam_options = PipelineOptions(beam_args, streaming=True)

class BuildRecordFn(beam.DoFn):
    def process(self, element,  window=beam.DoFn.WindowParam):
        window_start = window.start.to_utc_datetime().isoformat()
        return [element + (window_start,)]  # [(a, b) + (c,)] >> [(a, b, c)]

def run():
    with beam.Pipeline(options=beam_options) as p:(
        p | "Read from Pub/Sub" >> beam.io.ReadFromPubSub(subscription=INPUT_SUBSCRIPTION)
        | 'Decode' >> beam.Map(lambda x: x.decode('utf-8'))
        | "Parse JSON" >> beam.Map(json.loads)
        | "UseFixedWindow" >> beam.WindowInto(beam.window.FixedWindows(60))
        | 'Group By User ID' >> beam.Map(lambda elem: (elem['start_station_id'], elem['duration_sec']))
        | 'Sum' >> beam.CombinePerKey(sum)
        | 'AddWindowEndTimestamp' >> (beam.ParDo(BuildRecordFn()))
        | 'Parse to JSON' >> beam.Map(lambda x : {'start_station_id': x[0],'sum_duration_sec':x[1],'window_timestamp':x[2]})
        | 'Write to Table' >> beam.io.WriteToBigQuery(OUTPUT_TABLE,
                        schema='start_station_id:STRING,sum_duration_sec:INTEGER,window_timestamp:TIMESTAMP',
                        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
    )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
