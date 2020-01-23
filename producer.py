from kafka import KafkaProducer
from kafka.errors import KafkaError
import os

import avro.schema
import io, random
from avro.io import DatumWriter

KAFKA_HOST = os.environ["KAFKA_HOST"]
KAFKA_USER = os.environ["KAFKA_USER"]
KAFKA_PASSWORD = os.environ["KAFKA_PASSWORD"]
KAFKA_TOPIC = os.environ["KAFKA_TOPIC"]
SCHEMA_PATH = "test.avro"

class Producer:
    def __init__(self):
        self.producer = KafkaProducer(bootstrap_servers=[KAFKA_HOST],
                                      sasl_mechanism='PLAIN',
                                      security_protocol='SASL_SSL',
                                      sasl_plain_username=KAFKA_USER,
                                      sasl_plain_password=KAFKA_PASSWORD,
                                      compression_type='gzip',
                                      value_serializer=self.avro_format
                                      )
        schema = avro.schema.Parse(open(SCHEMA_PATH).read())
        self.writer = avro.io.DatumWriter(schema)

    def send(self, msg, topic=KAFKA_TOPIC):
        future = self.producer.send(topic, msg)

        # Block for 'synchronous' sends
        try:
            record_metadata = future.get(timeout=10)
        except KafkaError as e:
            print(f"Error: {e}")

    def avro_format(self, message):
        bytes_writer = io.BytesIO()
        encoder = avro.io.BinaryEncoder(bytes_writer)
        self.writer.write(message, encoder)
        raw_bytes = bytes_writer.getvalue()

        return raw_bytes

Producer().send({"name": "test",
                 "favorite_number": 0,
                 "favorite_color": "blue"})

