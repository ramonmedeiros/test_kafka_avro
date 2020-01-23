from kafka import KafkaConsumer
from kafka.errors import KafkaError
import os

import avro.schema
import avro.io
import io

KAFKA_HOST = os.environ["KAFKA_HOST"]
KAFKA_USER = os.environ["KAFKA_USER"]
KAFKA_PASSWORD = os.environ["KAFKA_PASSWORD"]
KAFKA_TOPIC = os.environ["KAFKA_TOPIC"]
SCHEMA_PATH = "test.avro"

class Consumer:
    def __init__(self, topic=KAFKA_TOPIC):
        self.consumer = KafkaConsumer(topic,
                                      group_id="consumer",
                                      auto_offset_reset='earliest',
                                      bootstrap_servers=[KAFKA_HOST],
                                      sasl_mechanism='PLAIN',
                                      security_protocol='SASL_SSL',
                                      sasl_plain_username=KAFKA_USER,
                                      sasl_plain_password=KAFKA_PASSWORD,
                                      value_deserializer=self.deserializer)
        schema = avro.schema.Parse(open(SCHEMA_PATH).read())
        self.reader = avro.io.DatumReader(schema)

    def deserializer(self, msg):
        bytes_reader = io.BytesIO(msg)
        decoder = avro.io.BinaryDecoder(bytes_reader)
        return self.reader.read(decoder)

    def read(self):
        for msg in self.consumer:
            print(msg)

Consumer().read()
