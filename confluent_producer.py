from confluent_kafka import Producer
import os, sys

import avro.schema
import io, random
from avro.io import DatumWriter

KAFKA_HOST = os.environ["KAFKA_HOST"]
KAFKA_USER = os.environ["KAFKA_USER"]
KAFKA_PASSWORD = os.environ["KAFKA_PASSWORD"]
KAFKA_TOPIC = os.environ["KAFKA_TOPIC"]
SCHEMA_PATH = "test.avro"

class KafkaProducer:
    def __init__(self):
        self.producer = Producer({
                                 "bootstrap.servers":KAFKA_HOST,
                                 "sasl.mechanism":'PLAIN',
                                 "security.protocol":'SASL_SSL',
                                 "sasl.username":KAFKA_USER,
                                 "sasl.password":KAFKA_PASSWORD
                                 })

        schema = avro.schema.Parse(open(SCHEMA_PATH).read())
        self.writer = avro.io.DatumWriter(schema)

    def avro_format(self, message):
        bytes_writer = io.BytesIO()
        encoder = avro.io.BinaryEncoder(bytes_writer)
        self.writer.write(message, encoder)
        raw_bytes = bytes_writer.getvalue()

        return raw_bytes

    def send(self, msg, topic=KAFKA_TOPIC):
        msg = self.avro_format(msg)
        try:
            self.producer.produce(topic, msg, callback=self.delivery_callback)
        except Exception as e:
            print(f"Error while sending message: {e}")
        self.producer.poll(0)
        self.producer.flush()

    @staticmethod
    def delivery_callback(err, msg):
        if err:
            sys.stderr.write('%% Message failed delivery: %s\n' % err)
        else:
            sys.stdout.write('%% Message delivered to %s [%d] @ %d\n' %
                             (msg.topic(), msg.partition(), msg.offset()))


KafkaProducer().send({"name": "test",
                      "favorite_number": 0,
                      "favorite_color": "blue"})

