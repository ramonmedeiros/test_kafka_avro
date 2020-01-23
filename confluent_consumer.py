from confluent_kafka import Consumer, KafkaException
import os, sys

import avro.schema
import avro.io
import io

KAFKA_HOST = os.environ["KAFKA_HOST"]
KAFKA_USER = os.environ["KAFKA_USER"]
KAFKA_PASSWORD = os.environ["KAFKA_PASSWORD"]
KAFKA_TOPIC = os.environ["KAFKA_TOPIC"]
SCHEMA_PATH = "test.avro"

class KafkaConsumer:
    def __init__(self, topic=KAFKA_TOPIC):
        self.consumer = Consumer({
                                 "group.id":"consumer",
                                 "auto.offset.reset":'earliest',
                                 "bootstrap.servers":KAFKA_HOST,
                                 "sasl.mechanism":'PLAIN',
                                 "security.protocol":'SASL_SSL',
                                 "sasl.username":KAFKA_USER,
                                 "sasl.password":KAFKA_PASSWORD,
                                 })
        schema = avro.schema.Parse(open(SCHEMA_PATH).read())
        self.reader = avro.io.DatumReader(schema)


    @staticmethod
    def print_assignment(consumer, partitions):
        print('Assignment:', partitions)

    def deserializer(self, msg):
        bytes_reader = io.BytesIO(msg)
        decoder = avro.io.BinaryDecoder(bytes_reader)
        return self.reader.read(decoder)

    def read(self):
        self.consumer.subscribe([KAFKA_TOPIC], on_assign=self.print_assignment)
        # Read messages from Kafka, print to stdout
        try:
            while True:
                msg = self.consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    raise KafkaException(msg.error())
                else:
                    # Proper message
                    sys.stderr.write('%% %s [%d] at offset %d with key %s:\n' %
                                     (msg.topic(), msg.partition(), msg.offset(),
                                      str(msg.key())))
                    print(self.deserializer(msg.value()))

        except KeyboardInterrupt:
            sys.stderr.write('%% Aborted by user\n')

        finally:
            # Close down consumer to commit final offsets.
            self.consumer.close()
KafkaConsumer().read()
