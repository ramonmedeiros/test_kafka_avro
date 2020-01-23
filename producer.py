from kafka import KafkaProducer
from kafka.errors import KafkaError
import os


KAFKA_HOST = os.environ["KAFKA_HOST"]
KAFKA_USER = os.environ["KAFKA_USER"]
KAFKA_PASSWORD = os.environ["KAFKA_PASSWORD"]

class Producer:
    def __init__(self):
        self.producer = KafkaProducer(bootstrap_servers=[KAFKA_HOST],
                                      sasl_mechanism='PLAIN',
                                      security_protocol='SASL_SSL',
                                      sasl_plain_username=KAFKA_USER,
                                      sasl_plain_password=KAFKA_PASSWORD,
                                      compression_type='gzip',
                                      )


    def send(self, msg, topic="test-topic"):
        future = self.producer.send(topic, msg)

        # Block for 'synchronous' sends
        try:
            record_metadata = future.get(timeout=10)
        except KafkaError as e:
            print(f"Error: {e}")

        # Successful result returns assigned partition and offset
        print(record_metadata.topic)
        print(record_metadata.partition)
        print(record_metadata.offset)


Producer().send(b"test")
