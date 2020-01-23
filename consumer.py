from kafka import KafkaConsumer
from kafka.errors import KafkaError
import os


KAFKA_HOST = os.environ["KAFKA_HOST"]
KAFKA_USER = os.environ["KAFKA_USER"]
KAFKA_PASSWORD = os.environ["KAFKA_PASSWORD"]
KAFKA_TOPIC = os.environ["KAFKA_TOPIC"]

class Consumer:
    def __init__(self, topic=KAFKA_TOPIC):
        self.consumer = KafkaConsumer(topic,
                                      group_id="consumer",
                                      auto_offset_reset='earliest',
                                      bootstrap_servers=[KAFKA_HOST],
                                      sasl_mechanism='PLAIN',
                                      security_protocol='SASL_SSL',
                                      sasl_plain_username=KAFKA_USER,
                                      sasl_plain_password=KAFKA_PASSWORD)


    def read(self):
        for msg in self.consumer:
            print(msg)

Consumer().read()
