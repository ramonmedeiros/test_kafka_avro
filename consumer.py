from kafka import KafkaConsumer
from kafka.errors import KafkaError
import os


KAFKA_HOST = os.environ["KAFKA_HOST"]
KAFKA_USER = os.environ["KAFKA_USER"]
KAFKA_PASSWORD = os.environ["KAFKA_PASSWORD"]

class Consumer:
    def __init__(self, topic="test-topic"):
        self.consumer = KafkaConsumer(topic,
                                      group_id="consumer",
                                      auto_offset_reset='earliest',
                                      bootstrap_servers=[KAFKA_HOST],
                                      sasl_mechanism='PLAIN',
                                      security_protocol='SASL_SSL',
                                      sasl_plain_username=KAFKA_USER,
                                      sasl_plain_password=KAFKA_PASSWORD)


    def read(self):
        import pdb;pdb.set_trace()
        for msg in self.consumer:
            print(msg)

Consumer().read()
