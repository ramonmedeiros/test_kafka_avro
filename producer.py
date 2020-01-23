from confluent_kafka import Producer
import os, sys


KAFKA_HOST = os.environ["KAFKA_HOST"]
KAFKA_USER = os.environ["KAFKA_USER"]
KAFKA_PASSWORD = os.environ["KAFKA_PASSWORD"]
KAFKA_TOPIC = os.environ["KAFKA_TOPIC"]

class KafkaProducer:
    def __init__(self):
        self.producer = Producer({
                                 "bootstrap.servers":KAFKA_HOST,
                                 "sasl.mechanism":'PLAIN',
                                 "security.protocol":'SASL_SSL',
                                 "sasl.username":KAFKA_USER,
                                 "sasl.password":KAFKA_PASSWORD
                                 })

    def send(self, msg, topic=KAFKA_TOPIC):

        def delivery_callback(err, msg):
            if err:
                sys.stderr.write('%% Message failed delivery: %s\n' % err)
            else:
                sys.stdout.write('%% Message delivered to %s [%d] @ %d\n' %
                                 (msg.topic(), msg.partition(), msg.offset()))

        try:
            self.producer.produce(topic, msg, callback=delivery_callback)
        except Exception as e:
            print(f"Error while sending message: {e}")
        self.producer.poll(0)
        self.producer.flush()

KafkaProducer().send(b"test")
