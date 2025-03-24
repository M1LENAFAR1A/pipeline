import json
import logging
import sys

import pika

sys.path.append('/opt/airflow')
from model.producer import Producer

logger = logging.getLogger(__name__)


class RabbitMQProducer(Producer):
    def __init__(self, channel: str,
                 host: str,
                 port: str,
                 user: str,
                 passwd: str):
        super().__init__(channel)

        self.channel = channel
        self.host = host
        self.port = port
        self.user = user
        self.passwd = passwd

    def publish_message(self, message: dict):
        credentials = pika.PlainCredentials(self.user, self.passwd)
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host,
                                                                       port=self.port,
                                                                       credentials=credentials))
        channel = connection.channel()
        channel.queue_declare(queue=self.channel, durable=True)

        logger.info(f"Sending message {message} to rabbitmq channel {self.channel}")
        channel.basic_publish(exchange='',
                              routing_key=self.channel,
                              body=json.dumps(message))

        connection.close()
