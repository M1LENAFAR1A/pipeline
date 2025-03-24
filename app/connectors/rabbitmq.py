import json
import os
import time
from logging import Logger

import pika
import pika.exceptions as pika_exceptions

from typing import Tuple

from app.connectors.postgres import PostgresConnector
from app.models.queue_connector import QueueConnector
from app.models.transformation_pipeline import TransformationPipeline
from app.core.config import settings

MAX_RETRY = settings.MAX_RETRY


class RabbitMQConnector(QueueConnector):
    def __init__(self,
                 queue: str,
                 host: str,
                 port: str,
                 user: str,
                 pwd: str,
                 postgres_connector: PostgresConnector,
                 transformation_pipeline: TransformationPipeline,
                 logger: Logger):
        super().__init__(queue)

        # connection settings / rabbit mq queue
        self.host = host
        self.port = port
        self.user = user
        self.pwd = pwd
        self.queue = queue

        # postgres connector
        self.postgres_connector = postgres_connector

        # transformation pipeline
        self.transformation_pipeline = transformation_pipeline
        self.logger = logger

    def connect(self):
        credentials = pika.PlainCredentials(self.user, self.pwd)
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host,
                                                                       port=self.port,
                                                                       credentials=credentials))
        self.channel = connection.channel()

    def create_queue_if_not_exists(self):
        try:
            self.channel.queue_declare(queue=self.queue, passive=True)
            # uses passive just to check if it already exists
            # use durable=True to persist the queue even with RabbitMq restarts
            self.logger.info(f"Queue {self.queue} already exists")
        except (pika_exceptions.ChannelClosedByBroker, pika_exceptions.ChannelWrongStateError) as e:
            # connect again because the exception closes the channel
            self.connect()
            # create the queue with durable=true for persistance
            self.channel.queue_declare(queue=self.queue, durable=True)
            self.logger.info(f"Queue {self.queue} created")

    def start_consuming(self):
        # uses basic.qos protocol to tell RabbitMq to not give more than a message to a worker at a time
        # don't dispatch a new message to a worker until it has processed and acknowledge the previous one
        # instead, it will dispatch it to the next worker that is still not busy
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(queue=self.queue,
                                   auto_ack=False,
                                   # manual message acknowledgements, turn False to manually acknowledge
                                   on_message_callback=self.callback)

        self.logger.info(' [*] Waiting for messages. To exit press CTRL+C')
        self.channel.start_consuming()

    def callback(self, ch, method, properties, body: bytes):  # body: bytes
        self.logger.info(f" [x] Received message {body}")

        object_path = body.decode("utf-8")
        dict_object_path = json.loads(object_path)
        file_path = dict_object_path.get('path')
        retry = dict_object_path.get('retry')
        source = dict_object_path.get('source')
        if source != 'Inaturalist' and source != 'GBIF':
            self.logger.warning(f"Source {source} not valid for transformation yet.")
            transformation_result = False, ""
        else:
            transformation_result: Tuple[bool, str] = self.transformation_pipeline.run(file_path=file_path)

        if not transformation_result[0]:
            self.logger.error("Error in the transformation, inserting again in the queue")
            time.sleep(5)
            retry += 1
            if retry <= MAX_RETRY:
                dict_object_path.update({'retry': retry})
                self.channel.basic_publish(exchange='',
                                           routing_key=self.queue,
                                           body=json.dumps(dict_object_path))
            else:
                self.logger.warning(f'Retried {MAX_RETRY} times. Stopping insertion in the queue')
                # TODO add a notification or write in a file to warn users
        else:
            self.logger.info("Finished transformation, inserting data in the database")
            transformed_file_path = transformation_result[1]
            self.logger.info(f"Transformed file path {transformed_file_path}")

            json_data = self.postgres_connector.open_csv_file(file_path=transformed_file_path)
            try:
                validated_date = self.postgres_connector.validate_data(source=dict_object_path.get('source'),
                                                                       json_data=json_data)
                self.postgres_connector.insert_in_table(source=dict_object_path.get('source'),
                                                        transformed_values=validated_date)
                self.logger.info("Insertion completed successfully.")
                os.remove(transformed_file_path)
            except Exception as e:
                self.logger.error(f"An error occurred: {e}")
                retry += 1
                if retry <= MAX_RETRY:
                    self.logger.error("Error in the transformation, inserting again in the queue")
                    dict_object_path.update({'retry': retry})
                    self.channel.basic_publish(exchange='',
                                               routing_key=self.queue,
                                               body=json.dumps(dict_object_path))
                else:
                    self.logger.warning(f'Retried {MAX_RETRY} times. Stopping insertion in the queue')
                    # TODO add a notification or write in a file to warn users

        ch.basic_ack(delivery_tag=method.delivery_tag)

    def publish_message(self, message: dict):
        credentials = pika.PlainCredentials(self.user, self.pwd)
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host,
                                                                       port=self.port,
                                                                       credentials=credentials))
        channel = connection.channel()
        channel.queue_declare(queue=self.channel, durable=True)
        # use durable=True to persist the queue even with RabbitMq restarts

        self.logger.info(f"Sending message {message} to rabbitmq channel {self.channel}")
        channel.basic_publish(exchange='',
                              routing_key=self.queue,
                              body=json.dumps(message),
                              properties=pika.BasicProperties(
                                  delivery_mode=pika.DeliveryMode.Persistent,
                              ))

        # using pika delivery mode persistent the messages will be persisted even with rabbitmq restarts
        # saves message to disk
        connection.close()
