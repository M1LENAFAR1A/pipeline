import json
import time

import pika

if __name__ == '__main__':
    credentials = pika.PlainCredentials('admin', 'admin')
    """connection = pika.BlockingConnection(pika.ConnectionParameters(host='192.168.30.21',
                                                                   port=30673,
                                                                   credentials=credentials))"""
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost',
                                                                   port=31074,
                                                                   credentials=credentials))
    channel = connection.channel()
    channel.queue_declare(queue='environbit', durable=True)

    message_object = {
        'path': 'inaturalist_data/atropelada_data/2024/12/06/atropelada_observations_160118.csv',
        'source': 'Inaturalist',
        'keyword': 'atropelada',
        'extraction_date': time.time_ns(),
        'retry': 0
    }

    channel.basic_publish(exchange='',
                          routing_key='environbit',
                          body=json.dumps(message_object))

    print(" [x] Sent 'Hello World!'")

    connection.close()
