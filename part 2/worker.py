import os
import sys
import time

import pika
from pika.spec import Basic, BasicProperties
from pika.adapters.blocking_connection import BlockingChannel


def callback(channel: BlockingChannel, method: Basic.Deliver,
             properties: BasicProperties, body: bytes):
    """
        Функция, которую будем вызывать при получении сообщения.
    """
    print(f" [x] Received {body.decode()}")
    time.sleep(int(body.decode()))
    print(" [x] Done")
    channel.basic_ack(delivery_tag=method.delivery_tag)
    print(" [x] Acknowledged")


def main():
    # Подключение к RabbitMQ
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='127.0.0.1'))
    channel = connection.channel()

    # Создание очереди
    channel.queue_declare(queue='first_queue_name', durable=True)

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue='first_queue_name',
                          on_message_callback=callback)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
