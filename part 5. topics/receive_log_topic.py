import os
import sys

import pika
from pika.spec import Basic, BasicProperties
from pika.adapters.blocking_connection import BlockingChannel


def callback(
    channel: BlockingChannel,
    method: Basic.Deliver,
    properties: BasicProperties,
    body: bytes,
):
    """
    Функция, которую будем вызывать при получении сообщения.
    """
    print(f" [x] Received {method.routing_key} {body.decode()}", flush=True)


def main():
    # Подключение к RabbitMQ
    connection = pika.BlockingConnection(pika.ConnectionParameters(host="127.0.0.1"))
    channel = connection.channel()

    # Создание очереди
    queue = channel.queue_declare(queue="", exclusive=True)

    # Создание обменника
    channel.exchange_declare(exchange="exchange_name", exchange_type="topic")

    # Связывание очереди и обменника несколькими связями
    for topic_filter in sys.argv[1:]:
        channel.queue_bind(
            queue=queue.method.queue, exchange="exchange_name", routing_key=topic_filter
        )

    channel.basic_consume(queue="", auto_ack=True, on_message_callback=callback)

    print(" [*] Waiting for logs. To exit press CTRL+C")
    channel.start_consuming()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Interrupted")
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
