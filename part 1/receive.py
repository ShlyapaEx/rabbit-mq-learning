import os
import sys

import pika


def main():
    # Подключение к RabbitMQ
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='127.0.0.1'))
    channel = connection.channel()

    # Создание очереди
    channel.queue_declare(queue='first_queue_name')

    # Функция, которую будем вызывать при получении сообщения
    def callback(channel, method, properties, body):
        print(f'Received message: "{body.decode()}"')

    channel.basic_consume(queue='first_queue_name',
                          auto_ack=True,
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
