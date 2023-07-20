import sys

import pika
from pika.spec import PERSISTENT_DELIVERY_MODE


# Генерация сообщений
messages_count = int(sys.argv[1])
messages = []

start = 0
for _ in range(messages_count):
    start += 1
    messages.append(start)

# Подключение к RabbitMQ
connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='127.0.0.1'))
channel = connection.channel()

# Создание очереди
channel.queue_declare(queue='first_queue_name', durable=True)

for message in messages:

    # Отправление сообщения в очередь
    channel.basic_publish(exchange='', routing_key='first_queue_name',
                          body=str(message),
                          properties=pika.BasicProperties(
                              delivery_mode=PERSISTENT_DELIVERY_MODE))
    print(f'Successfully sent a message: "{message}"')

# Закрытие подключения к RabbitMQ
connection.close()
