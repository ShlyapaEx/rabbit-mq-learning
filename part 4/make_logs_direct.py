import random
import sys

import pika

severities = ('info', 'warning', 'error')
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

# Создание очереди со случайным названием
queue = channel.queue_declare(queue='', exclusive=True)


# Создание обменника(?)
channel.exchange_declare(exchange='exchange_name',
                         exchange_type='direct')

# Связывание очереди и обменника(?)
channel.queue_bind(queue=queue.method.queue, exchange='exchange_name')

for message in messages:
    severity = random.choice(severities)
    # Отправление сообщения в очередь
    channel.basic_publish(exchange='exchange_name',
                          routing_key=severity,
                          body=str(message))
    print(
        f'Successfully sent a message: "{message}" with severity: "{severity}"')

# Закрытие подключения к RabbitMQ
connection.close()
