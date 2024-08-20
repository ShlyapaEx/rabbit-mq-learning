import sys

import pika

# Генерация сообщений
messages_count = int(sys.argv[1])
messages = []

for i in range(messages_count):
    messages.append(i + 1)


# Подключение к RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters(host="127.0.0.1"))
channel = connection.channel()

# Создание очереди со случайным названием
queue = channel.queue_declare(queue="", exclusive=True)


# Создание обменника
channel.exchange_declare(exchange="exchange_name", exchange_type="topic")

# Связывание очереди и обменника
channel.queue_bind(queue=queue.method.queue, exchange="exchange_name")

for message in messages:
    # severity = random.choice(severities)
    routing_key = " ".join(sys.argv[2:])
    # Отправление сообщения в очередь
    channel.basic_publish(
        exchange="exchange_name", routing_key=routing_key, body=str(message)
    )
    print(f'Successfully sent a message: "{message}" with routing_key: "{routing_key}"')

# Закрытие подключения к RabbitMQ
connection.close()
