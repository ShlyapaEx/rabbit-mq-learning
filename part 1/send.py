import pika


# Подключение к RabbitMQ
connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='127.0.0.1'))
channel = connection.channel()

# Создание очереди
channel.queue_declare(queue='first_queue_name')

# Отправление сообщения в очередь
channel.basic_publish(exchange='', routing_key='first_queue_name',
                      body='Hello World!')

print('Successfully sent a message!')

# Закрытие подключения к RabbitMQ
connection.close()
