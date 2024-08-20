"""Этот код будет выполняться на стороне бэкенда FastAPI."""

import asyncio

import aio_pika

RABBIT_REPLY = "amq.rabbitmq.reply-to"


async def consume_response(msg: aio_pika.IncomingMessage):
    print("Опа, получили ответ!")
    print(msg.body.decode())


async def main():
    connection = await aio_pika.connect_robust("amqp://guest:guest@127.0.0.1/")

    async with connection:
        channel = await connection.channel()

        callback_queue = await channel.get_queue(RABBIT_REPLY)

        # создаем asyncio.Queue для ответа
        rq = asyncio.Queue(maxsize=1)

        # сначала подписываемся
        consumer_tag = await callback_queue.consume(
            callback=consume_response,
            no_ack=True,  # еще один важный нюанс
        )
        print("Отправляем запрос на создание виртуалок...")
        # потом публикуем
        await channel.default_exchange.publish(
            message=aio_pika.Message(
                body="Эй, создай-ка мне виртуалок да побольше!".encode("utf-8"),
                reply_to=RABBIT_REPLY,  # указываем очередь для ответа
            ),
            routing_key="test",
        )

        # получаем ответ из asyncio.Queue
        response = await rq.get()
        print(response.body)

        # освобождаем RABBIT_REPLY
        await callback_queue.cancel(consumer_tag)


if __name__ == "__main__":
    asyncio.run(main())
