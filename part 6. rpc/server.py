"""Этот код будет выполняться на стороне бэкенда виртуализации."""
import asyncio
from functools import partial

import aio_pika


async def consumer(
    msg: aio_pika.IncomingMessage,
    channel: aio_pika.RobustChannel,
):
    # используем контекстный менеджер для ack'а сообщения
    async with msg.process():
        print("Поступил запрос на создание виртуалок...")

        # Симулируем поднятие виртуалки
        await asyncio.sleep(3)

        # Проверяем, требует ли сообщение ответа
        if msg.reply_to:
            # Отправляем ответ в default exchange
            await channel.default_exchange.publish(
                message=aio_pika.Message(
                    body="Виртуалки созданы, как и просили :)".encode(),
                    correlation_id=msg.correlation_id,
                ),
                routing_key=msg.reply_to,  # самое важное
            )
            print("Отправили ответ, что всё OK!")


async def main():
    connection = await aio_pika.connect_robust("amqp://guest:guest@127.0.0.1/")

    queue_name = "test"

    async with connection:
        channel = await connection.channel()
        queue = await channel.declare_queue(queue_name)
        # через partial прокидываем в наш обработчик сам канал
        await queue.consume(partial(consumer, channel=channel))

        try:
            await asyncio.Future()
        except Exception:
            pass


if __name__ == "__main__":
    asyncio.run(main())
