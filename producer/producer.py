import aiohttp
import asyncio
from config import BTC_WS_URL, COUNTER_TOPIC, AGGREGATOR_TOPIC, TRANSACTION_TOPIC, KAKFA_BS, KAFKA_VALUE_SERIALIZER
from kafka import KafkaProducer


producer = KafkaProducer(bootstrap_servers=KAKFA_BS, value_serializer=KAFKA_VALUE_SERIALIZER)


async def main():
    session = aiohttp.ClientSession()
    async with session.ws_connect(BTC_WS_URL, ssl=False) as ws:

        await ws.send_str("""{"op":"unconfirmed_sub"}""")

        async for msg in ws:
            print('Message received from server:', msg.json())

            tx_block = msg.json()
            producer.send(COUNTER_TOPIC, tx_block)
            producer.send(AGGREGATOR_TOPIC, tx_block)
            producer.send(TRANSACTION_TOPIC, tx_block)

            if msg.type in (aiohttp.WSMsgType.CLOSED,
                            aiohttp.WSMsgType.ERROR):
                producer.close()
                break


if __name__ == '__main__':
    try:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        print('Exiting!')
        raise SystemExit(0)