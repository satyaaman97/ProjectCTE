import asyncio
import json
import logging
from zoneinfo import ZoneInfo
from datetime import datetime
import websockets
from aiokafka import AIOKafkaProducer

# Configuration
MAX_QUEUE_SIZE = 10000  # Adjust based on your RAM/Needs
SYMBOL = "btcusdt"

async def ingest_binance_data(data_queue: asyncio.Queue):
    url = f"wss://fstream.binance.com/ws/{SYMBOL}@aggTrade"
    IST = ZoneInfo("Asia/Kolkata")

    while True:
        try:
            async with websockets.connect(url) as websocket:
                logging.info(f"Connected to {url}")
                async for message in websocket:
                    data = json.loads(message)

                    # 1. Transform Data
                    trade_time = datetime.fromtimestamp(data['T'] / 1000.0, tz=ZoneInfo("UTC")).astimezone(IST)
                    clean_data = {
                        "symbol": data['s'],
                        "price": float(data['p']),
                        "quantity": float(data['q']),
                        "timestamp": trade_time.isoformat(),
                        "side": "SELL" if data['m'] else "BUY"
                    }

                    # 2. Backpressure Logic: Drop Oldest if Full
                    if data_queue.full():
                        # Remove the oldest item without waiting
                        try:
                            data_queue.get_nowait()
                            logging.warning("Queue Full! Dropping oldest record to maintain low latency.")
                        except asyncio.QueueEmpty:
                            pass
                    print(clean_data)
                    # 3. Add newest data
                    await data_queue.put(clean_data)

        except Exception as e:
            logging.error(f"Stream Error: {e}. Reconnecting...")
            await asyncio.sleep(5)

async def kafka_producer_task(data_queue: asyncio.Queue):
    """Consumes from the local data_queue and publishes to Redpanda"""
    producer = AIOKafkaProducer(
        bootstrap_servers='localhost:19092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    await producer.start()
    logging.info("Kafka Producer started and connected to Redpanda.")

    try:
        while True:
            # 1. Get data from your Phase 1 queue
            trade_data = await data_queue.get()

            # 2. Publish to the 'trades_btc' topic
            try:
                await producer.send_and_wait("trades_btc", trade_data)
                # logging.info(f"Published to Redpanda: {trade_data['price']}")
            except Exception as e:
                logging.error(f"Failed to publish: {e}")
            finally:
                # 3. Mark the task as done
                data_queue.task_done()
    finally:
        await producer.stop()

async def main():
    # Queue is created here, inside the running event loop
    data_queue = asyncio.Queue(maxsize=MAX_QUEUE_SIZE)

    # Run both tasks concurrently
    # Task 1: Fetch from Binance -> Put in Queue
    # Task 2: Fetch from Queue -> Send to Redpanda
    await asyncio.gather(
        ingest_binance_data(data_queue),
        kafka_producer_task(data_queue)
    )

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())