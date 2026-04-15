import asyncio
import json
import logging
import os
import time
from datetime import datetime
from typing import Dict, Any

from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel

app = FastAPI()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BROKER = os.getenv("KAFKA_BROKERS", "kafka:9092").split(",")
EVENT_TOPIC = os.getenv("EVENT_TOPIC", "events-topic")

producer = None


class EventData(BaseModel):
    data: Dict[str, Any]


@app.on_event('startup')
async def startup_event():
    global producer
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
    await producer.start()
    logger.info("✅ Kafka producer started")
    # Запускаем consumer в фоне
    asyncio.create_task(consume_events())


@app.on_event('shutdown')
async def shutdown_event():
    global producer
    if producer:
        await producer.stop()


async def consume_events():
    consumer = AIOKafkaConsumer(
        EVENT_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        group_id="events-consumer-group",
    )
    await consumer.start()
    logger.info(f"🚀 Starting Kafka consumer for topic: {EVENT_TOPIC}")
    try:
        async for msg in consumer:
            try:
                event_data = msg.value
                logger.info(
                    f"[CONSUMER] ✅ Received event: Type={event_data['type']} | "
                    f"Time={event_data['timestamp']} | Data={event_data['data']}"
                )
            except Exception as e:
                logger.error(f"[CONSUMER] ❌ Error processing message: {e}")
    finally:
        await consumer.stop()


@app.post("/api/events/{event_type}")
async def create_event(event_type: str, payload: EventData):
    event = {
        "type": event_type,
        "timestamp": datetime.utcnow().isoformat(),
        "data": payload.data
    }

    try:
        await producer.send_and_wait(EVENT_TOPIC, event)
        logger.info(f"[PRODUCER] 📤 Sent event: Type={event_type} | Data={payload.data}")
    except Exception as e:
        logger.error(f"[PRODUCER] ❌ Failed to send event: {e}")
        raise HTTPException(status_code=500, detail="Failed to publish event")

    return {"status": "event created", "type": event_type, "data": payload.data}


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "8082"))
    logger.info(f"✅ Events service started on port {port}")
    uvicorn.run(app, host="0.0.0.0", port=port)