import os
import random
import logging
from datetime import datetime
from typing import Dict, Any

from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel

# --- Настройка логирования ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Конфигурация ---
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "kafka:9092").split(",")
EVENT_TOPIC = os.getenv("EVENT_TOPIC", "events-topic")
PORT = int(os.getenv("PORT", "8082"))

app = FastAPI(title="Events Microservice", version="1.0")

# Глобальный producer
producer = None

class EventRequest(BaseModel):
    data: Dict[str, Any]

@app.on_event("startup")
async def startup_event():
    global producer
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BROKERS,
        value_serializer=lambda x: json.dumps(x).encode("utf-8"),
    )
    await producer.start()
    logger.info(f"✅ Kafka producer started (topic: {EVENT_TOPIC})")

    # Запускаем consumer в фоне
    asyncio.create_task(consume_events())

@app.on_event("shutdown")
async def shutdown_event():
    global producer
    if producer:
        await producer.stop()

async def consume_events():
    consumer = AIOKafkaConsumer(
        EVENT_TOPIC,
        bootstrap_servers=KAFKA_BROKERS,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        group_id="events-consumer-group",
        auto_offset_reset="earliest",
    )
    await consumer.start()
    logger.info(f"🚀 Starting Kafka consumer for topic: {EVENT_TOPIC}")
    try:
        async for msg in consumer:
            try:
                event_data: dict = msg.value
                event_type: str = event_data.get("type", "unknown")
                event_time: str = event_data.get("timestamp", "N/A")
                event_payload: dict = event_data.get("data", {})

                logger.info(
                    f"[CONSUMER] ✅ Received event | type={event_type!r} | time={event_time} | data={event_payload}"
                )
            except Exception as e:
                logger.error(f"[CONSUMER] ❌ Error processing message: {e} | raw={msg.value}")
    finally:
        await consumer.stop()

@app.post("/api/events/{event_type}", status_code=201)
async def create_event(event_type: str, payload: EventRequest):
    """
    Создаёт событие и отправляет его в Kafka.
    Ожидает тело: {"data": {...}}
    Возвращает единый формат: {"status": "success"}
    """
    event = {
        "type": event_type,
        "timestamp": datetime.utcnow().isoformat(),
        "data": payload.data
    }

    try:
        await producer.send_and_wait(EVENT_TOPIC, event)
        logger.info(f"[PRODUCER] 📤 Sent event: type={event_type}, data={payload.data}")
    except Exception as e:
        logger.error(f"[PRODUCER] ❌ Failed to send event: {e}")
        raise HTTPException(status_code=500, detail="Failed to publish event")

    # ✅ ЕДИНЫЙ ФОРМАТ ОТВЕТА — как требуют тесты
    return {"status": "success"}

# Health-check (для совместимости с Postman-тестами)
@app.get("/api/events/health")
async def health():
    return {"status": "ok"}

if __name__ == "__main__":
    import uvicorn
    import json
    logger.info(f"✅ Events service started on port {PORT}")
    uvicorn.run(app, host="0.0.0.0", port=PORT)