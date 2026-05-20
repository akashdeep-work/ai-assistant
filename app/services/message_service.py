import asyncio
import json
from app.utils.logger import logger
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
import redis.asyncio as aioredis


class ChatMessagingService:
    def __init__(self, kafka_server:str, redis_url:str, prompt_topic:str, response_topic:str):
        self.kafka_server = kafka_server
        self.redis_url = redis_url
        self.prompt_topic = prompt_topic
        self.response_topic = response_topic

        self.kafka_producer = None
        self.redis_client = None
        self._consumer_task = None

    async def start(self):
        self.kafka_producer = AIOKafkaProducer(bootstrap_servers=self.kafka_server)
        self.redis_client = aioredis.from_url(self.redis_url)

        await self.kafka_producer.start()

        self._consumer_task = asyncio.create_task(self._kafka_response_dispatcher())

        logger.info("Chat message service started")

    async def stop(self):
        if self.kafka_producer:
            await self.kafka_producer.stop()
        if self._consumer_task:
            self._consumer_task.cancel()
        if self.redis_client:
            await self.redis_client.close()
        
        logger.info("Chat message service stopped")

    async def _kafka_response_dispatcher(self):
        consumer = AIOKafkaConsumer(self.response_topic,bootstrap_servers=self.kafka_server,group_id="api-streamers")

        await consumer.start()

        try:
            async for msg in consumer:
                data = json.loads(msg.value.decode('utf-8'))
                req_id = data.get('request_id')
                if req_id:
                    await self.redis_client.publish(f"stream:{req_id}",json.dumps(data))

        except asyncio.CancelledError:
            pass
        finally:
            await consumer.stop()

    async def publish_prompt(self,request_id:str,prompt:str):
        payload = {"request_id":request_id,"prompt":prompt}
        await self.kafka_producer.send_and_wait(self.prompt_topic,json.dumps(payload).encode('utf-8'))

    async def yield_stream(self,request_id:str):
        pubsub = self.redis_client.pubsub()

        await pubsub.subscribe(f"stream:{request_id}")

        try:
            async for msg in pubsub.listen():
                if msg['type'] == 'message':
                    data = json.loads(msg['data'])
                    if data.get("status") == "done":
                        break
                    yield f"data: {json.dumps(data)}\n\n"

        finally:
            await pubsub.unsubscribe(f"stream:{request_id}")
            await pubsub.close()