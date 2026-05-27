from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
import asyncio
import json
import os
from dotenv import load_dotenv
from langchain_core.messages import HumanMessage,AIMessage
from ai_assistant import AiAssistant
from app.utils.logger import logger
from app.utils.text_file_handler import pdf_doc_ingestion
import redis.asyncio as aioredis
from app.config import settings

load_dotenv()

KAFKA_SERVER = os.getenv("KAFKA_BOOTSTARP_SERVER","kafka:29092")
PROMPT_TOPIC = os.getenv("KAFKA_PROMPT_REQUEST_TOPIC","llm_prompt_request")
RESPONSE_TOPIC = os.getenv("KAFKA_PROMPT_RESPONSE_TOPIC","llm_prompt_response")
REDIS_URL = os.getenv("REDIS_URL","redis://localhost:6379/0")

async def handle_chat_stream(payload:dict,producer:AIOKafkaProducer,aiassistant:AiAssistant, redis_client:aioredis.Redis):
    request_id = payload.get("request_id")
    prompt = payload.get("prompt")
    thread_id = payload.get("thread_id",request_id)
    
    config = {"configurable":{"thread_id":thread_id}}
    state_update = {"messages":[HumanMessage(content=prompt)]}
    redis_channel = f"stream:{request_id}"
    try:
        async for event in aiassistant.rag_agent.astream(input=state_update,config=config,stream_mode="messages"):
            message_chunk, metadata = event
            logger.info(f"LangGraph Event - Metadata: {metadata}, Type: {type(message_chunk)}")
            if hasattr(message_chunk,"tool_call_chunk") and message_chunk.tool_call_chunk:
                logger.info("Skipping actual tool call chunk...")
                continue
            content = message_chunk.content or ""
            logger.info(f"Message content before condition: {content}")
            if message_chunk.type == "ai" and content:
                logger.info(f"Is AI Message: {content}")
                if content.strip().startswith('{"name":') or '"parameters":' in content:
                    logger.info(f"SAFETY NET CAUGHT HALLUCINATION: {content}")
                    continue
                response = {"request_id":request_id,"status":"streaming","text":content}
                logger.info(f"Json Response for Redis: {response}")
                await redis_client.publish(redis_channel,json.dumps(response))
                await producer.send_and_wait(RESPONSE_TOPIC,json.dumps(response).encode("utf-8"))
    except Exception as e:
        logger.info(f"Stream failed: {str(e)}")
        error_response = {"request_id": request_id, "status": "streaming", "text": f"\n[System Error: {str(e)}]"}
        await redis_client.publish(redis_channel, json.dumps(error_response))

    finally:            
        response = {"request_id":request_id,"status":"done"}
        await redis_client.publish(redis_channel,json.dumps(response))
        await producer.send_and_wait(RESPONSE_TOPIC,json.dumps(response).encode("utf-8"))

def handle_file_ingestion(payload:dict,aiassistant:AiAssistant):
    file_path = payload.get("file_path")
    file_ext = payload.get("file_ext")

    pdf_doc_ingestion(path=file_path, ext=file_ext,ai_assistant=aiassistant)


async def main():
    aiassistant = AiAssistant()
    await aiassistant.initialize_checkpointer()

    consumer = AIOKafkaConsumer(PROMPT_TOPIC,bootstrap_servers=KAFKA_SERVER,group_id="ai-worker", auto_offset_reset="earliest")
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_SERVER)
    redis_client = aioredis.from_url(REDIS_URL)
    await consumer.start()
    await producer.start()
    logger.info(f"worker started listening to {PROMPT_TOPIC}")

    try:
        async for msg in consumer:
            try:
                payload = json.loads(msg.value.decode('utf-8'))
                task_type = payload.get("task_type")
                if task_type == settings.TaskType.CHAT_STREAM:
                    await handle_chat_stream(payload,producer,aiassistant,redis_client)
                elif task_type == settings.TaskType.DOCUMENT_INGESTION:
                    handle_file_ingestion(payload=payload,aiassistant=aiassistant)
            except json.JSONDecodeError:
                logger.info(f"received melform json data in {msg.value}")
            except Exception as e:
                logger.info(f"task fail: {e}")

    finally:
        await consumer.stop()
        await producer.stop()
        await redis_client.close()
        await aiassistant.close()


if __name__ == "__main__":
    asyncio.run(main())

