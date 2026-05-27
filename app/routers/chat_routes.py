from fastapi import APIRouter,Depends, HTTPException, BackgroundTasks, File, UploadFile
from app.models.chat import AllChatsListRequest,AllChatListResponse,ChatMessagesResponse, ChatMessageRequest
from app.dependencies import get_messaging_service
from app.services.message_service import ChatMessagingService
from langchain_core.messages import HumanMessage, SystemMessage, AIMessage 
import json
import uuid
from fastapi.responses import StreamingResponse
import os
import shutil
from app.config import settings
from app.utils.logger import logger
import asyncio
from fastapi.concurrency import run_in_threadpool

router = APIRouter(prefix="/chats",tags=["chats"])

os.makedirs(settings.UPLOAD_TEMP_DIR,exist_ok=True)

@router.post("/all",response_model=AllChatListResponse)
async def get_all_chats(request:AllChatsListRequest, messaging:ChatMessagingService=Depends(get_messaging_service)):
    thread_ids = request.thread_ids
    preview = []
    for id in thread_ids:
        config = {"configurable":{"thread_id":id}}
        
        state_snapshot = await messaging.checkpointer.aget_tuple(config)

        if state_snapshot and state_snapshot.checkpoint:
            channel_value = state_snapshot.checkpoint.get("channel_values",{})
            messages = channel_value.get("messages",[])
            if messages:
                preview.append({"thread_id":id,"last_message":messages[-1].content})

    return {"chats": preview}

@router.get("/{thread_id}",response_model=ChatMessagesResponse)
async def get_chat_by_thread(thread_id:str, messaging:ChatMessagingService=Depends(get_messaging_service)):
    
    config = {"configurable":{"thread_id":thread_id}}
        
    state_snapshot = await messaging.checkpointer.aget_tuple(config)

    if not state_snapshot or not state_snapshot.checkpoint:
        raise HTTPException(status_code=404,detail="Chat history not found")
    channel_value = state_snapshot.checkpoint.get("channel_values",{})
    messages = channel_value.get("messages",[])
    formatted_messages = []
    for msg in messages:
        if isinstance(msg,HumanMessage):
            role="user"
        elif isinstance(msg,AIMessage):
            role="assistant"
        elif isinstance(msg,SystemMessage):
            role="system"
        else:
            role="tool"

        if role == "tool" and not msg.content:
            continue

        formatted_messages.append({"role":role,"content":msg.content})

    return {"thread_id":thread_id,"messages":formatted_messages}
    

@router.post("/query")
async def user_query(
    request: ChatMessageRequest, 
    messaging: ChatMessagingService = Depends(get_messaging_service)
):
    thread_id = request.thread_id
    prompt = request.prompt
    request_id = str(uuid.uuid4())

    payload = {
        "task_type": settings.TaskType.CHAT_STREAM,
        "thread_id": thread_id,
        "prompt": prompt,
        "request_id": request_id
    }

    pubsub = messaging.redis_client.pubsub()
    await pubsub.subscribe(f"stream:{request_id}")
    logger.info(f"Subscribed to Redis channel: stream:{request_id}")

    # 1. Wrap the background task to catch silent errors!
    async def send_to_kafka_safely():
        try:
            logger.info("Attempting to send prompt to Kafka...")
            await messaging.kafka_producer.send_and_wait(
                messaging.prompt_topic, 
                json.dumps(payload).encode("utf-8")
            )
            logger.info("Successfully sent prompt to Kafka!")
        except Exception as e:
            logger.error(f"KAFKA SEND ERROR: {e}")
            # If Kafka fails, send a dummy message to close the stream gracefully
            error_msg = {"request_id": request_id, "status": "done", "text": "Kafka Error"}
            await messaging.redis_client.publish(f"stream:{request_id}", json.dumps(error_msg))

    # Fire the safe task
    asyncio.create_task(send_to_kafka_safely())

    # 2. Add heavily instrumented streaming generator
    async def event_generator():
        try:
            # Yield immediately so Nginx doesn't close the empty connection
            yield f"data: {json.dumps({'status': 'connected'})}\n\n"
            logger.info("Waiting for Redis messages...")
            
            async for msg in pubsub.listen():
                logger.info(f"Raw Redis message: {msg}")
                if msg['type'] == 'message':
                    data = json.loads(msg['data'])
                    
                    if data.get("status") == "done":
                        logger.info("Received 'done' signal. Closing stream.")
                        break
                        
                    yield f"data: {json.dumps(data)}\n\n"
        except Exception as e:
            logger.error(f"STREAMING ERROR: {e}")
        finally:
            logger.info("Unsubscribing and closing stream.")
            await pubsub.unsubscribe(f"stream:{request_id}")
            await pubsub.close()

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no"
        }
    )

@router.post("/upload")
async def upload_file(background_tasks:BackgroundTasks, file:UploadFile = File(...), messaging:ChatMessagingService=Depends(get_messaging_service)):
    allowed_extensions = settings.ALLOWED_EXTENSIONS
    file_ext = os.path.splitext(file.filename)[1].lower()
    if file_ext not in allowed_extensions:
        raise HTTPException(status_code=400, 
            detail=f"Unsupported file type. Allowed: {', '.join(allowed_extensions)}")
    unique_name = f"{uuid.uuid4}{file_ext}"
    file_path = os.path.join(settings.UPLOAD_TEMP_DIR,unique_name)
    try:
        with open(file_path,"wb") as buffer:
            await run_in_threadpool(shutil.copyfileobj,file.file,buffer)
    except Exception as e:
        raise HTTPException(status_code=500,detail="Failed to save the file")
    finally:
        await file.close()

    payload = {
        "task_type":settings.TaskType.DOCUMENT_INGESTION,
        "file_path":file_path,
        "file_ext":file_ext
    }

    background_tasks.add_task(messaging.kafka_producer.send_and_wait,messaging.prompt_topic,json.dumps(payload).encode("utf-8"))

    return {"status":"queued","message":"File uploaded successfully. Processing chunking and vector store ingestion in background."}