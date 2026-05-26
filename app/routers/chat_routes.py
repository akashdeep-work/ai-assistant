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
import asyncio

router = APIRouter(prefix="/chats",tags=["chats"])


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
        if isinstance(msg,AIMessage):
            role="assistant"
        if isinstance(msg,SystemMessage):
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

    # 1. Create PubSub and subscribe FIRST to guarantee we don't miss messages
    pubsub = messaging.redis_client.pubsub()
    await pubsub.subscribe(f"stream:{request_id}")

    # 2. Now fire the background task to Kafka
    asyncio.create_task(
        messaging.kafka_producer.send_and_wait(
            messaging.prompt_topic, 
            json.dumps(payload).encode("utf-8")
        )
    )

    # 3. Define the generator inline so it uses the already-subscribed pubsub
    async def event_generator():
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
            shutil.copyfileobj(file.file,buffer)
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