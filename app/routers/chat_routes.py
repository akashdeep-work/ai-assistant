from fastapi import APIRouter,Depends, HTTPException
from app.models.chat import AllChatsListRequest,AllChatListResponse,ChatMessagesResponse
from app.dependencies import get_messaging_service
from app.services.message_service import ChatMessagingService
from langchain_core.messages import HumanMessage, SystemMessage, AIMessage 

router = APIRouter(prefix="/chats",tags=["chats"])


@router.post("/all",response_model=AllChatListResponse)
async def get_all_chats(request:AllChatsListRequest, messaging:ChatMessagingService=Depends(get_messaging_service)):
    thread_ids = request.thread_ids
    preview = []
    for id in thread_ids:
        config = {"configurable":{"thread_id":id}}
        
        state_snapshot = messaging.checkpointer.get_tuple(config)

        if state_snapshot and state_snapshot.checkpoint:
            channel_value = state_snapshot.checkpoint.get("channel_values",{})
            messages = channel_value.get("message",[])
            if messages:
                preview.append({"thread_id":id,"last_message":messages[-1].content})

    return {"chats": preview}

@router.post("/{thread_id}",response_model=ChatMessagesResponse)
async def get_chat_by_thread(thread_id:str, messaging:ChatMessagingService=Depends(get_messaging_service)):
    
    config = {"configurable":{"thread_id":id}}
        
    state_snapshot = messaging.checkpointer.get_tuple(config)

    if not state_snapshot or not state_snapshot.checkpoint:
        return HTTPException(status_code=404,detail="Chat history not found")
    channel_value = state_snapshot.checkpoint.get("channel_values",{})
    messages = channel_value.get("message",[])
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
    