from fastapi import APIRouter,Depends
from app.models.chat import AllChatsListRequest,AllChatListResponse
from app.dependencies import get_messaging_service
from app.services.message_service import ChatMessagingService

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
    