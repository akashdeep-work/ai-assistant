from fastapi import Request
from app.services.message_service import ChatMessagingService

async def get_messaging_service(request:Request) -> ChatMessagingService:
    return request.app.state.messaging