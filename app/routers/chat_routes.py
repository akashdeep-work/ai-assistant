from fastapi import APIRouter, Request


router = APIRouter(prefix="/chats",tags=["chats"])


@router.get("/")
async def get_all_chats(request:Request):
    