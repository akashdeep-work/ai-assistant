from pydantic import BaseModel,Field
from typing import List

class AllChatsListRequest(BaseModel):
    thread_ids:list[str]

class ChatItem(BaseModel):
    thread_id:str = Field(description="Unique id for each message thread")
    last_message:str = Field(description="the last message")


class AllChatListResponse(BaseModel):
    chats:List[ChatItem]

class ChatMessageRequest(BaseModel):
    thread_id:str
    prompt:str

class MessageDetails(BaseModel):
    role: str = Field(description="The sender role: 'user', 'assistant', 'system', or 'tool'.")
    content: str = Field(description="The text content or output data of the message.")

class ChatMessageResponse(BaseModel):
    thread_id:str
    messages:List[MessageDetails]