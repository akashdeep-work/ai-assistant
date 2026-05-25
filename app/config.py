from pydantic_settings import BaseSettings,SettingsConfigDict
from typing import List
class Settings(BaseSettings):
    APP_NAME:str
    HOST:str
    PORT:int
    APP_VERSION:str
    KAFKA_BOOTSTARP_SERVER:str
    KAFKA_PROMPT_REQUEST_TOPIC:str
    KAFKA_PROMPT_RESPONSE_TOPIC:str
    REDIS_URL:str
    OLLAMA_BASE_URL:str

    UPLOAD_TEMP_DIR:str = "/upload_temp_dir"
    ALLOWED_EXTENSIONS:List[str] = [".pdf",".md",".txt"]

    class TaskType:
        CHAT_STREAM:str = "chat_stream"
        DOCUMENT_INGESTION:str = "document_ingestion"

    model_config = SettingsConfigDict(env_file=".env",extra="ignore")


settings = Settings()