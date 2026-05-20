from pydantic_settings import BaseSettings,SettingsConfigDict

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

    model_config = SettingsConfigDict(env_file=".env",extra="ignore")


settings = Settings()