from fastapi import FastAPI
from ai_assistant import AiAssistant
from contextlib import asynccontextmanager
from app.middlewares.logging_middleware import LoggingMiddleware
from app.config import settings
from app.services.message_service import ChatMessagingService
from fastapi.middleware.cors import CORSMiddleware
from app.routers.chat_routes import router as chat_router

@asynccontextmanager
async def lifespan(app:FastAPI):
    app.state.messaging = ChatMessagingService(settings.KAFKA_BOOTSTARP_SERVER,settings.REDIS_URL,settings.KAFKA_PROMPT_REQUEST_TOPIC,settings.KAFKA_PROMPT_RESPONSE_TOPIC)
    
    await app.state.messaging.start()
    yield
    await app.state.messaging.stop()
    


def createApp()-> FastAPI:
    app = FastAPI(title=settings.APP_NAME, 
                  version=settings.APP_VERSION,
                  lifespan=lifespan)
    app.add_middleware(LoggingMiddleware)

    app.add_middleware(CORSMiddleware,allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],)

    api_prefix = "/v1/api"

    @app.get("/health_check")
    def health_check():
        return {"status":"OK"}

    app.include_router(chat_router,prefix=api_prefix)
    return app