from fastapi import FastAPI
from ai_assistant import AiAssistant
from contextlib import asynccontextmanager
from app.middlewares.logging_middleware import LoggingMiddleware
from app.config import settings

@asynccontextmanager
async def lifespan(app:FastAPI):
    yield


def createApp()-> FastAPI:
    app = FastAPI(title=settings.APP_NAME, 
                  version=settings.APP_VERSION,
                  lifespan=lifespan)
    app.add_middleware(LoggingMiddleware)
    return app