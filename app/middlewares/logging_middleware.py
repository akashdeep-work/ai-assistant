from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware
from app.utils.logger import logger

class LoggingMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        logger.info(f"Incoming: {request.method} {request.url.path}")
        response = await call_next(request)
        logger.info(f"Completed: Status {response.status_code}")
        return response