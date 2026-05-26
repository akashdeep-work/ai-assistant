from starlette.types import ASGIApp, Receive, Scope, Send
from app.utils.logger import logger

class LoggingMiddleware:
    def __init__(self, app: ASGIApp):
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        method = scope["method"]
        path = scope["path"]
        logger.info(f"Incoming: {method} {path}")

        async def send_wrapper(message):
            if message["type"] == "http.response.start":
                status_code = message["status"]
                logger.info(f"Completed: Status {status_code} for {path}")
            await send(message)

        # Forward the request downstream using our safe wrapper
        await self.app(scope, receive, send_wrapper)
