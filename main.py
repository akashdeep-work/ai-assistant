from app import createApp
import uvicorn
from app.config import settings
app = createApp()

if __name__ == "__main__":
    uvicorn.run(
        "main:app",        # Target string: file_name:app_instance_name
        host=settings.HOST, 
        port=settings.PORT,
        reload=True        # Set to False in production
    )