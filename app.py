from fastapi import FastAPI
from pika_client import PikaClient
from router import router
import asyncio


class API_APP(FastAPI):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pika_client = PikaClient(
            self.log_incoming_message
        )
        
    @classmethod
    def log_incoming_message(self, message:dict) -> None:
        """Log incoming message"""
        print(f'API_APP log_incoming_message: {message}')
        
        
app = API_APP()
app.include_router(router)


@app.on_event('startup')
async def startup_event():
    loop = asyncio.get_running_loop()
    task = loop.create_task(app.pika_client.consume(loop))
    await task
        