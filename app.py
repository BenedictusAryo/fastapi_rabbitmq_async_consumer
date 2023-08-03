from fastapi import FastAPI
from pika_client import PikaClient
from router import router
import asyncio
import os

class API_APP(FastAPI):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pika_client = PikaClient(
            self.log_incoming_message
        )
        
    @classmethod
    def log_incoming_message(self, message:dict) -> None:
        """Log incoming message"""
        print(f'API_APP PID: [{os.getpid()}] log_incoming_message: {message}')
        
        
app = API_APP()
app.include_router(router)


@app.on_event('startup')
async def startup_event():
    pid = os.getpid()
    print(f"PID: {pid}")
    loop = asyncio.get_running_loop()
    task = loop.create_task(app.pika_client.consume(loop))
    await task
        