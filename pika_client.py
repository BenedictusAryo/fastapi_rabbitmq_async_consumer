import pika
import uuid
from aio_pika import connect_robust, IncomingMessage
import asyncio
import os
import json
from typing import Callable

class PikaClient:
    
    def __init__(self, process_callable:Callable) -> None:
        self.publisher_queue_name = os.getenv('PUBLISHER_QUEUE_NAME', 'fastapi_app')
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=os.getenv('RABBITMQ_HOST', 'localhost'),
                port=os.getenv('RABBITMQ_PORT', 5672),
                credentials=pika.PlainCredentials(
                    os.getenv('RABBITMQ_USER', 'guest'), 
                    os.getenv('RABBITMQ_PASSWORD', 'guest')
                )
            )
        )
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange=self.publisher_queue_name, exchange_type='fanout')
        # self.publish_queue = self.channel.queue_declare(queue=self.publisher_queue_name)
        self.publish_queue = self.channel.queue_declare(queue='', exclusive=True)
        self.callback_queue = self.publish_queue.method.queue
        self.channel.queue_bind(exchange=self.publisher_queue_name, queue=self.callback_queue)
        self.response = None
        self.process_callable = process_callable
        print('PikaClient connection initialized')
        
    async def consume(self, loop:asyncio.AbstractEventLoop) -> None:
        """Setup message listener with the current running loop"""
        connection = await connect_robust(
            host=os.getenv('RABBITMQ_HOST', 'localhost'),
            port=os.getenv('RABBITMQ_PORT', 5672),
            login=os.getenv('RABBITMQ_USER', 'guest'),
            password=os.getenv('RABBITMQ_PASSWORD', 'guest'),
            loop=loop
        )
        channel = await connection.channel()
        exchange = await channel.declare_exchange(self.publisher_queue_name, 'fanout')
        queue = await channel.declare_queue('')
        await queue.bind(exchange)
        await queue.consume(self.process_incomming_message, no_ack=False)
        print('Established pika async listener')
        return connection
    
    async def process_incomming_message(self, message:IncomingMessage) -> None:
        """Process incoming message from the queue"""
        await message.ack()
        body = message.body
        print(f'PikaClient process_incomming_message body: {body}')
        if body:
            self.process_callable(json.loads(body))
            
            
    def send_message(self, message:dict) -> None:
        """Send message to the queue"""
        self.channel.basic_publish(
            exchange=self.publisher_queue_name,
            routing_key='',
            properties=pika.BasicProperties(
                reply_to=self.callback_queue,
                correlation_id=str(uuid.uuid4())
            ),
            body=json.dumps(message)
        )
        print(f'PikaClient send_message message: {message}')
        
