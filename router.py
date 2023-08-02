from fastapi import APIRouter
from fastapi.responses import JSONResponse
from fastapi.requests import Request
from api_schema import MessageSchema


router = APIRouter(
    tags=['items'],
    responses={404: {'description': 'Not found'}}    
)

@router.post('/send-message')
async def send_message(payload: MessageSchema, request: Request):
    """Send message to the queue"""
    print(f'API_APP send_message: {payload}')
    request.app.pika_client.send_message(
        {"message": payload.message}
    )
    return JSONResponse(status_code=200, content={'message': 'Message sent to the queue', "status": "success"})