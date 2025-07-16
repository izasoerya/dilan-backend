from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi_mqtt.config import MQTTConfig
from fastapi_mqtt.fastmqtt import FastMQTT
from dotenv import load_dotenv
import requests
import os

# Load environment variables from .env.development file
load_dotenv(".env.development")
print(os.getenv('MQTT_BROKER'))
fast_mqtt = FastMQTT(config=MQTTConfig(
    host=os.getenv('MQTT_BROKER'),
    port=8883,
    username='arr1',
    password='arr1',
    ssl=True,
))

@asynccontextmanager
async def _lifespan(_app: FastAPI):
    await fast_mqtt.mqtt_startup()
    yield
    await fast_mqtt.mqtt_shutdown()

app = FastAPI(lifespan=_lifespan)

@fast_mqtt.on_connect()
def connect(client, flags, rc, properties):
    fast_mqtt.client.subscribe(os.getenv('MQTT_TOPIC_NODE_PROP')) 
    print("Connected: ", client, flags, rc, properties)

@fast_mqtt.on_message()
async def message(client, topic, payload, qos, properties):
    print("Received message: ",topic, payload.decode(), qos, properties)
    res = requests.get(os.getenv('SUPABASE_ENDPOINT'), data=payload)
    print( res.json)

@fast_mqtt.subscribe(os.getenv('MQTT_TOPIC_NODE_PROP'))
async def message_to_topic(client, topic, payload, qos, properties):
    print("Received message to specific topic: ", topic, payload.decode(), qos, properties)

@fast_mqtt.on_disconnect()
def disconnect(client, packet, exc=None):
    print("Disconnected")

@fast_mqtt.on_subscribe()
def subscribe(client, mid, qos, properties):
    print("subscribed", client, mid, qos, properties)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="localhost", port=8000)
