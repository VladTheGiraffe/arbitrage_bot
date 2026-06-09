#Imports
import os 
import asyncio
import time
import aiohttp
import base64
from dotenv import load_dotenv
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives.serialization import load_pem_private_key

#Call .env
load_dotenv()

#Define Asynchronous Loop
async def main():
    api_key = os.getenv("KALSHI_API_KEY")
    print(f"API KEY: {api_key}")
    current_time = time.time()
    timestamp = str(int(current_time * 1000))
    print(f"TIMESTAMP: {timestamp}")
    message_string = f"{timestamp}GET/trade-api/ws/v2"
    print(f"MESSAGE STRING: {message_string}")
    with open("demo_api.key", "rb") as key_file:
        private_key_bytes = key_file.read()
    print(f"Key Loaded: {type(private_key_bytes)}")
    private_key = load_pem_private_key(private_key_bytes, password=None)
    message_bytes = message_string.encode('utf-8')
    raw_signature = private_key.sign(message_bytes, padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.MAX_LENGTH), hashes.SHA256())
    print(f"RAW SIGNATURE LENGTH: {len(raw_signature)}")
    base64_signature = base64.b64encode(raw_signature).decode('utf-8')
    print(f"Base64 Signature: {base64_signature}")
    

if __name__ == "__main__":
    asyncio.run(main())