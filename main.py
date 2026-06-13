#Imports
import os 
import asyncio
import time
import aiohttp
import base64
import json
from dotenv import load_dotenv
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives.serialization import load_pem_private_key

#Call .env
load_dotenv()

#URL
url = "wss://external-api-ws.kalshi.com/trade-api/ws/v2"

#Define Asynchronous Loop
async def main():
    api_key = os.getenv("KALSHI_API_KEY")
    print(f"API KEY: {api_key}")
    current_time = time.time()
    timestamp = str(int(current_time * 1000))
    print(f"TIMESTAMP: {timestamp}")
    message_string = f"{timestamp}GET/trade-api/ws/v2"
    print(f"MESSAGE STRING: {message_string}")
    
    with open("kalshi_api.key", "rb") as key_file:
        private_key_bytes = key_file.read()
    print(f"Key Loaded: {type(private_key_bytes)}")
    
    private_key = load_pem_private_key(private_key_bytes, password=None)
    message_bytes = message_string.encode('utf-8')
    raw_signature = private_key.sign(message_bytes, padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.MAX_LENGTH), hashes.SHA256())
    print(f"RAW SIGNATURE LENGTH: {len(raw_signature)}")
    
    base64_signature = base64.b64encode(raw_signature).decode('utf-8')
    print(f"Base64 Signature: {base64_signature}")
    

    auth_headers = {
        "KALSHI-ACCESS-KEY": api_key,
        "KALSHI-ACCESS-TIMESTAMP": timestamp,
        "KALSHI-ACCESS-SIGNATURE": base64_signature
        }
    
    async with aiohttp.ClientSession() as session:
        async with session.ws_connect(url, headers=auth_headers) as ws:
            print("Connected to Kalshi WebSocket. Sending Handshake...")
            
            sub_payload = {
                "id": 1,
                "cmd": "subscribe",
                "params": {
                        "channels": ["orderbook_delta"],
                        "market_ticker": "KXINXMAXMM-30JUN2026" 
                }               
                
            }
            await ws.send_json(sub_payload)
            print("Subscription command sent. Awaiting response...")

            async for msg in ws:
                data = json.loads(msg.data)
                yes_ask = data.get('yes_ask')
                no_ask = data.get('no_ask')
                
                if yes_ask and no_ask:
                    total_cost = yes_ask + no_ask

                    if total_cost < 1.00:
                        profit = 1.00 - total_cost
                        print(f"ARBITRAGE DETECTED! Profit Margin: {profit:.2f}")


if __name__ == "__main__":
    asyncio.run(main())