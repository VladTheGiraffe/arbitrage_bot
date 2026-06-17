#Imports
import os 
import asyncio
import time
import aiohttp
import base64
import json
import requests
from dotenv import load_dotenv
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives.serialization import load_pem_private_key

#Call .env
load_dotenv()

#Master Target List
WATCHLIST_PREFIXES = ['BTC', 'ETH', 'INX', "SOL"]

#Fetch Live BTC Tickers
def fetch_live_tickers():
    """Loops through our target assets and aggregates open contracts into one list."""
    all_tickers = []
    for prefix in WATCHLIST_PREFIXES:

        url = f"https://external-api.kalshi.com/trade-api/v2/markets?status=open&ticker_prefix={prefix}&limit=50"

        try:
            response = requests.get(url, timeout=5)
            response.raise_for_status()
            data = response.json()

            for market in data.get("markets", []):
                ticker = market.get("ticker")
                if ticker:
                    all_tickers.append(ticker)
            
            print(f"Successfully loaded contracts for {prefix}")
        
        except Exception as e:
            print(f"Error fetching tickers for {prefix}: {e}")

    return all_tickers


#Order Submission
async def place_arbitrage_order(session, ticker, side, price, size, api_key):
    """Sends an immediate market execution order to Kalshi's matching engine."""
    url = "https://external-api.kalshi.com/trade-api/v2/portfolio/orders"

    import uuid
    import time

    timestamp = str(int(time.time() * 1000))
    client_order_id = str(uuid.uuid4())

    payload = {
        "client_order_id": client_order_id,
        "market_ticker": ticker,
        "side": side,
        "price": int(price * 100),
        "count": int(size),
        "type": "market",
        "action": "buy"

    }

    message_string = f"{timestamp}POST/trade-api/v2/portfolio/orders"
    message_bytes = message_string.encode('utf-8')

    with open("kalshi_api.key", "rb") as key_file:
        private_key = load_pem_private_key(key_file.read(), password=None)

    raw_signature = private_key.sign(
        message_bytes,
        padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.MAX_LENGTH),
        hashes.SHA256()
    )
    base64_signature = base64.b64encode(raw_signature).decode('utf-8')
    headers = {
        "KALSHI-ACCESS-KEY": api_key,
        "KALSHI_ACCESS_TIMESTAMP": timestamp,
        "KALSHI-ACCESS-SIGNATURE": base64_signature,
        "Content-Type": "application/json"
    }

    try:
        async with session.post(url, json=payload, headers=headers) as response:
            res_data = await response.json()
            if response.status in [200, 201]:
                print(f"Executed {side.upper()} order for {size} contracts on {ticker} successfully.")
            else:
                print(f"Order Placement rejected: {res_data}")
            return res_data
    except Exception as e:
        print(f"Critical Exception: {e}")


#Define Async Main Function
async def main():
    api_key = os.getenv("KALSHI_API_KEY")
    url = "wss://external-api-ws.kalshi.com/trade-api/ws/v2"
    retry_delay = 1
    print("Engine Initialized. Entering autonomous runtime...")

    while True:
        try:
            watchlist = fetch_live_tickers()
            print(f"Watchlist armed with {len(watchlist)} contracts.")

            current_time = time.time()
            timestamp = str(int(current_time * 1000))
            message_string = f"{timestamp}GET/trade-api/ws/v2"
            
            with open("kalshi_api.key", "rb") as key_file:
                private_key_bytes = key_file.read()
        
        
                private_key = load_pem_private_key(private_key_bytes, password=None)
                message_bytes = message_string.encode('utf-8')
                raw_signature = private_key.sign(message_bytes, padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.MAX_LENGTH), hashes.SHA256())
                
                base64_signature = base64.b64encode(raw_signature).decode('utf-8')
        
        

                auth_headers = {
                    "KALSHI-ACCESS-KEY": api_key,
                    "KALSHI-ACCESS-TIMESTAMP": timestamp,
                    "KALSHI-ACCESS-SIGNATURE": base64_signature
                    }
                
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(url, headers=auth_headers) as ws:
                        print("Connected to Kalshi WebSocket. Sending Handshake...")
                        retry_delay = 1
                        
                        sub_payload = {
                            "id": 1,
                            "cmd": "subscribe",
                            "params": {
                                    "channels": ["orderbook_delta"],
                                    "market_ticker": watchlist 
                            }               
                            
                        }
                        await ws.send_json(sub_payload)
                        print("Subscription command sent. Awaiting response...")

                        MAX_TEST_SIZE = 2
                        print("Scanning live multi-stream for arbitrage opps...")

                        async for msg in ws:
                            data = json.loads(msg.data)
                            
                            if data.get("type") == "orderbook_delta":
                                msg_content = data.get("msg", {})
                                ticker = msg_content.get("market_ticker", "Unknown")
                                
                                yes_bid_raw = msg_content.get("yes_bid")
                                no_bid_raw = msg_content.get("no_bid")
                                yes_qty_raw = msg_content.get("yes_bid_quantity")
                                no_qty_raw = msg_content.get("no_bid_quantity")

                                if all([yes_bid_raw, no_bid_raw, yes_qty_raw, no_qty_raw]):
                                    yes_bid = float(yes_bid_raw)
                                    no_bid = float(no_bid_raw)
                                    yes_qty = int(yes_qty_raw)
                                    no_qty = int(no_qty_raw)

                                    total_bid_sum = yes_bid + no_bid

                                    if total_bid_sum > 1.00:
                                        profit_per_contract = total_bid_sum - 1.00
                                        available_liquidity = min(yes_qty, no_qty)
                                        execution_size = min(MAX_TEST_SIZE, available_liquidity)

                                        if execution_size > 0:
                                            total_profit = profit_per_contract + execution_size

                                            print(f"ARBITRAGE DETECTED on: {ticker}!")
                                            print(f"YES bid: ${yes_bid:.2f} (Vol: {yes_qty}) | NO bid: ${no_bid:.2f} (Vol: {no_qty})")
                                            print(f"Execution Plan: Buy {execution_size} contracts")
                                            print(f"Total Expected Return: ${total_profit:.2f}")

                                            await asyncio.gather(
                                                place_arbitrage_order(session, ticker, "yes", yes_bid, execution_size, api_key),
                                                place_arbitrage_order(session, ticker, "no", no_bid, execution_size, api_key)
                                            )
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            print(f"Network drop detected from exchange side: {e}")
            print(f"Circuits resetting. Reconnecting automatically in {retry_delay} seconds...")
            await asyncio.sleep(retry_delay)

            retry_delay = min(retry_delay * 2, 60)
        
        except Exception as e:
            print(f"Error occured: {e}")
            await asyncio.sleep(5)  


if __name__ == "__main__":
    asyncio.run(main())