#Imports
import os
import time
import asyncio
import aiohttp
import json
import base64
import requests
import uuid
from datetime import datetime, timedelta
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives.serialization import load_pem_private_key

#Target List
TARGET_SERIES = ['KXBTC15M', 'KXETH15M', 'KXSOL15M']

#Fetch Kalshi Tickers
def fetch_kalshi_tickers():
    market_map = {}
    
    for series in TARGET_SERIES:
        url = f"https://external-api.kalshi.com/trade-api/v2/markets?status=open&series_ticker={series}&limit=50"
        #print(f"Calling API: {url}")

        try:
            response = requests.get(url, timeout=5)
            response.raise_for_status()
            data = response.json()
            markets = data.get("markets", [])
            # print(f"DEBUG: Retrieved {len(markets)} total markets.")
        
            for market in markets:
                expiry_raw = market.get("close_time", "")
                ticker_string = market.get("ticker", "")
                title = market.get("title", "")
                
                if len(expiry_raw) >= 10:
                    normalized_date = expiry_raw[:10]
                    ticker = ""
                    
                    if "BTC" in ticker_string: ticker = "BTC"
                    elif "ETH" in ticker_string: ticker = "ETH"
                    elif "SOL" in ticker_string: ticker = "SOL"

                    if not ticker:
                        continue

                    utc_end = datetime.strptime(expiry_raw, "%Y-%m-%dT%H:%M:%SZ")
                    est_end = utc_end + timedelta(hours=-4)
                    est_start = est_end + timedelta(minutes=-15)

                    start_str = est_start.strftime("%I:%M%p").lstrip('0')
                    end_str = est_end.strftime("%I:%M%p").lstrip('0')

                    bucket_id = f"{ticker}|{normalized_date}|{start_str}-{end_str}"

                    market_map[bucket_id] = {
                        "ticker": ticker_string,
                        "close_time": expiry_raw
                    }

                    # print(f"DEBUG KALSHI: {ticker_string} | Title: {title} | Close: {expiry_raw}")

        except Exception as e:
            print(f"ERROR fetching Kalshi {series}: {e}")
    
    # print(f"DEBUG: Kalshi map built with {len(market_map)} normalized markets.")
    
    return market_map
    pass

def generate_kalshi_signature(method, path):
    current_time = time.time()
    timestamp = str(int(current_time * 1000))
    message_string = f"{timestamp}{method}{path}"
    # print(f"DEBUG: Hashing String --> {message_string}")
    
    with open("kalshi_api.key", "rb") as key_file:
        private_key_bytes = key_file.read()
        private_key = load_pem_private_key(private_key_bytes, password=None)
        message_bytes = message_string.encode('utf-8')
        raw_signature = private_key.sign(
            message_bytes,
            padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.MAX_LENGTH),
            hashes.SHA256()
        )
        base64_signature = base64.b64encode(raw_signature).decode('utf-8')

    return timestamp, base64_signature


async def get_kalshi_balance():
    path = "/trade-api/v2/portfolio/balance"
    method = "GET"

    timestamp, signature = generate_kalshi_signature(method, path)

    headers = {
        "KALSHI-ACCESS-KEY": os.getenv("KALSHI_API_KEY"),
        "KALSHI-ACCESS-TIMESTAMP": timestamp,
        "KALSHI-ACCESS-SIGNATURE": signature
    }

    url = f"https://external-api.kalshi.com{path}"

    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers) as response:
            data = await response.json()

            raw_cents = data.get("balance", 0)
            usd_balance = float(raw_cents) / 100.0

            return usd_balance


async def run_kalshi(kalshi_watchlist, kalshi_market_state):
    api_key = os.getenv("KALSHI_API_KEY")
    url = "wss://external-api-ws.kalshi.com/trade-api/ws/v2"
    retry_delay = 1
    print("Engine Initialized. Entering autonomous runtime...")

    while True:
        try:
            timestamp, base64_signature = generate_kalshi_signature("GET", "/trade-api/ws/v2")

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
                                "channels": ["ticker"],
                                "market_tickers": kalshi_watchlist 
                        }               
                        
                    }
                    await ws.send_json(sub_payload)
                    print("Subscription command sent. Awaiting response...")

                    # debug_cap = 0
                    async for msg in ws:
                        data = json.loads(msg.data)
                        # if debug_cap < 3:
                        #     print(f"\n--- RAW ECHANGE TICK ---")
                        #     print(data)
                        #     debug_cap += 1
                
                        if data.get("type") == "ticker":
                            msg_content = data.get("msg", {})
                            ticker = msg_content.get("market_ticker", "Unknown")
                            
                            yes_bid_raw = msg_content.get("yes_bid_dollars")
                            yes_ask_raw = msg_content.get("yes_ask_dollars")
                            yes_qty_raw = msg_content.get("yes_bid_size_fp")
                            yes_ask_qty_raw = msg_content.get("yes_ask_size_fp")

                            if all([yes_bid_raw, yes_ask_raw, yes_qty_raw, yes_ask_qty_raw]):
                                yes_bid = float(yes_bid_raw)
                                yes_ask = float(yes_ask_raw)
                                yes_bid_qty = float(yes_qty_raw)
                                yes_ask_qty = float(yes_ask_qty_raw)

                                no_bid = 1.00 - yes_ask
                                no_ask = 1.00 - yes_bid
                                no_bid_qty = yes_ask_qty
                                no_ask_qty = yes_bid_qty

                                if ticker not in kalshi_market_state:
                                    kalshi_market_state[ticker] = {
                                        "YES": {"bid": 0.0, "ask": 0.0, "bid_qty": 0.0, "ask_qty": 0.0},
                                        "NO": {"bid": 0.0, "ask": 0.0, "bid_qty": 0.0, "ask_qty": 0.0}
                                    }

                                kalshi_market_state[ticker]["YES"]["bid"] = yes_bid
                                kalshi_market_state[ticker]["YES"]["ask"] = yes_ask
                                kalshi_market_state[ticker]["YES"]["bid_qty"] = yes_bid_qty
                                kalshi_market_state[ticker]["YES"]["ask_qty"] = yes_ask_qty
                                kalshi_market_state[ticker]["NO"]["bid"] = no_bid
                                kalshi_market_state[ticker]["NO"]["ask"] = no_ask
                                kalshi_market_state[ticker]["NO"]["bid_qty"] = no_bid_qty
                                kalshi_market_state[ticker]["NO"]["ask_qty"] = no_ask_qty

                                        
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            print(f"Network drop detected from exchange side: {e}")
            print(f"Circuits resetting. Reconnecting automatically in {retry_delay} seconds...")
            await asyncio.sleep(retry_delay)

            retry_delay = min(retry_delay * 2, 60)
        
        except Exception as e:
            print(f"Error occured: {e}")
            await asyncio.sleep(5)  


async def execute_kalshi_buy(ticker, yes_no, count, price, client_order_id):
    api_key = os.getenv("KALSHI_API_KEY")
    url = "https://external-api.kalshi.com/trade-api/v2/portfolio/events/orders"
    path = "/trade-api/v2/portfolio/events/orders"

    timestamp, base64_signature = generate_kalshi_signature("POST", path)

    headers = {
        "KALSHI-ACCESS-KEY": api_key,
        "KALSHI-ACCESS-TIMESTAMP": timestamp,
        "KALSHI-ACCESS-SIGNATURE": base64_signature,
        "Content-Type": "application/json"
    }

    if yes_no.lower() == "no":
        inverted_price = 1.00 - float(price)
        formatted_price = f"{inverted_price:.4f}"
        formatted_side = "ask"

    else:
        formatted_price = f"{float(price):.4f}"
        formatted_side = "bid" if yes_no.lower() == "yes" else "ask"

    formatted_count = str(int(float(count)))

    order_payload = {
        "ticker": ticker,
        "side": formatted_side,
        "count": formatted_count,
        "price": formatted_price,
        "client_order_id": client_order_id,
        "time_in_force": "fill_or_kill",
        "self_trade_prevention_type": "taker_at_cross"
    }

    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(url, headers=headers, json=order_payload, allow_redirects=False) as response:
                result = await response.json()
                print(f"Kalshi Execution Status: {response.status}")
                return result
        
        except Exception as e:
            print(f"Failed to execute Kalshi order: {e}")
            return None


async def execute_kalshi_sell(ticker, yes_no, count, price, max_slippage, client_order_id):
    api_key = os.getenv("KALSHI_API_KEY")
    url = "https://external-api.kalshi.com/trade-api/v2/portfolio/events/orders"
    path = "/trade-api/v2/portfolio/events/orders"

    timestamp, base64_signature = generate_kalshi_signature("POST", path)

    headers = {
        "KALSHI-ACCESS-KEY": api_key,
        "KALSHI-ACCESS-TIMESTAMP": timestamp,
        "KALSHI-ACCESS-SIGNATURE": base64_signature,
        "Content-Type": "application/json"
    }

    base_price = float(price)

    if yes_no.lower() == "yes":
        formatted_side = "ask"
        calculated_price = base_price - max_slippage
    else:
        formatted_side = "bid"
        calculated_price = base_price + max_slippage
    
    formatted_count = str(int(float(count)))
    formatted_price = f"{calculated_price:.4f}"

    order_payload = {
        "ticker": ticker,
        "side": formatted_side,
        "count": formatted_count,
        "price": formatted_price,
        "client_order_id": client_order_id,
        "time_in_force": "fill_or_kill",
        "self_trade_prevention_type": "taker_at_cross"
    }

    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(url, headers=headers, json=order_payload, allow_redirects=False) as response:
                result = await response.json()
                print(f"Kalshi Execution Status: {response.status}")
                return result
        
        except Exception as e:
            print(f"Failed to execute Kalshi order: {e}")
            return None


async def check_kalshi_order(client_order_id):
    url = "https://external-api.kalshi.com/trade-api/v2"
    endpoint_path = f"/portfolio/orders/{client_order_id}"

    try:
        timestamp, signature = generate_kalshi_signature("GET", endpoint_path)

        headers = {
            "KALSHI-ACCESS-KEY": os.getenv("KALSHI_API_KEY"),
            "KALSHI-ACCESS-TIMESTAMP": timestamp,
            "KALSHI-ACCESS-SIGNATURE": signature
        }

        async with aiohttp.ClientSession() as session:
            async with session.get(url + endpoint_path, headers=headers) as resp:
                response = await resp.json()

        status = response.get("order", {}).get("status")

        if status == "executed":
            return True
        return False

    except Exception as e:
        print(f"Interrogation failed: {e}")
        return False


if __name__ == "__main__":
    import asyncio
    from dotenv import load_dotenv
    load_dotenv()

    balance = asyncio.run(get_kalshi_balance())
    print(f"Kalshi Account Balance: ${balance}")