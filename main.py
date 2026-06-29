#Imports
import os 
import asyncio
import time
import aiohttp
import base64
import json
import requests
import psycopg2
import re
from dotenv import load_dotenv
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives.serialization import load_pem_private_key

#Call .env
load_dotenv()

#Initialize empty dictionary
kalshi_market_state = {}

#Master Target List
TARGET_SERIES = ['KXBTCD', 'KXBTC', 'KXETHD', 'KXETH', 'KXINX', 'KXSOLD', 'KXSOL']
TARGET_KEYWORDS = ["Bitcoin", "BTC", "Ethereum", "ETH", "Solana", "SOL", "S&P 500", "SPX"]

#Fetch Live BTC Tickers
def fetch_kalshi_tickers():
    market_map = {}
    
    for series in TARGET_SERIES:
        url = f"https://external-api.kalshi.com/trade-api/v2/markets?status=open&series_ticker={series}&limit=50"
        print(f"Calling API: {url}")

        try:
            response = requests.get(url, timeout=5)
            response.raise_for_status()
            data = response.json()
            markets = data.get("markets", [])
            print(f"DEBUG: Retrieved {len(markets)} total markets.")
        
            for market in markets:
                strike = None
                expiry_raw = market.get("close_time", "")
                ticker_string = market.get("ticker", "")
                
                if ticker_string and "-" in ticker_string:
                    strike_block = ticker_string.split("-")[-1]
                    
                    if len(strike_block) > 1:
                        try:                
                            strike = float(strike_block[1:])
                        except ValueError:
                            pass
                


                if strike is not None and len(expiry_raw) >= 10:
                    normalized_date = expiry_raw[:10]
                    clean_strike = float(round(strike))

                    bucket_id = f"{normalized_date}|{clean_strike}"
                    market_map[bucket_id] = market.get("ticker")
                else:
                    print(f"DEBUG: Found market {market.get('ticker')} but NO strike price.")

        except Exception as e:
            print(f"ERROR fetching Kalshi {series}: {e}")
    
    print(f"DEBUG: Kalshi map built with {len(market_map)} normalized markets.")
    return market_map


def fetch_polymarket_tickers():
    market_map = {}
    target_categories = ["crypto", "business"]
    offsets = range(0, 3000, 100)

    for category in target_categories:

        for offset in offsets:
            url = f"https://gamma-api.polymarket.com/events?tag_slug={category}&active=true&closed=false&limit=100&offset={offset}"
            try:
                response = requests.get(url, timeout=5)
                response.raise_for_status()
                payload = response.json()

                if len(payload) == 0:
                    print(f"Reached the end of active markets at offset {offset}. Stopping scan.")
                    break

                print(f"DEBUG: Polymarket returned {len(payload)} total markets.")
                
                
                for event in payload:
                    for market in event.get("markets", []):
                        expiry = market.get('endDateIso')
                        question = market.get('question', '')

                        if any(keyword.lower() in question.lower() for keyword in TARGET_KEYWORDS):
                            match = re.search(r'\$([0-9,]+)', question)
                            print(f"MATCHED KEYWORD IN: {question}")
                            if match:
                                print(f"REGEX SUCCESS: {match.group(1)}")
                            
                                if expiry:
                                    raw_number = match.group(1).replace(',', '')
                                    clean_strike = float(raw_number)
                                    normalized_date = expiry[:10]

                                    bucket_id = f"{normalized_date}|{clean_strike}"
                                    market_map[bucket_id] = question

                            else:
                                print(f"failed: REGEX missed the strike or expiry is missing.")
                
                print(f"DEBUG: Polymarket map built with {len(market_map)} markets.")
            
            except Exception as e:
                    print(f"ERROR DETECTED: {e}")
                    break
    
    return market_map 
                
        

def orchestrator():
    print("Fetching Kalshi...")
    kalshi_map = fetch_kalshi_tickers()

    print("Fetching Polymarket...")
    poly_map = fetch_polymarket_tickers()

    print("\n--- POLYMARKET'S CAPTURED MARKETS ---")
    for key, question in poly_map.items():
        print(f"KEY: {key}  ->  {question}")
    print("------------------------------------\n")

    print("\n--- KALSHI'S CAPTURED MARKETS ---")
    sample_keys = list(kalshi_map.keys())[:10]
    for key in sample_keys:
        print(f"KALSHI KEY: {key}  ->  {kalshi_map[key]}")
    print("------------------------------------\n")

    print("\n--- ARBITRAGE BRIDGE ---")
    matched_keys = set(kalshi_map.keys()).intersection(set(poly_map.keys()))

    if matched_keys:
        print(f"SUCCESS! Found {len(matched_keys)} overlapping markets!")
        for key in matched_keys:
            print(f"MATCH: {key}")
            print(f"KALSHI: {kalshi_map[key]}")
            print(f"POLY: {poly_map[key]}")
            print("_" * 30)

    else:
        print("No matches found. Bridge is structurally sound, but no overlapping strikes exist right now.")

#Define Async Kalshi Function
# async def run_kalshi():
#     api_key = os.getenv("KALSHI_API_KEY")
#     url = "wss://external-api-ws.kalshi.com/trade-api/ws/v2"
#     retry_delay = 1
#     print("Engine Initialized. Entering autonomous runtime...")

#     while True:
#         try:
#             watchlist = fetch_live_tickers()
#             print(f"Watchlist armed with {len(watchlist)} contracts.")

#             current_time = time.time()
#             timestamp = str(int(current_time * 1000))
#             message_string = f"{timestamp}GET/trade-api/ws/v2"
            
#             with open("kalshi_api.key", "rb") as key_file:
#                 private_key_bytes = key_file.read()
        
        
#                 private_key = load_pem_private_key(private_key_bytes, password=None)
#                 message_bytes = message_string.encode('utf-8')
#                 raw_signature = private_key.sign(message_bytes, padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.MAX_LENGTH), hashes.SHA256())
                
#                 base64_signature = base64.b64encode(raw_signature).decode('utf-8')
        
        

#                 auth_headers = {
#                     "KALSHI-ACCESS-KEY": api_key,
#                     "KALSHI-ACCESS-TIMESTAMP": timestamp,
#                     "KALSHI-ACCESS-SIGNATURE": base64_signature
#                     }
                
#                 async with aiohttp.ClientSession() as session:
#                     async with session.ws_connect(url, headers=auth_headers) as ws:
#                         print("Connected to Kalshi WebSocket. Sending Handshake...")
#                         retry_delay = 1
                        
#                         sub_payload = {
#                             "id": 1,
#                             "cmd": "subscribe",
#                             "params": {
#                                     "channels": ["ticker"],
#                                     "market_tickers": watchlist 
#                             }               
                            
#                         }
#                         await ws.send_json(sub_payload)
#                         print("Subscription command sent. Awaiting response...")

#                         MAX_TEST_SIZE = 2
#                         print("Scanning live multi-stream for arbitrage opps...")

#                         async for msg in ws:
#                             data = json.loads(msg.data)
#                             print(f"Raw Data: {data}")
                            
#                             if data.get("type") == "ticker":
#                                 msg_content = data.get("msg", {})
#                                 ticker = msg_content.get("market_ticker", "Unknown")
                                
#                                 yes_bid_raw = msg_content.get("yes_bid_dollars")
#                                 no_bid_raw = msg_content.get("yes_ask_dollars")
#                                 yes_qty_raw = msg_content.get("yes_bid_size_fp")
#                                 no_qty_raw = msg_content.get("yes_ask_size_fp")

#                                 if all([yes_bid_raw, no_bid_raw, yes_qty_raw, no_qty_raw]):
#                                     yes_bid = float(yes_bid_raw)
#                                     no_bid = float(no_bid_raw)
#                                     yes_qty = float(yes_qty_raw)
#                                     no_qty = float(no_qty_raw)

#                                     if ticker not in kalshi_market_state:
#                                         kalshi_market_state[ticker] = {
#                                             "YES": {"bid": 0.0, "ask": 0.0},
#                                             "NO": {"bid": 0.0, "ask": 0.0}
#                                         }

#                                     kalshi_market_state[ticker]["YES"]["bid"] = yes_bid
#                                     kalshi_market_state[ticker]["NO"]["bid"] = no_bid

                                           
#         except (aiohttp.ClientError, asyncio.TimeoutError) as e:
#             print(f"Network drop detected from exchange side: {e}")
#             print(f"Circuits resetting. Reconnecting automatically in {retry_delay} seconds...")
#             await asyncio.sleep(retry_delay)

#             retry_delay = min(retry_delay * 2, 60)
        
#         except Exception as e:
#             print(f"Error occured: {e}")
#             await asyncio.sleep(5)  


if __name__ == "__main__":
    #fetch_kalshi_tickers()
    #fetch_polymarket_tickers()
    orchestrator()
    