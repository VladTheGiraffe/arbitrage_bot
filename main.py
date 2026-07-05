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
from datetime import datetime, timedelta

#Call .env
load_dotenv()

#Initialize empty dictionary
kalshi_market_state = {}
poly_market_state = {}

#Master Target List
TARGET_SERIES = ['KXBTC15M', 'KXETH15M', 'KXSOL15M']
TARGET_KEYWORDS = ["Bitcoin", "BTC", "Ethereum", "ETH", "Solana", "SOL", "S&P 500", "SPX"]

#Fetch Live BTC Tickers
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
            print(f"DEBUG: Retrieved {len(markets)} total markets.")
        
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

                    print(f"DEBUG KALSHI: {ticker_string} | Title: {title} | Close: {expiry_raw}")

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
            payload = []

            while True:
                try:
                    response = requests.get(url, timeout=5)
                    response.raise_for_status()
                    payload = response.json()
                    
                    time.sleep(0.2)
                    break

                except Exception as e:
                    if "422" in str(e):
                        print(f"Reached max database bounds at offset {offset}. Ending scan.")
                        break

                    print(f"Network block at offset {offset}. Cooling down for 5 seconds.. [{e}]")
                    time.sleep(5)

            if len(payload) == 0:
                print(f"Reached the end of active markets at offset {offset}. Stopping scan.")
                break

            print(f"DEBUG: Polymarket returned {len(payload)} total markets.")
            
            
            for event in payload:
                for market in event.get("markets", []):
                    expiry = market.get('endDateIso')
                    question = market.get('question', '')

                    if any(keyword.lower() in question.lower() for keyword in TARGET_KEYWORDS):
                        match = re.search(r'([A-Za-z]+) Up or Down - [A-Za-z]+ \d{1,2}, (\d{1,2}:\d{2}[AP]M)-(\d{1,2}:\d{2}[AP]M)', question)
                        #print(f"MATCHED KEYWORD IN: {question}")
                        if match:
                            #print(f"REGEX SUCCESS: {match.group(1)}")
                        
                            if expiry:
                                normalized_date = expiry[:10]
                                clob_token_ids = market.get('clobTokenIds', [])
                                asset = match.group(1).upper()
                                start_time = match.group(2)
                                end_time = match.group(3)

                                ticker_map = {"BITCOIN": "BTC", "ETHEREUM": "ETH", "SOLANA": "SOL"}
                                ticker = ticker_map.get(asset, asset)

                                bucket_id = f"{ticker}|{normalized_date}|{start_time}-{end_time}"
                                market_map[bucket_id] = {
                                    "question": question,
                                    "tokens": clob_token_ids
                                }

                        # else:
                        #     print(f"failed: REGEX missed the strike or expiry is missing.")
            
            print(f"DEBUG: Polymarket map built with {len(market_map)} markets.")
    
    return market_map 
                
async def snapshot_polymarket_tickers(poly_map):
    async with aiohttp.ClientSession() as session:
        for value in poly_map.values():
            tokens = value.get("tokens", [])
            
            if len(tokens) >= 2:
                poly_yes_token = tokens[0]
                poly_no_token = tokens[1]

                for token in [poly_yes_token, poly_no_token]:
                    url = f"https://clob.polymarket.com/book?token_id={token}"

                    try:
                        async with session.get(url) as response:
                            if response.status == 200:
                                data = await response.json

                                bid_price = 0.0
                                ask_price = 0.0

                                bids = data.get("bids", [])
                                if len(bids) > 0:
                                    bid_price = float(bids[0].get("price"))

                                asks = data.get("asks", [])
                                if len(asks) > 0:
                                    ask_price = float(asks[0].get("price"))

                                if token not in poly_market_state:
                                    poly_market_state[token] = {"bid": 0.0, "ask": 0.0}

                                poly_market_state[token]["bid"] = bid_price
                                poly_market_state[token]["ask"] = ask_price

                    except Exception as e:
                        print(f"Snapshot failed for {token}: {e}")

def orchestrator():
    print("Fetching Kalshi...")
    kalshi_map = fetch_kalshi_tickers()

    print("Fetching Polymarket...")
    poly_map = fetch_polymarket_tickers()

    print("\n--- POLYMARKET'S CAPTURED MARKETS ---")
    poly_sample = list(poly_map.items())[:10]
    for key, data in poly_sample:
        print(f"KEY: {key}  ->  {data['question']}")
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
            print(f"POLY: {poly_map[key]["question"]}")
            print("_" * 30)

    else:
        print("No matches found. Bridge is structurally sound, but no overlapping strikes exist right now.")

#Websocket Watchlist
    kalshi_watchlist = []
    poly_watchlist = []

    for bridge_key in matched_keys:
        native_ticker = kalshi_map[bridge_key]["ticker"]
        kalshi_watchlist.append(native_ticker)

        poly_tokens = poly_map[bridge_key]["tokens"]
        poly_watchlist.extend(poly_tokens)

    print(f"Watchlist armed with {len(kalshi_watchlist)} native Kalshi tickers ready for streaming.")

    asyncio.run(run_arbitrage_engine(kalshi_watchlist, poly_watchlist, matched_keys, kalshi_map, poly_map))

#Define Async Kalshi Function
async def run_kalshi(kalshi_watchlist):
    api_key = os.getenv("KALSHI_API_KEY")
    url = "wss://external-api-ws.kalshi.com/trade-api/ws/v2"
    retry_delay = 1
    print("Engine Initialized. Entering autonomous runtime...")

    while True:
        try:
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


async def run_polymarket(poly_watchlist):
    url = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    retry_delay = 1

    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(url) as ws:
                    print("Connected to Polymarket WebSocket. Sending Handshake...")

                    sub_payload = {
                        "assets_ids": poly_watchlist,
                        "type": "market"
                    }

                    await ws.send_json(sub_payload)

                    debug_cap = 0
                    async for msg in ws:
                        data = json.loads(msg.data)
                        if debug_cap < 3:
                                print(f"\n--- RAW EXCHANGE TICK ---")
                                print(data)
                                debug_cap += 1

                        
                        for event in data:
                            token_id = event.get("asset_id")
                            bid_price = 0.0
                            ask_price = 0.0

                            bids = event.get("bids", [])

                            if len(bids) > 0:
                                bid_price = float(bids[0].get("price"))

                            asks = event.get("asks", [])

                            if len(asks) > 0:
                                ask_price = float(asks[0].get("price"))

                            if token_id:
                                if token_id not in poly_market_state:
                                    poly_market_state[token_id] = {"bid": 0.0, "ask": 0.0}

                            poly_market_state[token_id]["bid"] = bid_price
                            poly_market_state[token_id]["ask"] = ask_price

        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            print(f"Polymarket network drop: {e}. Reconnecting...")
            await asyncio.sleep(retry_delay)
            retry_delay = min(retry_delay * 2, 60)

        except Exception as e:
            print(f"Polymarket error: {e}.")
            await asyncio.sleep(5)


async def run_arbitrage_engine(kalshi_watchlist, poly_watchlist, matched_keys, kalshi_map, poly_map):
    print("Igniting dual-stream WebSockets...")

    await asyncio.gather(
        run_kalshi(kalshi_watchlist),
        run_polymarket(poly_watchlist),
        arbitrage_scanner(matched_keys, kalshi_map, poly_map)
    )


async def arbitrage_scanner(matched_keys, kalshi_map, poly_map):
    print("Scanner active. Hunting for risk free margins...")

    while True:
        for bridge_key in matched_keys:
            kalshi_ticker = kalshi_map[bridge_key]["ticker"]
            poly_tokens = poly_map[bridge_key]["tokens"]

            poly_yes_token = poly_tokens[0]
            poly_no_token = poly_tokens[1]

            print(f"Lock Status -> Kalshi: {kalshi_ticker in kalshi_market_state} | Poly YES: {poly_yes_token in poly_market_state} | Poly NO: {poly_no_token in poly_market_state}")
            print(f"Poly State Keys: {list(poly_market_state.keys())}")

            if (kalshi_ticker in kalshi_market_state and
                poly_yes_token in poly_market_state and
                poly_no_token in poly_market_state):

                kalshi_yes_cost = kalshi_market_state[kalshi_ticker]["YES"]["ask"]
                kalshi_no_cost = kalshi_market_state[kalshi_ticker]["NO"]["ask"]

                poly_yes_cost = poly_market_state[poly_yes_token]["ask"]
                poly_no_cost = poly_market_state[poly_no_token]["ask"]

                

                #Math Engine
                fee_buffer = 0.02

                scenario_a_cost = kalshi_yes_cost + poly_no_cost + fee_buffer
                scenario_b_cost = kalshi_no_cost + poly_yes_cost + fee_buffer

                print(f"Tracking {kalshi_ticker} | Cost A: ${scenario_a_cost:.2f} | Cost B: ${scenario_b_cost:.2f}")

                if scenario_a_cost < 1.00:
                    profit = 1.00 - scenario_a_cost
                    print(f"ARB FOUND [Scenario A]")
                    print(f"Market: {bridge_key}")
                    print(f"Cost: ${scenario_a_cost:.2f} | Profit: ${profit:.2f}\n")

                if scenario_b_cost < 1.00:
                    profit = 1.00 - scenario_b_cost
                    print(f"ARB FOUND [Scenario B]")
                    print(f"Market: {bridge_key}")
                    print(f"Cost: ${scenario_b_cost:.2f} | Profit: ${profit:.2f}\n")

        await asyncio.sleep(1)


if __name__ == "__main__":
    #fetch_kalshi_tickers()
    #fetch_polymarket_tickers()
    orchestrator()
    