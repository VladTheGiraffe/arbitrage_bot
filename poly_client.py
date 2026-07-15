#Imports
import os
import time
import hmac
import hashlib
import base64
import asyncio
import aiohttp
import json
import re
import requests
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs, OrderType
from py_clob_client.order_builder.constants import BUY, SELL

#Target List
TARGET_KEYWORDS = ["Bitcoin", "BTC", "Ethereum", "ETH", "Solana", "SOL", "S&P 500", "SPX"]


#Fetch Tickers
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
    pass


async def snapshot_polymarket_tickers(matched_keys, poly_map, poly_market_state):
    async with aiohttp.ClientSession() as session:
        print(f"Snapshot Engine started. Mapping {len(poly_map)} markets...")
        for bridge_key in matched_keys:
            tokens = poly_map[bridge_key]["tokens"]
            
            if len(tokens) >= 2:
                poly_yes_token = tokens[0]
                poly_no_token = tokens[1]

                for token in [poly_yes_token, poly_no_token]:
                    url = f"https://clob.polymarket.com/book?token_id={token}"

                    try:
                        # print(f"Pulling CLOB data for token: {token[-6:]}...")
                        async with session.get(url) as response:
                            if response.status == 200:
                                data = await response.json()
                                # print("Data received and parsed.")

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
    pass

#Run Polymarket
async def run_polymarket(poly_watchlist, poly_market_state):
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

                    # debug_cap = 0
                    async for msg in ws:
                        data = json.loads(msg.data)
                        print(f"Poly WS Ping: {str(data)[:60]}...")
                        # if debug_cap < 3:
                        #         print(f"\n--- RAW EXCHANGE TICK ---")
                        #         print(data)
                        #         debug_cap += 1

                        
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
                            print(f"POLY UPDATE | Token: {token_id[-6:]} | New Ask: ${ask_price}")

        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            print(f"Polymarket network drop: {e}. Reconnecting...")
            await asyncio.sleep(retry_delay)
            retry_delay = min(retry_delay * 2, 60)

        except Exception as e:
            print(f"Polymarket error: {e}.")
            await asyncio.sleep(5)


async def execute_polymarket_buy(ticker, side, size, price):
    api_key = os.getenv("POLY_API_ID")
    poly_address = os.getenv("POLY_ADDRESS")
    wallet_key = os.getenv("RABBY_PRIVATE_KEY")

    client = ClobClient(
        host="https://clob.polymarket.com",
        key=wallet_key, 
        chain_id=137, 
        funder=poly_address, 
        signature_type=2
    )

    client.set_api_creds(client.create_or_derive_api_creds())

    order_args = OrderArgs(
        price=float(price),
        size=float(size),
        side=BUY,
        token_id=ticker
    )

    signed_order = client.create_order(order_args)
    resp = client.post_order(signed_order, OrderType.GTC)
    
    return resp