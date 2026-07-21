print("--- [MAIN.PY] Starting Orchestrator ---")
#Imports
import os
import asyncio
import ast
from datetime import datetime, timedelta
from dotenv import load_dotenv


#Modular Imports
from kalshi_client import fetch_kalshi_tickers, generate_kalshi_signature, run_kalshi, execute_kalshi_buy, get_kalshi_balance
from poly_client import initialize_poly_client, fetch_polymarket_tickers, snapshot_polymarket_tickers, run_polymarket, execute_polymarket_buy, get_poly_balance
from scanner import arbitrage_scanner

#Load Environment
load_dotenv()
MIN_KALSHI_BALANCE = float(os.getenv("MIN_KALSHI_BALANCE", 5.00))
MIN_POLY_BALANCE = float(os.getenv("MIN_POLY_BALANCE", 5.00))

initialize_poly_client()

#Functions
async def orchestrator():
    print("--- [Orchestrator] Starting Arbitrage Bot ---")
    while True:
        print("\n--- [Orchestrator] Starting New 15 Minute Cycle ---")

        current_kalshi = await get_kalshi_balance()
        current_poly = await get_poly_balance()
        print(f"GATE CHECK: Kalshi Balance: ${current_kalshi:.2f} | Polymarket Balance: ${current_poly:.2f}")

        if current_kalshi < MIN_KALSHI_BALANCE or current_poly < MIN_POLY_BALANCE:
            print("CRITICAL: A platform balance has fallen below the minimum threshold.")
            print("Halting arbitrage engine until balances are replenished.")
            break

        kalshi_market_state = {}
        poly_market_state = {}

        print("Fetching Kalshi...")
        kalshi_map = fetch_kalshi_tickers()

        print("Fetching Polymarket...")
        poly_map = fetch_polymarket_tickers()

        print("\n--- ARBITRAGE BRIDGE ---")
        matched_keys = set(kalshi_map.keys()).intersection(set(poly_map.keys()))

        if matched_keys:
            print(f"SUCCESS! Found {len(matched_keys)} overlapping markets!")
            
        else:
            print("No matches found. Bridge is structurally sound, but no overlapping strikes exist right now.")
            await asyncio.sleep(60)
            continue

    #Websocket Watchlist
        kalshi_watchlist = []
        poly_watchlist = []

        for bridge_key in matched_keys:
            native_ticker = kalshi_map[bridge_key]["ticker"]
            kalshi_watchlist.append(native_ticker)

            poly_tokens = poly_map[bridge_key]["tokens"]

            if isinstance(poly_tokens, str):
                poly_tokens = ast.literal_eval(poly_tokens)
                poly_map[bridge_key]["tokens"] = poly_tokens

            poly_watchlist.extend(poly_tokens)

        print(f"Watchlist armed with {len(kalshi_watchlist)} native Kalshi tickers ready for streaming.")

        await snapshot_polymarket_tickers(matched_keys, poly_map, poly_market_state)
        
        time_remaining = get_seconds_to_next_interval(15)

        try:
            print(f"Igniting Engine with {time_remaining:.1f}s wall clock fuse...")
            await asyncio.wait_for(
                run_arbitrage_engine(
                    kalshi_watchlist,
                    poly_watchlist,
                    matched_keys,
                    kalshi_map,
                    poly_map,
                    kalshi_market_state,
                    poly_market_state,
                ),
                timeout=time_remaining
            )

        except asyncio.TimeoutError:
            print("\n[!] 15 minute fuse blew! Terminating stale markets and turning over...")
            continue


async def run_arbitrage_engine(kalshi_watchlist, poly_watchlist, matched_keys, kalshi_map, poly_map, kalshi_market_state, poly_market_state):
    print("Igniting dual-stream WebSockets...")



    await asyncio.gather(
        run_kalshi(kalshi_watchlist, kalshi_market_state),
        run_polymarket(poly_watchlist, poly_market_state),
        arbitrage_scanner(matched_keys, kalshi_map, poly_map, kalshi_market_state, poly_market_state)
    )

def get_seconds_to_next_interval(interval_minutes=15):
    now = datetime.now()

    minutes_past = now.minute % interval_minutes
    minutes_to_next = interval_minutes - minutes_past

    next_interval = now.replace(second=0, microsecond=0) + timedelta(minutes=minutes_to_next)

    seconds = (next_interval - now).total_seconds()
    return max(seconds, 1.0)


if __name__ == "__main__":
    asyncio.run(orchestrator())
    
    
