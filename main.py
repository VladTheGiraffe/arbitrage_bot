print("--- [MAIN.PY] Starting Orchestrator ---")
#Imports
import asyncio
import ast
from dotenv import load_dotenv

#Modular Imports
from kalshi_client import fetch_kalshi_tickers, generate_kalshi_signature, run_kalshi, execute_kalshi_buy
from poly_client import client, fetch_polymarket_tickers, snapshot_polymarket_tickers, run_polymarket, execute_polymarket_buy
from scanner import arbitrage_scanner

#Load Environment
load_dotenv()

#Functions
async def orchestrator():
    print("--- [Orchestrator] Starting Arbitrage Bot ---")
    while True:
        print("\n--- [Orchestrator] Starting New 15 Minute Cycle ---")

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
            return

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
        
        try:
            print("Igniting Engine with 15 minute fuse...")
            await asyncio.wait_for(
                run_arbitrage_engine(
                    client,
                    kalshi_watchlist,
                    poly_watchlist,
                    matched_keys,
                    kalshi_map,
                    poly_map,
                    kalshi_market_state,
                    poly_market_state,
                ),
                timeout=900.0
            )

        except asyncio.TimeoutError:
            print("\n[!] 15 minute fuse blew! Terminating stale markets and turning over...")
            continue


async def run_arbitrage_engine(client, kalshi_watchlist, poly_watchlist, matched_keys, kalshi_map, poly_map, kalshi_market_state, poly_market_state):
    print("Igniting dual-stream WebSockets...")



    await asyncio.gather(
        run_kalshi(kalshi_watchlist, kalshi_market_state),
        run_polymarket(poly_watchlist, poly_market_state),
        arbitrage_scanner(client, matched_keys, kalshi_map, poly_map, kalshi_market_state, poly_market_state)
    )


if __name__ == "__main__":
    asyncio.run(orchestrator())
    
    
