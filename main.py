#Imports
import asyncio
import ast
from dotenv import load_dotenv

#Modular Imports
from kalshi_client import fetch_kalshi_tickers, generate_kalshi_signature, run_kalshi, execute_kalshi_buy
from poly_client import fetch_polymarket_tickers, snapshot_polymarket_tickers, run_polymarket
from scanner import arbitrage_scanner

#Load Environment
load_dotenv()

#Functions
def orchestrator():
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

    asyncio.run(snapshot_polymarket_tickers(matched_keys, poly_map, poly_market_state))

    asyncio.run(run_arbitrage_engine(
        kalshi_watchlist,
        poly_watchlist,
        matched_keys,
        kalshi_map,
        poly_map,
        kalshi_market_state,
        poly_market_state
    ))


async def run_arbitrage_engine(kalshi_watchlist, poly_watchlist, matched_keys, kalshi_map, poly_map, kalshi_market_state, poly_market_state):
    print("Igniting dual-stream WebSockets...")



    await asyncio.gather(
        run_kalshi(kalshi_watchlist, kalshi_market_state),
        run_polymarket(poly_watchlist, poly_market_state),
        arbitrage_scanner(matched_keys, kalshi_map, poly_map, kalshi_market_state, poly_market_state)
    )

async def test_execution():
    print(f"Firing Kalshi signature validation smoke test...")

    response = await execute_kalshi_buy("TEST_FAKE_TICKER", "yes", 1, 0.10)

    print(f"Final Test Response: {response}")

if __name__ == "__main__":
    #orchestrator()
    #asyncio.run(test_execution())
    pass