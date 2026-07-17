#Imports
import asyncio
import time
import math
from datetime import datetime
from poly_client import execute_polymarket_buy
from kalshi_client import execute_kalshi_buy

#Arbitrage Scanner
async def arbitrage_scanner(matched_keys, kalshi_map, poly_map, kalshi_market_state, poly_market_state):
    print("Scanner active. Hunting for risk free margins...")

    while True:
        for bridge_key in matched_keys:
            kalshi_ticker = kalshi_map[bridge_key]["ticker"]
            poly_tokens = poly_map[bridge_key]["tokens"]

            poly_yes_token = poly_tokens[0]
            poly_no_token = poly_tokens[1]

            # print(f"Lock Status -> Kalshi: {kalshi_ticker in kalshi_market_state} | Poly YES: {poly_yes_token in poly_market_state} | Poly NO: {poly_no_token in poly_market_state}")
            # print(f"Poly State Keys: {list(poly_market_state.keys())}")

            if (kalshi_ticker in kalshi_market_state and
                poly_yes_token in poly_market_state and
                poly_no_token in poly_market_state):

                kalshi_yes_cost = kalshi_market_state[kalshi_ticker]["YES"]["ask"]
                kalshi_no_cost = kalshi_market_state[kalshi_ticker]["NO"]["ask"]

                poly_yes_cost = poly_market_state[poly_yes_token]["ask"]
                poly_no_cost = poly_market_state[poly_no_token]["ask"]

                if (kalshi_yes_cost <= 0.00 or kalshi_yes_cost >= 1.00) or \
                    (poly_no_cost <= 0.00 or poly_no_cost >= 1.00) or \
                    (kalshi_no_cost <= 0.00 or kalshi_no_cost >= 1.00) or \
                    (poly_yes_cost <= 0.00 or poly_yes_cost >= 1.00):

                    print(f"GATE BLOCKED | K_YES: {kalshi_yes_cost} | P_NO: {poly_no_cost} | K_NO: {kalshi_no_cost} | P_YES: {poly_yes_cost} ")
                    continue

                #Math Engine
                fee_buffer = 0.02

                scenario_a_cost = kalshi_yes_cost + poly_no_cost + fee_buffer
                scenario_b_cost = kalshi_no_cost + poly_yes_cost + fee_buffer

                print(f"Tracking {kalshi_ticker} | Cost A: ${scenario_a_cost:.2f} | Cost B: ${scenario_b_cost:.2f}")

                if scenario_a_cost < 1.00:
                    print(f"!!! ARB FOUND !!! Profit: $round(1.00 - scenario_a_cost, 2) | K_YES + P_NO")
                    print("Executing simultaneous cross-chain trades...")
                    
                    
                    price_per_share = poly_no_cost
                    

                    notional_target = 1.05
                    size_by_notional = math.ceil(notional_target / float(price_per_share))
                    trade_size = max(min_size_from_exchange, size_by_notional)
                    trade_size = round(trade_size, 2)
                    
                    print(f"DEBUG: Using trade_size {trade_size} (Min size from exchange: {min_size_from_exchange}")
                    
                    try:
                        results = await asyncio.gather(
                            execute_kalshi_buy(kalshi_ticker, "yes", trade_size, kalshi_yes_cost),
                            execute_polymarket_buy(poly_no_token, "no", trade_size, poly_no_cost),
                            return_exceptions=True
                        )
                        print(f"Execution Pipeline Results: {results}")

                        print("Execution complete. Freezing scanning for 30 seconds to prevent duplicate fires...")
                        await asyncio.sleep(30)

                    except Exception as e:
                        print(f"Error during trade execution: {e}")

                if scenario_b_cost < 1.00:
                    print(f"!!! ARB FOUND !!! Profit: ${round(1.00 - scenario_b_cost, 2)} | K_NO + P_YES")
                    print("Executing simultaneous cross-chain trades...")
                    
                    
                    price_per_share = poly_yes_cost
                    

                    notional_target = 1.05
                    size_by_notional = math.ceil(notional_target / float(price_per_share))
                    trade_size = max(min_size_from_exchange, size_by_notional)
                    trade_size = round(trade_size, 2)
                    
                    print(f"DEBUG: Using trade_size {trade_size} (Min size from exchange: {min_size_from_exchange}") 
                    
                    try:
                        print("Firing execution leg...")
                        results = await asyncio.gather(
                            execute_kalshi_buy(kalshi_ticker, "no", trade_size, kalshi_no_cost),
                            execute_polymarket_buy(poly_yes_token, "yes", trade_size, poly_yes_cost),
                            return_exceptions=True
                        )
                        for i, result in enumerate(results):
                            if isinstance(result, Exception):
                                print(f"Leg {i} Failed: {result}")
                            else:
                                print(f"Trade execution for leg {i} successful: {result}")
                        

                        print("Execution complete. Freezing scanning for 30 seconds to prevent duplicate fires...")
                        await asyncio.sleep(30)

                    except Exception as e:
                        print(f"Error during trade execution: {e}")

        await asyncio.sleep(1)