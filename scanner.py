#Imports
import asyncio
import time
import math
import os
import uuid
from datetime import datetime
from poly_client import execute_polymarket_buy, get_poly_balance, execute_polymarket_sell, check_poly_inventory
from kalshi_client import execute_kalshi_buy, get_kalshi_balance, execute_kalshi_sell, check_kalshi_order

POLY_MIN_SHARES = 5.0
MIN_KALSHI_BALANCE = float(os.getenv("MIN_KALSHI_BALANCE", 5.00))
MIN_POLY_BALANCE = float(os.getenv("MIN_POLY_BALANCE", 5.00))

async def has_sufficient_funds(trade_size, k_price, p_price):
    kalshi_cost = trade_size * k_price
    poly_cost = trade_size * p_price

    k_bal = await get_kalshi_balance()
    p_bal = await get_poly_balance()

    if k_bal < (kalshi_cost + MIN_KALSHI_BALANCE) or p_bal < (poly_cost + MIN_POLY_BALANCE):
        print(f"Gate Blocked: Insufficient funds for trade. K_bal: ${k_bal:.2f}, P_bal: ${p_bal:.2f}")
        return False

    return True

#Arbitrage Scanner
async def arbitrage_scanner(matched_keys, kalshi_map, poly_map, kalshi_market_state, poly_market_state):
    print("Scanner active. Hunting for risk free margins...")

    while True:
        print(f"Tracking {len(matched_keys)} markets...", end="\r")
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

                    print(f"\n--- RAW DATA DUMP ---")
                    print(f"Kalshi State: {kalshi_market_state[kalshi_ticker]}")
                    print(f"Poly State: {poly_market_state[poly_yes_token]}, {poly_market_state[poly_no_token]}")
                    print(f"--- END RAW DATA DUMP ---\n")

                    print(f"GATE BLOCKED | K_YES: {kalshi_yes_cost} | P_NO: {poly_no_cost} | K_NO: {kalshi_no_cost} | P_YES: {poly_yes_cost} ")
                    continue

                #Math Engine
                fee_buffer = 0.02

                scenario_a_cost = kalshi_yes_cost + poly_no_cost + fee_buffer
                scenario_b_cost = kalshi_no_cost + poly_yes_cost + fee_buffer

                # print(f"Tracking {kalshi_ticker} | Cost A: ${scenario_a_cost:.2f} | Cost B: ${scenario_b_cost:.2f}")

                if scenario_a_cost < 1.00:
                    print(f"!!! ARB FOUND !!! Profit: ${round(1.00 - scenario_a_cost, 2)} | K_YES + P_NO")
                    print("Executing simultaneous cross-chain trades...")
                    
                    entry_slippage_cap = 0.01
                    
                    price_per_share = poly_no_cost
                    

                    notional_target = 1.05
                    size_by_notional = math.ceil(notional_target / float(price_per_share))
                    trade_size = max(POLY_MIN_SHARES, size_by_notional)
                    trade_size = int(trade_size)
                    
                    is_funded = await has_sufficient_funds(trade_size, kalshi_yes_cost, poly_no_cost)

                    if not is_funded:
                        continue

                    kalshi_order_id = str(uuid.uuid4())

                    try:
                        results = await asyncio.gather(
                            execute_kalshi_buy(kalshi_ticker, "yes", trade_size, kalshi_yes_cost, kalshi_order_id, entry_slippage_cap),
                            execute_polymarket_buy(poly_no_token, "no", trade_size, poly_no_cost, entry_slippage_cap),
                            return_exceptions=True
                        )
                        
                        kalshi_result = results[0]
                        poly_result = results[1]

                        if isinstance(kalshi_result, Exception) or kalshi_result.get("status") == 409:
                            print("Network drop detected on Kalshi leg. Interrogating API...")

                            kalshi_actually_filled = await check_kalshi_order(kalshi_order_id)

                            if kalshi_actually_filled:
                                print("State Reconciled: Kalshi trade executed in the dark. Canceling unwind.")
                                kalshi_result = {"success": True}

                    
                        if isinstance(poly_result, Exception):
                            print("Network drop detected on Poly leg. Checking inventory...")

                            poly_actually_filled = await check_poly_inventory(poly_no_token, trade_size)

                            if poly_actually_filled:
                                print("State Reconciled: Poly trade executed in the dark. Cancelling unwind.")
                                poly_result = {"success": True}

                        kalshi_filled = not isinstance(kalshi_result, Exception) and "error" not in kalshi_result
                        poly_filled = not isinstance(poly_result, Exception) and poly_result.get("success") is True

                        if kalshi_filled == poly_filled:
                            print("Execution Symmetrical. No unwind required.")

                        elif kalshi_filled and not poly_filled:
                            print("!!! ASYMMETRIC FILL: Unwinding Kalshi Leg !!!")
                            await execute_kalshi_sell(kalshi_ticker, "yes", trade_size, kalshi_yes_cost, 0.02, str(uuid.uuid4()))

                        elif poly_filled and not kalshi_filled:
                            print("!!! ASYMETTRIC FILL: Unwinding Poly Leg !!!")

                            max_retries = 5
                            retry_count = 0
                            tokens_arrived = False

                            while retry_count < max_retries and not tokens_arrived:
                                print(f"Checking wallet for tokens... (Attempt {retry_count + 1}/{max_retries})")

                                poly_actually_filled = await check_poly_inventory(poly_no_token, trade_size)

                                if poly_actually_filled:
                                    tokens_arrived = True
                                    print("Tokens settled on-chain. Firing execution leg..")
                                
                                    await execute_polymarket_sell(poly_no_token, "no", trade_size, poly_no_cost, poly_no_cost - 0.01)

                                else:
                                    retry_count += 1
                                    await asyncio.sleep(2)

                            if not tokens_arrived:
                                print("CRITICAL: Blockchain failed to settle tokens. Manual Unwind Required.")

                        print("Execution complete. Freezing scanning for 30 seconds to prevent duplicate fires...")
                        await asyncio.sleep(30)

                    except Exception as e:
                        print(f"Error during trade execution: {e}")

                if scenario_b_cost < 1.00:
                    print(f"!!! ARB FOUND !!! Profit: ${round(1.00 - scenario_b_cost, 2)} | K_NO + P_YES")
                    print("Executing simultaneous cross-chain trades...")
                    
                    entry_slippage_cap = 0.01

                    price_per_share = poly_yes_cost
                    

                    notional_target = 1.05
                    size_by_notional = math.ceil(notional_target / float(price_per_share))
                    trade_size = max(POLY_MIN_SHARES, size_by_notional)
                    trade_size = int(trade_size)
                    
                    is_funded = await has_sufficient_funds(trade_size, kalshi_no_cost, poly_yes_cost)

                    if not is_funded:
                        continue
                    

                    kalshi_order_id = str(uuid.uuid4())

                    try:
                        print("Firing execution leg...")
                        results = await asyncio.gather(
                            execute_kalshi_buy(kalshi_ticker, "no", trade_size, kalshi_no_cost, kalshi_order_id, entry_slippage_cap),
                            execute_polymarket_buy(poly_yes_token, "yes", trade_size, poly_yes_cost, entry_slippage_cap),
                            return_exceptions=True
                        )
                        

                        kalshi_result = results[0]
                        poly_result = results[1]

                        if isinstance(kalshi_result, Exception):
                            print("Network drop detected on Kalshi leg. Interrogating API...")

                            kalshi_actually_filled = await check_kalshi_order(kalshi_order_id)

                            if kalshi_actually_filled:
                                print("State Reconciled: Kalshi trade executed in the dark. Canceling unwind.")
                                kalshi_result = {"success": True}

                        if isinstance(poly_result, Exception):
                            print("Network drop detected on Poly leg. Checking inventory...")

                            poly_actually_filled = await check_poly_inventory(poly_yes_token, trade_size)

                            if poly_actually_filled:
                                print("State Reconciled: Poly trade executed in the dark. Cancelling unwind.")
                                poly_result = {"success": True}

                        kalshi_filled = not isinstance(kalshi_result, Exception) and "error" not in kalshi_result
                        poly_filled = not isinstance(poly_result, Exception) and poly_result.get("success") is True

                        if kalshi_filled == poly_filled:
                            print("Execution Symmetrical. No unwind required.")

                        elif kalshi_filled and not poly_filled:
                            print("!!! ASYMMETRIC FILL: Unwinding Kalshi Leg !!!")
                            await execute_kalshi_sell(kalshi_ticker, "no", trade_size, kalshi_no_cost, 0.02, str(uuid.uuid4()))

                        elif poly_filled and not kalshi_filled:
                            print("!!! ASYMETTRIC FILL: Unwinding Poly Leg !!!")
                            
                            max_retries = 5
                            retry_count = 0
                            tokens_arrived = False

                            while retry_count < max_retries and not tokens_arrived:
                                print(f"Checking wallet for tokens... (Attempt {retry_count + 1}/{max_retries})")

                                poly_actually_filled = await check_poly_inventory(poly_yes_token, trade_size)

                                if poly_actually_filled:
                                    tokens_arrived = True
                                    print("Tokens settled on-chain. Firing execution leg..")
                                
                                    await execute_polymarket_sell(poly_yes_token, "yes", trade_size, poly_yes_cost, poly_yes_cost - 0.01)

                                else:
                                    retry_count += 1
                                    await asyncio.sleep(2)

                            if not tokens_arrived:
                                print("CRITICAL: Blockchain failed to settle tokens. Manual Unwind Required.")
                            

                        print("Execution complete. Freezing scanning for 30 seconds to prevent duplicate fires...")
                        await asyncio.sleep(30)

                    except Exception as e:
                        print(f"Error during trade execution: {e}")

        await asyncio.sleep(1)