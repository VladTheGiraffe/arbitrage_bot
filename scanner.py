#Imports
import asyncio

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