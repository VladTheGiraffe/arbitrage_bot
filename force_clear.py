import asyncio

from poly_client import execute_polymarket_sell

async def manual_liquidation():
    print("Initiating manual backend liquidation... ")
    target_token = input("Enter Token ID: ").strip()
    direction = input("Enter Direction (yes/no): ").strip().lower()

    shares_to_dump = int(input("Enter number of shares: ").strip())

    estimated_cost = float(input("Enter current cost per share (e.g., 0.44): ").strip())

    print(f"\n[!] Armed: Dumping {shares_to_dump} shares of {direction.upper()}... ")
    print("[!] Executing direct API strike bypassing UI...")

    slippage = 0.01

    await execute_polymarket_sell(target_token, direction, shares_to_dump, estimated_cost, slippage)

    print("\n[+] Payload Fired.")

if __name__ == "__main__":
    asyncio.run(manual_liquidation())