import asyncio
import json
import os
from datetime import datetime, timezone
import sqlite3
import aiosqlite
import websockets
from dotenv import load_dotenv
import matplotlib.pyplot as plt

load_dotenv()

DB_PATH = "trades.db"


async def subscribe_to_trades(websocket, api_key):
    """Subscribes to BTC/USD trades on CoinAPI."""
    subscribe_message = {
        "type": "hello",
        "apikey": api_key,
        "heartbeat": False,
        "subscribe_data_type": ["trade"],
        "subscribe_filter_symbol_id": ["BITSTAMP_SPOT_BTC_USD"],
    }
    await websocket.send(json.dumps(subscribe_message))


async def process_trade(trade_data, cursor):
    """Processes a trade message and stores it in the database."""
    try:
        time_exchange_str = trade_data["time_exchange"]
        symbol_id = trade_data["symbol_id"]
        price = trade_data["price"]
        size = trade_data["size"]
        taker_side = trade_data["taker_side"]

        # Convert and localize to UTC for printing
        time_exchange = datetime.fromisoformat(
            time_exchange_str.replace("Z", "+00:00")
        ).replace(tzinfo=timezone.utc)

        # Format price and size
        formatted_price = f"{price:,.2f}"
        formatted_size = f"{size:.5f}"

        await cursor.execute(
            """
            INSERT INTO trades (time_exchange, symbol_id, price, size, taker_side)
            VALUES (?, ?, ?, ?, ?)
        """,
            (time_exchange_str, symbol_id, price, size, taker_side),
        )

        print(
            f"BTC/USD Trade|Symbol:{symbol_id}|Taker_Side:{taker_side}|Price:{formatted_price}|Size:{formatted_size}|Time:{time_exchange}"
        )
    except sqlite3.Error as e:
        print(f"Database error: {e}")


async def visualize_data(conn):
    """Fetches data from the database, converts time, and plots a line chart of prices."""
    async with conn.execute("SELECT time_exchange, price FROM trades") as cursor:
        rows = await cursor.fetchall()

    # Parse timestamps as datetime objects directly from the database results
    times = [
        datetime.fromisoformat(row[0].replace("Z", "+00:00")).replace(
            tzinfo=timezone.utc
        )
        for row in rows
    ]
    prices = [row[1] for row in rows]

    plt.figure(figsize=(12, 6))  # Adjust the size as needed
    plt.plot(times, prices, marker="o", linestyle="-")
    plt.title("BTC/USD Price Over Time")
    plt.xlabel("Time")
    plt.ylabel("Price (USD)")
    plt.xticks(rotation=45)  # Rotate x-axis labels for readability
    plt.grid(axis="y")
    plt.tight_layout()
    plt.show()


async def main():
    """Main function to run the ticker."""
    uri = "wss://ws.coinapi.io/v1/"
    api_key = os.getenv("COINAPI_API_KEY")

    async with aiosqlite.connect(DB_PATH) as conn:
        cursor = await conn.cursor()

        # Create the trades table if it doesn't exist
        await cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                time_exchange TEXT,
                symbol_id TEXT,
                price REAL,
                size REAL,
                taker_side TEXT
            )
        """
        )
        await conn.commit()

        task = asyncio.create_task(websocket_loop(uri, api_key, cursor))

        # Handle Ctrl+C (KeyboardInterrupt) gracefully
        try:
            await task
        except KeyboardInterrupt:
            print("\nStopping the script. Please wait for visualization...")

        # Visualize the data after stopping
        await visualize_data(conn)


async def websocket_loop(uri, api_key, cursor):
    """Handle the WebSocket connection in a separate loop."""
    try:
        async with websockets.connect(uri) as websocket:
            await subscribe_to_trades(websocket, api_key)

            while True:
                try:
                    data = await websocket.recv()
                    trade_data = json.loads(data)

                    if trade_data["type"] == "trade":
                        await process_trade(trade_data, cursor)
                except websockets.ConnectionClosed:
                    print("WebSocket connection closed.")
                    break
                except json.JSONDecodeError as e:
                    print(f"JSON decoding error: {e}")
    except asyncio.CancelledError:
        print("Task cancelled🚫. Plotting the graph📈")
    finally:
        # Perform any necessary cleanup here if needed
        pass


if __name__ == "__main__":
    asyncio.run(main())
