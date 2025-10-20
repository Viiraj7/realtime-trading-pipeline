import websocket # For WebSocket connection
import json      # For handling JSON messages
import time      # For pausing during retries
import logging   # For logging messages
from kafka import KafkaProducer # For sending data to Kafka
from src.config import KAFKA_SERVER, KAFKA_TOPIC_RAW, FINNHUB_API_KEY # Our settings

# Set up basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def create_kafka_producer():
    """
    Creates and returns a KafkaProducer.
    Will retry connection indefinitely if Kafka is not available.
    """
    producer = None
    while producer is None:
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_SERVER],
                # We serialize the value as a JSON string, then encode to bytes
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            logging.info("Kafka Producer connected successfully.")
            return producer
        except Exception as e:
            logging.error(f"Error connecting to Kafka: {e}. Retrying in 5 seconds...")
            time.sleep(5)

def on_message(ws, message, producer):
    """
    Callback function executed when a message is received from the WebSocket.
    Sends the trade data to the Kafka topic.
    """
    try:
        data_list = json.loads(message)
        # Finnhub sends messages in batches, sometimes just a 'ping'
        if data_list['type'] == 'trade':
            for trade in data_list['data']:
                # 's' = symbol, 'p' = price, 't' = timestamp (ms), 'v' = volume
                logging.info(f"Received trade: {trade}")
                # Send the raw trade dictionary to our Kafka topic
                producer.send(KAFKA_TOPIC_RAW, value=trade)
        elif data_list['type'] == 'ping':
            logging.info("Received ping from Finnhub.")
        else:
            logging.warning(f"Received unknown message type: {data_list.get('type')}")
    except json.JSONDecodeError:
        logging.error(f"Failed to decode JSON message: {message}")
    except Exception as e:
        logging.error(f"Error processing message: {e}")

def on_error(ws, error):
    """Callback function executed on WebSocket error."""
    logging.error(f"WebSocket Error: {error}")

def on_close(ws, close_status_code, close_msg):
    """Callback function executed when WebSocket connection is closed."""
    logging.info(f"WebSocket closed with code {close_status_code}: {close_msg}")
    # You might want to implement reconnection logic here in a real application

def on_open(ws):
    """Callback function executed when WebSocket connection is opened."""
    logging.info("WebSocket connection opened.")
    # Subscribe to trades for specific tickers
    tickers = ["AAPL", "MSFT", "AMZN", "BINANCE:BTCUSDT", "BINANCE:ETHUSDT"]
    for ticker in tickers:
        subscribe_message = json.dumps({"type": "subscribe", "symbol": ticker})
        ws.send(subscribe_message)
        logging.info(f"Subscribed to {ticker}")

def main():
    """Main function to start the producer."""
    if not FINNHUB_API_KEY:
        logging.error("FINNHUB_API_KEY not found in environment variables. Exiting.")
        return

    # Create the Kafka producer (this will block until Kafka is ready)
    producer = create_kafka_producer()

    # Define the WebSocket URL
    ws_url = f"wss://ws.finnhub.io?token={FINNHUB_API_KEY}"

    # Set up the WebSocketApp
    # Note: We pass the 'producer' instance to the on_message callback
    ws_app = websocket.WebSocketApp(
        ws_url,
        on_open=on_open,
        on_message=lambda ws, msg: on_message(ws, msg, producer),
        on_error=on_error,
        on_close=on_close
    )

    logging.info(f"Connecting to WebSocket at {ws_url}...")
    # This runs the WebSocket connection forever, handling reconnects automatically
    ws_app.run_forever(ping_interval=60, ping_timeout=10)

if __name__ == "__main__":
    # This ensures the main() function runs only when the script is executed directly
    main()