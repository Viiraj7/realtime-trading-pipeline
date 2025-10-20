import os
from dotenv import load_dotenv
import logging

# This line finds the .env file in your root folder and loads it.
# Now, os.environ.get("DB_USER") will work.
load_dotenv()

# Basic logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logging.info("Configuration file loaded.")

# --- Finnhub API ---
FINNHUB_API_KEY = os.environ.get("FINNHUB_API_KEY")
if not FINNHUB_API_KEY:
    # Use logging to show a warning if the key is missing
    logging.warning("FINNHUB_API_KEY is not set in .env file.")

# --- PostgreSQL Database ---
DB_USER = os.environ.get("DB_USER")
DB_PASSWORD = os.environ.get("DB_PASSWORD")
DB_HOST = os.environ.get("DB_HOST")
DB_PORT = os.environ.get("DB_PORT")
DB_NAME = os.environ.get("DB_NAME")

# --- Kafka Broker ---
# Make sure this line exists and is spelled correctly
KAFKA_SERVER = os.environ.get("KAFKA_SERVER") # <--- THIS LINE IS IMPORTANT
# ----------------------------------------------
KAFKA_TOPIC_RAW = "raw_trades" # This is the name of our "conveyor belt"

# --- Data Processing ---
# How many trades to process at once in a batch
CONSUMER_BATCH_SIZE = 100
# How long to wait (in ms) for new messages before processing a batch
CONSUMER_BATCH_TIMEOUT_MS = 5000 # 5 seconds