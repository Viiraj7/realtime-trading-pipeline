import json
import time
import logging
from kafka import KafkaConsumer
from src.config import ( # Import settings from our config file
    KAFKA_SERVER,
    KAFKA_TOPIC_RAW,
    CONSUMER_BATCH_SIZE,
    CONSUMER_BATCH_TIMEOUT_MS
)
from src.database import get_db_engine # Import our database connection function
from src.processing.transform import aggregate_trades_to_ohlc # Import our transformation logic
from src.processing.loader import load_data_to_db # Import our database loading function

# Configure logging for this worker
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_kafka_consumer():
    """
    Creates and returns a KafkaConsumer.
    Will retry connection indefinitely if Kafka is not available.
    """
    consumer = None
    while consumer is None:
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC_RAW,                     # The topic to read from
                bootstrap_servers=[KAFKA_SERVER],    # Kafka server address
                # Decode the message value from bytes to dict
                value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                auto_offset_reset='earliest',        # Start reading from the oldest message if no offset saved
                # Stop waiting for messages after this timeout (in ms)
                consumer_timeout_ms=CONSUMER_BATCH_TIMEOUT_MS,
                # Other settings for reliability/performance
                group_id='ohlc-aggregator-group'     # Allows multiple workers to share the load later
            )
            logger.info(f"Kafka Consumer connected successfully to {KAFKA_SERVER}.")
            return consumer
        except Exception as e:
            logger.error(f"Error connecting to Kafka: {e}. Retrying in 5 seconds...")
            time.sleep(5)

def main():
    """Main function to run the ETL worker loop."""
    logger.info("Starting ETL Worker...")

    consumer = create_kafka_consumer()
    engine = get_db_engine() # Get the shared database engine

    # This buffer will hold messages until we reach the batch size or timeout
    trade_buffer = []

    while True: # Run forever
        try:
            logger.debug("Checking for new messages...")
            # Clear the buffer for the new batch
            trade_buffer.clear()

            # The consumer will automatically fetch messages up to the timeout
            for message in consumer:
                trade_buffer.append(message.value) # Add the trade data (dict) to the buffer
                logger.debug(f"Added message to buffer: {message.value}")
                # If we hit the batch size, stop collecting and process immediately
                if len(trade_buffer) >= CONSUMER_BATCH_SIZE:
                    logger.info(f"Batch size reached ({len(trade_buffer)}). Processing...")
                    break # Exit the 'for message' loop

            # Process the buffer if it has any messages (either batch size reached or timeout occurred)
            if trade_buffer:
                logger.info(f"Processing batch of {len(trade_buffer)} trades.")

                # ---- 1. TRANSFORM ----
                # Call the function from transform.py
                ohlc_df = aggregate_trades_to_ohlc(trade_buffer)

                # ---- 2. LOAD ----
                if not ohlc_df.empty:
                    # Call the function from loader.py
                    load_data_to_db(ohlc_df, engine)
                else:
                    logger.info("Transformation resulted in an empty DataFrame. No data loaded.")

            else:
                # This means the consumer timed out with no new messages
                logger.info(f"No new messages received within the {CONSUMER_BATCH_TIMEOUT_MS}ms timeout. Waiting...")

            # Optional: Commit offsets manually if needed (usually handled automatically)
            # consumer.commit()

        except Exception as e:
            logger.error(f"An error occurred in the main ETL loop: {e}", exc_info=True)
            # Wait a bit before retrying after an error
            time.sleep(5)

if __name__ == "__main__":
    main()