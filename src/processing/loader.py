import pandas as pd
from sqlalchemy.engine import Engine # Used for type hinting
import logging

# Set up logging for this module
logger = logging.getLogger(__name__) # Use __name__ for logger identification

def load_data_to_db(df: pd.DataFrame, engine: Engine):
    """
    Appends a DataFrame containing OHLC data to the 'stock_bars_1min' table.

    Args:
        df (pd.DataFrame): The DataFrame to load. Expected columns are
                           ['timestamp', 'symbol', 'open', 'high', 'low', 'close', 'volume'].
        engine (Engine): The SQLAlchemy engine instance for database connection.
    """
    if df.empty:
        logger.info("Received empty DataFrame. Nothing to load.")
        return

    table_name = 'stock_bars_1min'
    logger.info(f"Attempting to load {len(df)} rows into '{table_name}'...")

    try:
        # Ensure the DataFrame columns are in the exact order expected by the database table
        # This is good practice although `to_sql` can often handle different orders.
        df_to_load = df[[
            'timestamp', 'symbol', 'open', 'high', 'low', 'close', 'volume'
        ]]

        # Use pandas `to_sql` method for efficient loading
        # - `if_exists='append'` adds the new rows without dropping the table.
        # - `index=False` tells pandas not to write the DataFrame index as a column.
        # - `method='multi'` often speeds up inserts for PostgreSQL.
        df_to_load.to_sql(
            name=table_name,
            con=engine,
            if_exists='append',
            index=False,
            method='multi'
        )
        logger.info(f"Successfully loaded {len(df_to_load)} rows into '{table_name}'.")

    except Exception as e:
        logger.error(f"Error loading data into '{table_name}': {e}")
        # In a production system, you might:
        # - Retry the operation a few times.
        # - Send the failed DataFrame to a "dead-letter queue" or error log file.
        # - Raise the exception if it's critical.
        # For now, we just log the error.