import pandas as pd
from sqlalchemy.engine import Engine
from sqlalchemy import text # Needed for executing raw SQL safely
import logging
import uuid # To create unique temporary table names

logger = logging.getLogger(__name__)

def load_data_to_db(df: pd.DataFrame, engine: Engine):
    """
    Upserts (inserts or updates) a DataFrame containing OHLC data into
    the 'stock_bars_1min' table using a temporary staging table.

    Args:
        df (pd.DataFrame): The DataFrame to load. Expected columns are
                           ['timestamp', 'symbol', 'open', 'high', 'low', 'close', 'volume'].
        engine (Engine): The SQLAlchemy engine instance for database connection.
    """
    if df.empty:
        logger.info("Received empty DataFrame. Nothing to load.")
        return

    main_table_name = 'stock_bars_1min'
    # Create a unique temporary table name to avoid conflicts if multiple workers run
    temp_table_name = f"temp_staging_{uuid.uuid4().hex[:8]}"

    logger.info(f"Attempting to upsert {len(df)} rows into '{main_table_name}' via temp table '{temp_table_name}'...")

    try:
        # Ensure correct column order
        df_to_load = df[['timestamp', 'symbol', 'open', 'high', 'low', 'close', 'volume']]

        # Use a transaction block (ensures atomicity)
        with engine.begin() as connection:
            # 1. Load data into the temporary table
            #    `if_exists='replace'` ensures the temp table is fresh each time
            df_to_load.to_sql(
                name=temp_table_name,
                con=connection,
                if_exists='replace',
                index=False,
                method='multi'
            )
            logger.info(f"Loaded {len(df_to_load)} rows into temporary table '{temp_table_name}'.")

            # 2. Construct the UPSERT SQL command
            #    This command tells PostgreSQL what to do if a conflict occurs on the primary key
            upsert_sql = text(f"""
                INSERT INTO {main_table_name} (timestamp, symbol, open, high, low, close, volume)
                SELECT timestamp, symbol, open, high, low, close, volume
                FROM {temp_table_name}
                ON CONFLICT (timestamp, symbol) -- If a row with the same timestamp and symbol exists...
                DO UPDATE SET                   -- ...then update the existing row with the new values
                    open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
                    volume = EXCLUDED.volume;
            """)

            # 3. Execute the UPSERT command
            result = connection.execute(upsert_sql)
            logger.info(f"Upsert command executed. {result.rowcount} rows affected in '{main_table_name}'.")

            # 4. Drop the temporary table (optional, but good practice)
            #    We don't strictly need this inside the transaction, but let's keep it clean
            # drop_temp_sql = text(f"DROP TABLE IF EXISTS {temp_table_name};")
            # connection.execute(drop_temp_sql)
            # logger.info(f"Dropped temporary table '{temp_table_name}'.")

        # Transaction automatically commits here if no errors occurred

    except Exception as e:
        logger.error(f"Error during upsert process: {e}", exc_info=True)
        # Transaction automatically rolls back here if an error occurred

    finally:
        # Ensure temporary table is dropped even if errors occurred outside the transaction
        try:
            with engine.connect() as connection:
                with connection.begin(): # Need another transaction for DDL
                     drop_temp_sql = text(f"DROP TABLE IF EXISTS {temp_table_name};")
                     connection.execute(drop_temp_sql)
                     logger.debug(f"Ensured temporary table '{temp_table_name}' is dropped.")
        except Exception as drop_e:
             logger.error(f"Error dropping temporary table '{temp_table_name}': {drop_e}")