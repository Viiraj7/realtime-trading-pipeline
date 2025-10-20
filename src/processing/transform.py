import pandas as pd
import logging

def aggregate_trades_to_ohlc(trades: list) -> pd.DataFrame:
    """
    Aggregates a list of raw trade dictionaries into a 1-minute OHLCV DataFrame.
    
    Args:
        trades (list): A list of trade dictionaries. 
                       Each dict is expected to have:
                       's' (symbol), 
                       'p' (price), 
                       't' (timestamp in milliseconds), 
                       'v' (volume).

    Returns:
        pd.DataFrame: A DataFrame with columns 
                      [symbol, timestamp, open, high, low, close, volume].
                      Returns an empty DataFrame if the input is empty.
    """
    if not trades:
        logging.info("Trade buffer is empty, no data to transform.")
        return pd.DataFrame()

    try:
        # 1. Convert list of dicts to a DataFrame for high-speed processing
        df = pd.DataFrame(trades)

        # 2. Convert raw data types
        df['timestamp'] = pd.to_datetime(df['t'], unit='ms', utc=True)
        df['p'] = pd.to_numeric(df['p'])
        df['v'] = pd.to_numeric(df['v'])

        # 3. Set the timestamp as the index
        df = df.set_index('timestamp')

        # 4. Group by symbol and resample/aggregate
        aggregations = {
            'p': 'ohlc', # Creates open, high, low, close from 'p'
            'v': 'sum'   # Creates sum from 'v'
        }
        ohlc_df = df.groupby('s').resample('1min').agg(aggregations)

        # 5. Clean up the resulting DataFrame
        
        # Flatten the MultiIndex columns (e.g., ('p', 'open') -> 'open')
        ohlc_df.columns = ohlc_df.columns.droplevel(0)

        # --- THIS IS THE FIX ---
        # Rename the 'v' column (which came from 'sum') to 'volume'
        ohlc_df = ohlc_df.rename(columns={'v': 'volume'})
        # ---------------------

        # Remove any 1-minute buckets that had no trades
        ohlc_df = ohlc_df.dropna()

        # Reset the index so 'symbol' ('s') and 'timestamp' become regular columns
        ohlc_df = ohlc_df.reset_index().rename(columns={'s': 'symbol'})

        if not ohlc_df.empty:
            logging.info(f"Transformed {len(trades)} trades into {len(ohlc_df)} OHLC bars.")
        
        return ohlc_df

    except Exception as e:
        logging.error(f"Error during data transformation: {e}")
        # Return an empty DataFrame on failure
        return pd.DataFrame()