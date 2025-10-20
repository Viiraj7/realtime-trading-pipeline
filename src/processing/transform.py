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
        pd.DataFrame: A DataFrame indexed by [symbol, timestamp]
                      with columns [open, high, low, close, volume].
                      Returns an empty DataFrame if the input is empty.
    """
    if not trades:
        logging.info("Trade buffer is empty, no data to transform.")
        return pd.DataFrame()

    try:
        # 1. Convert list of dicts to a DataFrame for high-speed processing
        df = pd.DataFrame(trades)

        # 2. Convert raw data types
        # 't' (timestamp) is in milliseconds, convert to full datetime objects
        df['timestamp'] = pd.to_datetime(df['t'], unit='ms', utc=True)
        # 'p' (price) and 'v' (volume) might be strings, convert to numbers
        df['p'] = pd.to_numeric(df['p'])
        df['v'] = pd.to_numeric(df['v'])

        # 3. Set the timestamp as the index, which is required for resampling
        df = df.set_index('timestamp')

        # 4. This is the core logic.
        # We group by the symbol ('s') and then "resample" (or "bucket")
        # all trades into 1-minute ('1T') windows.
        
        # Define the aggregation rules:
        # For price ('p'): get the 'ohlc' (open, high, low, close)
        # For volume ('v'): get the 'sum'
        aggregations = {
            'p': 'ohlc',
            'v': 'sum'
        }

        ohlc_df = df.groupby('s').resample('1T').agg(aggregations)

        # 5. Clean up the resulting DataFrame
        
        # After the .agg(), the columns are hierarchical (e.g., ('p', 'open'), ('v', 'sum'))
        # Let's flatten them to 'open', 'high', 'low', 'close', 'volume'
        ohlc_df.columns = ohlc_df.columns.droplevel(0)
        ohlc_df = ohlc_df.rename(columns={'': 'volume'}) # The 'v' col becomes nameless

        # Remove any 1-minute buckets that had no trades
        ohlc_df = ohlc_df.dropna()

        # Reset the index so 'symbol' and 'timestamp' become regular columns
        ohlc_df = ohlc_df.reset_index().rename(columns={'s': 'symbol'})

        if not ohlc_df.empty:
            logging.info(f"Transformed {len(trades)} trades into {len(ohlc_df)} OHLC bars.")
        
        return ohlc_df

    except Exception as e:
        logging.error(f"Error during data transformation: {e}")
        # Return an empty DataFrame on failure
        return pd.DataFrame()