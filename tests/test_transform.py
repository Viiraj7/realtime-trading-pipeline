import pandas as pd
from pandas.testing import assert_frame_equal
from src.processing.transform import aggregate_trades_to_ohlc

def test_transform_logic_handles_multiple_tickers_and_minutes():
    """
    This is our main test.
    It checks if the function correctly aggregates trades for
    multiple tickers (AAPL, MSFT) across different minutes.
    """

    # 1. ARRANGE: Create a list of fake raw trade data
    # (Timestamps are in milliseconds)
    raw_trades_list = [
        # --- Ticker: 'AAPL', 1st minute (13:20) ---
        {'s': 'AAPL', 'p': 150.00, 'v': 10, 't': 1678886401000}, # 13:20:01 (Open)
        {'s': 'AAPL', 'p': 150.50, 'v': 5,  't': 1678886420000}, # 13:20:20 (High)
        {'s': 'AAPL', 'p': 149.50, 'v': 20, 't': 1678886430000}, # 13:20:30 (Low)
        {'s': 'AAPL', 'p': 150.25, 'v': 15, 't': 1678886459000}, # 13:20:59 (Close)

        # --- Ticker: 'MSFT', 1st minute (13:20) ---
        {'s': 'MSFT', 'p': 300.00, 'v': 100, 't': 1678886405000}, # 13:20:05 (Open, Low)
        {'s': 'MSFT', 'p': 301.00, 'v': 50,  't': 1678886440000}, # 13:20:40 (High, Close)

        # --- Ticker: 'AAPL', 2nd minute (13:21) ---
        {'s': 'AAPL', 'p': 151.00, 'v': 30, 't': 1678886461000}  # 13:21:01 (Open, High, Low, Close)
    ]

    # 2. ACT: Run the function we are testing
    result_df = aggregate_trades_to_ohlc(raw_trades_list)

    # 3. ASSERT: Define the exact, correct output we expect
    expected_data = {
        'symbol':    ['AAPL', 'AAPL', 'MSFT'],
        'timestamp': pd.to_datetime(['2023-03-15T13:20:00Z', '2023-03-15T13:21:00Z', '2023-03-15T13:20:00Z']),
        'open':      [150.00, 151.00, 300.00],
        'high':      [150.50, 151.00, 301.00],
        'low':       [149.50, 151.00, 300.00],
        'close':     [150.25, 151.00, 301.00],
        # --- THIS IS THE FIX ---
        # Volumes are now integers to match the output dtype
        'volume':    [50, 30, 150]
        # ---------------------
    }

    # Create the DataFrame and sort it to have a predictable order for testing
    expected_df = pd.DataFrame(expected_data)
    expected_df = expected_df.set_index(['symbol', 'timestamp']).sort_index()

    # Also sort our result to match the expected order
    result_df = result_df.set_index(['symbol', 'timestamp']).sort_index()

    # Use pandas' special testing function to see if they are identical
    # check_dtype=False can sometimes help if minor dtype issues persist
    assert_frame_equal(result_df, expected_df, check_dtype=True)

def test_transform_empty_list():
    """Checks if the function correctly handles an empty list."""
    # 1. Arrange