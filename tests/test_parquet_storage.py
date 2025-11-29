import pandas as pd
from parquet_storage import ParquetStorage

def sample_df():
    return pd.DataFrame({
        "timestamp": pd.to_datetime([
            "2025-01-01 09:30:00",
            "2025-01-01 09:31:00",
            "2025-01-02 09:30:00"
        ]),
        "ticker": ["AAPL", "AAPL", "AAPL"],
        "open": [100, 101, 102],
        "high": [105, 106, 107],
        "low":  [99, 100, 101],
        "close": [102, 103, 104],
        "volume": [500, 600, 700]
    })

def test_parquet_write_and_read(tmp_path):
    df = sample_df()
    p = ParquetStorage(tmp_path)

    p.write_partitioned(df)
    loaded = p.load_all()

    assert len(loaded) == len(df)
    assert "ticker" in loaded.columns

def test_parquet_ticker_filter(tmp_path):
    df = sample_df()
    p = ParquetStorage(tmp_path)
    p.write_partitioned(df)

    out = p.get_ticker_data("AAPL", "2025-01-01", "2025-01-02")
    assert len(out) == 2

def test_parquet_rolling_volatility(tmp_path):
    df = sample_df()
    p = ParquetStorage(tmp_path)
    p.write_partitioned(df)

    vol = p.rolling_volatility(window=2)

    # Should have NaN for first row then valid std
    assert vol["rolling_vol"].iloc[0] != vol["rolling_vol"].iloc[1]
    assert "rolling_vol" in vol.columns
