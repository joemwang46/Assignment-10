import pandas as pd
from data_loader import load_market_data, clean_data

def test_clean_data_validates_tickers(tmp_path):
    # Create sample CSVs
    market = tmp_path / "market.csv"
    tickers = tmp_path / "tickers.csv"

    pd.DataFrame({
        "timestamp": ["2025-01-01 09:30:00"],
        "ticker": ["AAPL"],
        "open": [100], "high": [105], "low": [99], "close": [102], "volume": [500]
    }).to_csv(market, index=False)

    pd.DataFrame({"symbol": ["AAPL"]}).to_csv(tickers, index=False)

    df = clean_data(load_market_data(market), tickers)

    # Check cleaned columns exist
    assert "timestamp" in df.columns
    assert "ticker" in df.columns
    assert df["ticker"].iloc[0] == "AAPL"
