import pandas as pd

def load_market_data(filename: str) -> pd.DataFrame:
    df = pd.read_csv(filename)
    return df

def validate_missing_data(df: pd.DataFrame) -> bool:
    return not df.isnull().values.any()

def validate_tickers(df: pd.DataFrame, tickers_file: str) -> bool:
    tickers_df = pd.read_csv(tickers_file)
    valid_tickers = tickers_df['symbol'].tolist()
    tickers = df['ticker'].unique()
    is_valid = all(ticker in valid_tickers for ticker in tickers)
    return is_valid

def normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [col.strip().lower().replace(' ', '_') for col in df.columns]
    return df

def normalize_datetime_format(df: pd.DataFrame) -> pd.DataFrame:
    df['timestamp'] = pd.to_datetime(df['timestamp']).dt.strftime('%Y-%m-%d %H:%M:%S')
    return df

def clean_data(df: pd.DataFrame, tickers_file: str) -> pd.DataFrame:
    if not validate_missing_data(df):
        raise ValueError("Data contains missing values.")
    if not validate_tickers(df, tickers_file):
        raise ValueError("Data contains invalid tickers.")  
    df = normalize_column_names(df)
    df = normalize_datetime_format(df)
    return df