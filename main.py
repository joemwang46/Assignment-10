from data_loader import load_market_data, clean_data
from sqlite_storage import create_db, insert_data
from sqlite_storage import SQLiteStorage
from parquet_storage import ParquetStorage
import time

def timed(label, func, *args, **kwargs):
    start = time.perf_counter()
    result = func(*args, **kwargs)
    end = time.perf_counter()
    print(f"{label} took {end - start:.6f} seconds\n")
    return result


def __main__():
    df = clean_data(load_market_data('market_data_multi.csv'), 'tickers.csv')

    db_path = 'market_data.db'
    #create_db(db_path)
    #insert_data(df, db_path)

    sql = SQLiteStorage(db_path)
    print("Top 3 weekly returns:")
    top_3 = timed("SQL top_3_weekly_returns",
                sql.top_3_weekly_returns,
                '2025-11-17', '2025-11-21')
    for row in top_3:
        print(row)

    print("Ticker data for AAPL from 2025-11-17 to 2025-11-21:")
    ticker_data = timed("SQL get_ticker_data",
                        sql.get_ticker_data,
                        'AAPL', '2025-11-17', '2025-11-21')
    for row in ticker_data:
        #print(row)
        pass

    print("Daily open/close per ticker:")
    daily = timed("SQL daily_open_close", sql.daily_open_close)
    for row in daily:
        print(row)

    print("AVG Daily Volume:")
    avg_vol = timed("SQL avg_daily_volume", sql.avg_daily_volume)
    for row in avg_vol:
        print(row)
    
    parquet_dir = 'market_data'
    parquet = ParquetStorage(parquet_dir)
    #parquet.write_partitioned(df)
    all_data = timed("PARQUET load_all", parquet.load_all)
    print(f"\nLoaded {len(all_data)} rows from Parquet storage.")

    ticker_data = timed("PARQUET get_ticker_data",
                        parquet.get_ticker_data,
                        'AAPL', '2025-11-17', '2025-11-21')
    print(ticker_data)

    rolling_vol = timed("PARQUET rolling_volatility",
                        parquet.rolling_volatility,
                        window=5)
    print(rolling_vol)


if __name__ == "__main__":
    __main__()