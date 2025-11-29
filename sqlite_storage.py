import sqlite3
from typing import List, Tuple

class SQLiteStorage:
    def __init__(self, db_path: str):
        self.db_path = db_path

    def _connect(self):
        return sqlite3.connect(self.db_path)

    def get_ticker_data(self, symbol: str, start: str, end: str):
        query = """
        SELECT p.timestamp, t.symbol, p.open, p.high, p.low, p.close, p.volume
        FROM prices p
        JOIN tickers t ON t.ticker_id = p.ticker_id
        WHERE t.symbol = ?
          AND p.timestamp BETWEEN ? AND ?
        ORDER BY p.timestamp;
        """

        with self._connect() as conn:
            return conn.execute(query, (symbol, start, end)).fetchall()

    def avg_daily_volume(self):
        query = """
        SELECT t.symbol, AVG(p.volume) AS avg_volume
        FROM prices p
        JOIN tickers t ON t.ticker_id = p.ticker_id
        GROUP BY t.symbol
        ORDER BY avg_volume DESC;
        """

        with self._connect() as conn:
            return conn.execute(query).fetchall()

    def top_3_weekly_returns(self, start: str, end: str):
        query = """
        WITH first_last AS (
            SELECT
                t.symbol,
                MIN(p.timestamp) AS first_ts,
                MAX(p.timestamp) AS last_ts
            FROM prices p
            JOIN tickers t ON t.ticker_id = p.ticker_id
            WHERE p.timestamp BETWEEN ? AND ?
            GROUP BY t.symbol
        ),
        returns AS (
            SELECT
                f.symbol,
                (last_price.close - first_price.close) / first_price.close AS return_pct
            FROM first_last f
            JOIN prices first_price
                ON first_price.timestamp = f.first_ts
                AND first_price.ticker_id = (
                    SELECT ticker_id FROM tickers WHERE symbol = f.symbol
                )
            JOIN prices last_price
                ON last_price.timestamp = f.last_ts
                AND last_price.ticker_id = (
                    SELECT ticker_id FROM tickers WHERE symbol = f.symbol
                )
        )
        SELECT symbol, return_pct
        FROM returns
        ORDER BY return_pct DESC
        LIMIT 3;
        """

        with self._connect() as conn:
            return conn.execute(query, (start, end)).fetchall()

    def daily_open_close(self):
        query = """
        WITH ordered AS (
            SELECT
                t.symbol,
                DATE(p.timestamp) AS day,
                p.timestamp,
                p.close,
                ROW_NUMBER() OVER (
                    PARTITION BY t.symbol, DATE(p.timestamp)
                    ORDER BY p.timestamp
                ) AS rn_first,
                ROW_NUMBER() OVER (
                    PARTITION BY t.symbol, DATE(p.timestamp)
                    ORDER BY p.timestamp DESC
                ) AS rn_last
            FROM prices p
            JOIN tickers t ON t.ticker_id = p.ticker_id
        )
        SELECT
            symbol,
            day,
            MAX(CASE WHEN rn_first = 1 THEN close END) AS open_price,
            MAX(CASE WHEN rn_last = 1 THEN close END) AS close_price
        FROM ordered
        GROUP BY symbol, day
        ORDER BY symbol, day;
        """

        with self._connect() as conn:
            return conn.execute(query).fetchall()


def create_db(db_path, schema_path="schema.sql"):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    with open(schema_path, "r") as f:
        cur.executescript(f.read())

    conn.commit()
    conn.close()

def insert_data(df, db_path):
    conn = sqlite3.connect(db_path)
    
    tickers = df['ticker'].unique()
    conn.executemany("INSERT OR IGNORE INTO tickers(symbol) VALUES (?)",
                     [(t,) for t in tickers])
    
    cur = conn.cursor()
    cur.execute("SELECT ticker_id, symbol FROM tickers")
    mapping = {symbol: tid for tid, symbol in cur.fetchall()}
    
    rows = [
        (row.timestamp.isoformat(), mapping[row.ticker], row.open, row.high,
         row.low, row.close, row.volume)
        for row in df.itertuples()
    ]

    conn.executemany(
        "INSERT INTO prices(timestamp, ticker_id, open, high, low, close, volume)"
        " VALUES (?, ?, ?, ?, ?, ?, ?)",
        rows
    )

    conn.commit()
    conn.close()

