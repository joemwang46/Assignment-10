import sqlite3
import pandas as pd
from sqlite_storage import create_db, insert_data, SQLiteStorage

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

def test_sqlite_schema_and_insert(tmp_path):
    db = tmp_path / "test.db"
    create_db(db)

    df = sample_df()
    insert_data(df, db)

    # Verify tables
    conn = sqlite3.connect(db)
    cur = conn.cursor()

    cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = {t[0] for t in cur.fetchall()}
    assert "tickers" in tables
    assert "prices" in tables

    # Verify data inserted
    cur.execute("SELECT COUNT(*) FROM prices")
    count = cur.fetchone()[0]
    assert count == 3

    conn.close()

def test_get_ticker_data(tmp_path):
    db = tmp_path / "test.db"
    create_db(db)
    insert_data(sample_df(), db)

    sql = SQLiteStorage(db)
    rows = sql.get_ticker_data("AAPL", "2025-01-01", "2025-01-02")

    assert len(rows) == 2
    assert all(r[1] == "AAPL" for r in rows)

def test_avg_daily_volume(tmp_path):
    db = tmp_path / "test.db"
    create_db(db)
    insert_data(sample_df(), db)

    sql = SQLiteStorage(db)
    result = sql.avg_daily_volume()

    assert result[0][0] == "AAPL"
    assert result[0][1] == (500 + 600 + 700) / 3

def test_daily_open_close(tmp_path):
    db = tmp_path / "test.db"
    create_db(db)
    insert_data(sample_df(), db)

    sql = SQLiteStorage(db)
    results = sql.daily_open_close()

    # Should return 2 days: Jan 1 and Jan 2
    days = {row[1] for row in results}
    assert days == {"2025-01-01", "2025-01-02"}
