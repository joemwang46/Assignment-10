import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path

class ParquetStorage:
    def __init__(self, parquet_dir: str):
        self.parquet_dir = Path(parquet_dir)
        self.parquet_dir.mkdir(exist_ok=True, parents=True)

    def write_partitioned(self, df: pd.DataFrame):
        table = pa.Table.from_pandas(df)
        pq.write_to_dataset(
            table,
            root_path=self.parquet_dir,
            partition_cols=["ticker"]
        )

    def load_all(self) -> pd.DataFrame:
        dataset = pq.ParquetDataset(self.parquet_dir)
        table = dataset.read()
        return table.to_pandas()

    def get_ticker_data(self, symbol: str, start: str, end: str):
        ticker_path = self.parquet_dir / f"ticker={symbol}"

        if not ticker_path.exists():
            raise ValueError(f"No Parquet data found for ticker '{symbol}'")

        df = pq.ParquetDataset(ticker_path).read().to_pandas()

        df["timestamp"] = pd.to_datetime(df["timestamp"])
        mask = (df["timestamp"] >= start) & (df["timestamp"] <= end)

        return df.loc[mask].sort_values("timestamp")

    def rolling_volatility(self, window: int = 5) -> pd.DataFrame:
        dataset = pq.ParquetDataset(self.parquet_dir)
        df = dataset.read().to_pandas()

        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df = df.sort_values(["ticker", "timestamp"])

        df["returns"] = df.groupby("ticker", observed=False)["close"].pct_change()
        df["rolling_vol"] = (
            df.groupby("ticker", observed=False)["returns"]
              .rolling(window)
              .std()
              .reset_index(level=0, drop=True)
        )

        return df[["ticker", "timestamp", "rolling_vol"]]
