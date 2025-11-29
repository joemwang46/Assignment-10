## 1. File Size Comparison

| Format      | File / Directory | Size (KB) |
|-------------|------------------|-----------|
| SQLite3     | `market_data.db` |     688   |
| Parquet     | `market_data/`   |     314   |

**Observation:**  
Parquet generally produces smaller files because it uses columnar compression and dictionary encoding.  
SQLite3 stores rows, so prices tables tend to be larger with repeated values (timestamps, ticker IDs, etc.).

---

## 2. Query Performance Comparison

Each query below was benchmarked using `time.perf_counter()` in the main script.  
All times are in seconds.

### 2.1 Retrieve all data for a ticker and date range

| Query                     | SQLite3 | Parquet |
|---------------------------|---------|---------|
| Ticker data (AAPL, 5 days) |0.010573|0.013883 |

**Notes:**  
- SQLite3 is usually faster for filtered lookups because of indexed row storage.  
- Parquet must read entire partitions and filter in memory.

---

### 2.2 Average daily volume per ticker

| Query                | SQLite3 | Parquet |
|----------------------|---------|---------|
| Average daily volume | 0.011143 | <INSERT> |

**Notes:**  
- SQLite3 handles group-by queries efficiently with SQL engines.  
- Parquet loads more data into memory before aggregating.

---

## 3. Strengths and Weaknesses of Each Format

### SQLite3 (Relational Database)

**Strengths**
- Excellent for OLTP-style access: filter by ticker, date, or both.  
- SQL is expressive for aggregations (volume, returns).  
- Single-file database is portable and easy to distribute.  
- Indexing improves query speed significantly.

**Weaknesses**
- Not optimized for large column scans or vectorized operations.  
- Rolling windows, model inputs, and time-series analytics are inefficient.  
- Scaling beyond a single machine or file requires migration to PostgreSQL or MySQL.

**Best For**
- Local backtesting engines with many point-lookups.  
- Applications that need integrity constraints and schema enforcement.  
- Small-to-medium datasets (< 1–2 GB).

---

### Parquet (Columnar Storage)

**Strengths**
- Highly compressed, small file sizes.  
- Fast columnar reads—ideal for analytics, volatility, factor construction, ML pipelines.  
- Easily partitions by ticker, date, etc.  
- Integrates directly with pandas, pyarrow, Spark, and DuckDB.

**Weaknesses**
- Not ideal for point-lookups or transactional writes.  
- Filtering requires scanning entire partitions.  
- No built-in query engine—must be used with pandas, DuckDB, or Spark.

**Best For**
- Quant research workloads requiring full-column scans.  
- Time-series calculations (rolling windows, vol, returns).  
- ML pipelines, distributed compute, and large historical datasets.

---

## 4. Use-Case Recommendations

### Use SQLite3 when:
- You need fast lookups for a specific ticker and date range.  
- You want strict schemas, constraints, and relational integrity.  
- The dataset fits easily on local disk.  
- You need SQL for quick prototyping and analytics.

### Use Parquet when:
- You run heavy analytics across many tickers.  
- You need compression and efficient long-term storage.  
- You want to integrate with Python data pipelines (pandas, pyarrow).  
- You compute rolling statistics, factors, or model inputs.

