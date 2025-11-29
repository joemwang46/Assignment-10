# Query Tasks

## SQLite3

1. Retrieve all data for TSLA between 2025-11-17 and 2025-11-18.
    SQL get_ticker_data took 0.010573 seconds
    (output is too large but can run in code by uncommenting)
2. Calculate average daily volume per ticker.
    AVG Daily Volume:
    SQL avg_daily_volume took 0.011143 seconds

    ('TSLA', 2777.4245524296675)
    ('AAPL', 2767.83273657289)
    ('AMZN', 2753.424040920716)
    ('GOOG', 2740.160613810742)
    ('MSFT', 2686.550895140665)
3. Identify top 3 tickers by return over the full period.
    Top 3 weekly returns:
    SQL top_3_weekly_returns took 0.052194 seconds

    ('MSFT', 0.5497308173364513)
    ('AAPL', 0.17923065564087423)
    ('GOOG', 0.1649573262569031)
4. Find first and last trade price for each ticker per day.
    Daily open/close per ticker:
    SQL daily_open_close took 0.055144 seconds

    ('AAPL', '2025-11-17', 270.88, 287.68)
    ('AAPL', '2025-11-18', 287.48, 289.52)
    ('AAPL', '2025-11-19', 288.8, 295.87)
    ('AAPL', '2025-11-20', 296.99, 319.43)
    ('AAPL', '2025-11-21', 319.63, 334.57)
    ('AMZN', '2025-11-17', 125.46, 141.03)
    ('AMZN', '2025-11-18', 140.06, 133.47)
    ('AMZN', '2025-11-19', 131.98, 94.76)
    ('AMZN', '2025-11-20', 95.48, 94.05)
    ('AMZN', '2025-11-21', 93.05, 77.16)
    ('GOOG', '2025-11-17', 139.43, 105.0)
    ('GOOG', '2025-11-18', 104.2, 113.78)
    ('GOOG', '2025-11-19', 114.55, 139.3)
    ('GOOG', '2025-11-20', 139.73, 162.43)
    ('GOOG', '2025-11-21', 163.0, 153.9)
    ('MSFT', '2025-11-17', 183.89, 215.36)
    ('MSFT', '2025-11-18', 214.9, 242.24)
    ('MSFT', '2025-11-19', 241.16, 253.08)
    ('MSFT', '2025-11-20', 253.28, 284.98)
    ('MSFT', '2025-11-21', 286.81, 245.7)
    ('TSLA', '2025-11-17', 268.07, 286.86)
    ('TSLA', '2025-11-18', 286.16, 266.68)
    ('TSLA', '2025-11-19', 266.19, 272.15)
    ('TSLA', '2025-11-20', 271.09, 265.61)
    ('TSLA', '2025-11-21', 266.75, 292.32)

## Parquet

1. Load all data for AAPL and compute 5-minute rolling average of close price.
    Loaded 9775 rows from Parquet storage.
    PARQUET get_ticker_data took 0.013383 seconds

                timestamp    open    high     low   close  volume
    0    2025-11-17 09:30:00  271.45  272.07  270.77  270.88    1416
    1    2025-11-17 09:31:00  269.12  269.38  269.00  269.24    3812
    2    2025-11-17 09:32:00  270.36  271.24  270.22  270.86    3046
    3    2025-11-17 09:33:00  269.47  269.61  268.77  269.28    2090
    4    2025-11-17 09:34:00  269.17  269.79  269.02  269.32    2035
    ...                  ...     ...     ...     ...     ...     ...
    1559 2025-11-20 15:56:00  320.07  320.28  319.94  319.95    3558
    1560 2025-11-20 15:57:00  320.85  321.65  320.69  320.96    3569
    1561 2025-11-20 15:58:00  319.49  319.64  318.96  319.14    2840
    1562 2025-11-20 15:59:00  319.91  320.59  319.75  320.21    2384
    1563 2025-11-20 16:00:00  319.47  320.12  319.29  319.43    1429
2. Compute 5-day rolling volatility (std dev) of returns for each ticker.
    PARQUET rolling_volatility took 0.033893 seconds

        ticker           timestamp  rolling_vol
    0      AAPL 2025-11-17 09:30:00          NaN
    1      AAPL 2025-11-17 09:31:00          NaN
    2      AAPL 2025-11-17 09:32:00          NaN
    3      AAPL 2025-11-17 09:33:00          NaN
    4      AAPL 2025-11-17 09:34:00          NaN
    ...     ...                 ...          ...
    9770   TSLA 2025-11-21 15:56:00     0.005851
    9771   TSLA 2025-11-21 15:57:00     0.004582
    9772   TSLA 2025-11-21 15:58:00     0.004440
    9773   TSLA 2025-11-21 15:59:00     0.002759
    9774   TSLA 2025-11-21 16:00:00     0.004198
3. Compare query time and file size with SQLite3 for Task 1.
    SQL is slightly faster by about 0.0025 sec. market_data.db is 688 KB and Parquet files are 60+62+63+64+65KB = 314KB. Parquet is a bit slower but much lower file size