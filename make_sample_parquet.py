#!/usr/bin/env python3
"""Write a small Parquet file covering the common column types. Usage: make_sample_parquet.py OUT [ROWS]"""

import datetime as dt
import decimal
import os
import sys

import pyarrow as pa
import pyarrow.parquet as pq


def write_sample(out: str, rows: int) -> None:
    base = dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc)
    table = pa.table(
        {
            "id": pa.array(range(rows), pa.int64()),
            "name": pa.array([f"row-{i}" for i in range(rows)], pa.string()),
            "amount": pa.array(
                [decimal.Decimal(i) / 100 for i in range(rows)], pa.decimal128(18, 2)
            ),
            "ratio": pa.array([i / rows for i in range(rows)], pa.float64()),
            "is_even": pa.array([i % 2 == 0 for i in range(rows)], pa.bool_()),
            "event_date": pa.array(
                [(base + dt.timedelta(days=i % 365)).date() for i in range(rows)], pa.date32()
            ),
            "event_ts": pa.array(
                [base + dt.timedelta(minutes=i) for i in range(rows)], pa.timestamp("us")
            ),
        }
    )
    os.makedirs(os.path.dirname(os.path.abspath(out)), exist_ok=True)
    pq.write_table(table, out, compression="snappy")
    print(f"wrote {rows} rows to {out}")


def main() -> None:
    write_sample(sys.argv[1], int(sys.argv[2]) if len(sys.argv) > 2 else 1000)


if __name__ == "__main__":
    main()
