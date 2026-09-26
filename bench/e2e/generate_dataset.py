#!/usr/bin/env python3
"""Create a deterministic 10 GiB logical Parquet input without holding it in RAM."""

import argparse
import math
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=Path("data/input.parquet"))
    parser.add_argument("--size-gib", type=float, default=10.0)
    parser.add_argument("--row-group-rows", type=int, default=1_000_000)
    args = parser.parse_args()
    if args.size_gib <= 0 or args.row_group_rows <= 0:
        parser.error("size and row group size must be positive")

    logical_bytes = int(args.size_gib * 1024**3)
    rows = math.ceil(logical_bytes / 12)  # Int64 id + Float32 score.
    args.output.parent.mkdir(parents=True, exist_ok=True)
    schema = pa.schema([("id", pa.int64()), ("score", pa.float32())])
    writer = pq.ParquetWriter(args.output, schema, compression="zstd")
    random = np.random.default_rng(20260926)
    try:
        for start in range(0, rows, args.row_group_rows):
            count = min(args.row_group_rows, rows - start)
            ids = pa.array(np.arange(start, start + count, dtype=np.int64))
            scores = pa.array(random.integers(0, 10_000, size=count, dtype=np.int32).astype(np.float32))
            writer.write_table(pa.Table.from_arrays([ids, scores], schema=schema), row_group_size=count)
    finally:
        writer.close()
    print(f"wrote {rows:,} rows ({rows * 12 / 1024**3:.2f} GiB logical) to {args.output}")


if __name__ == "__main__":
    main()
