#!/usr/bin/env python3
"""Run the four-node E2E harness and validate terminal output when available."""

import argparse
import hashlib
import json
import shutil
import struct
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

import polars as pl
import pyarrow.parquet as pq


ROOT = Path(__file__).resolve().parents[2]
HERE = Path(__file__).resolve().parent
COMPOSE = HERE / "docker-compose.yml"


def run(command: list[str], cwd: Path = ROOT) -> None:
    subprocess.run(command, cwd=cwd, check=True)


def expected_sum(input_path: Path) -> float:
    result = (
        pl.scan_parquet(input_path)
        .filter(pl.col("score") > 30.0)
        .select(pl.col("score").cast(pl.Float64).sum().alias("score_sum"))
        .collect(streaming=True)
    )
    return float(result["score_sum"][0])


def result_checksum(value: float) -> str:
    return hashlib.sha256(struct.pack(">d", value)).hexdigest()


def wait_for_metrics(seed: str, timeout_seconds: int) -> None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(f"http://{seed}/metrics", timeout=2):
                return
        except Exception:
            time.sleep(1)
    raise RuntimeError(f"seed {seed} did not become ready")


def terminal_sum(paths: list[Path]) -> float:
    values = []
    for path in paths:
        table = pq.read_table(path, columns=["score_sum"])
        values.extend(table.column("score_sum").to_pylist())
    if len(values) != 1:
        raise RuntimeError(f"expected one aggregate row, found {len(values)}")
    return float(values[0])


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--size-gib", type=float, default=10.0)
    parser.add_argument("--seed", default="127.0.0.1:8000")
    parser.add_argument("--result-timeout", type=int, default=180)
    args = parser.parse_args()

    data = HERE / "data"
    input_path = data / "input.parquet"
    output_dir = data / "output"
    run(["docker", "compose", "-f", str(COMPOSE), "down", "--remove-orphans"])
    if output_dir.exists():
        shutil.rmtree(output_dir)
    if not input_path.exists():
        run([sys.executable, str(HERE / "generate_dataset.py"), "--output", str(input_path), "--size-gib", str(args.size_gib)])

    target = "wasm32-unknown-unknown"
    run(["rustup", "target", "add", target])
    run([
        "cargo", "build", "--manifest-path", str(HERE / "wasm_filters" / "Cargo.toml"),
        "--release", "--target", target, "--bins",
    ])
    run(["docker", "compose", "-f", str(COMPOSE), "up", "--build", "--detach"])
    started = time.perf_counter()
    wait_for_metrics(args.seed, 90)
    run(["cargo", "run", "--release", "-p", "kineplex-cli", "--", "submit", str(HERE / "job.yaml"), "--seed", args.seed])

    deadline = time.monotonic() + args.result_timeout
    while time.monotonic() < deadline:
        parts = sorted(output_dir.glob("part-*.parquet")) if output_dir.exists() else []
        if parts:
            expected = expected_sum(input_path)
            actual = terminal_sum(parts)
            expected_hash = result_checksum(expected)
            actual_hash = result_checksum(actual)
            if expected_hash != actual_hash:
                raise RuntimeError(f"checksum mismatch: expected {expected_hash}, got {actual_hash}")
            report = {
                "nodes": 4,
                "parquet_file_bytes": input_path.stat().st_size,
                "input_rows": pq.ParquetFile(input_path).metadata.num_rows,
                "elapsed_seconds": time.perf_counter() - started,
                "expected_sha256": expected_hash,
                "actual_sha256": actual_hash,
                "network_serialization_cpu_fraction": None,
            }
            (HERE / "report.json").write_text(json.dumps(report, indent=2) + "\n")
            print(json.dumps(report, indent=2))
            return 0
        time.sleep(1)

    print(
        "The seed accepted the graph, but no Parquet result appeared. "
        "Current node allocation does not execute the Receptor/Wasm/Terminal data plane yet.",
        file=sys.stderr,
    )
    return 3


if __name__ == "__main__":
    raise SystemExit(main())
