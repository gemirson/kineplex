# KinePlex E2E benchmark harness

The harness defines a four-node Compose cluster, a deterministic 10 GiB logical
Parquet input, three SDK-compiled Wasm filters, an aggregate stage, and a terminal
checksum comparison against Polars.

Install `requirements.txt`, Docker Compose, Rust, and the `wasm32-unknown-unknown`
target, then run `python3 run_e2e.py`. The runner generates input in row groups,
starts the cluster, submits `job.yaml`, and checks the terminal checksum.

At present, the Control Plane can validate and allocate this graph but the node
does not yet connect the Receptor, Wasm invocation, native aggregation, and
Terminal into an executing data plane. The runner therefore reports a missing
result instead of claiming a successful distributed benchmark. Its report leaves
network/serialization CPU share unset until that path exposes measurements.
