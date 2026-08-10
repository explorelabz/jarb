from __future__ import annotations

import statistics
import time

import hedge_core

ITERATIONS = 200_000


def benchmark(name, operation):
    samples = []
    started = time.perf_counter_ns()
    for index in range(ITERATIONS):
        point = time.perf_counter_ns()
        operation()
        if index % 100 == 0:
            samples.append((time.perf_counter_ns() - point) / 1_000)
    elapsed = (time.perf_counter_ns() - started) / 1_000_000_000
    ordered = sorted(samples)
    p99 = ordered[int(len(ordered) * .99)]
    print(f"{name}: {ITERATIONS / elapsed:,.0f} ops/s | sampled p50 {statistics.median(samples):.3f} µs | p99 {p99:.3f} µs")


benchmark("quote calculation", lambda: hedge_core.make_quotes(17_482_140, 17_493_860, .4382, .3167, 10.0, .05, 1.0))
benchmark("delta reconciliation", lambda: hedge_core.reconcile([("SELL", .01)], [("BUY", .01)]))
benchmark("trade pnl", lambda: hedge_core.trade_pnl("SELL", 15_025_000, 15_010_000, 1.0, 15_025.0, 3_002.0))
