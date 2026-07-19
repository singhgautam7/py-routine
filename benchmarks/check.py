"""Regression tripwires over a benchmarks/run.py --json report.

Usage: python benchmarks/check.py bench.json

These are deliberately loose bounds, not tight performance targets:
shared CI runners are noisy and have few cores, so each rule leaves
generous slack and exists to catch step change regressions (a lost
fast path, an accidental O(n) in a hot loop), not five percent drift.
Adjust the slack before tightening a rule, never silently delete one.
"""

import json
import sys


def _ratio_rule(scenario, subject, reference, limit):
    """subject must finish within limit x reference's time."""

    def rule(results):
        table = results.get(scenario)
        if table is None:
            return None  # scenario not in this report, skip
        subject_t = table[subject]
        reference_t = table[reference]
        ok = subject_t <= reference_t * limit
        msg = (
            f"{scenario}: {subject} {subject_t:.3f}s vs "
            f"{reference} {reference_t:.3f}s (limit {limit}x)"
        )
        return ok, msg

    return rule


def rules_for(free_threading):
    rules = [
        # channels must stay clearly ahead of queue.Queue streaming
        _ratio_rule("throughput", "pyroutine", "threading", 1.2),
        # rendezvous must stay in threading's neighborhood
        _ratio_rule("pingpong", "pyroutine", "threading", 1.5),
        # pooled spawning must stay near raw thread creation or better
        _ratio_rule("spawn", "pyroutine", "threading", 1.2),
        # prepared select must stay within sight of the forwarder idiom
        _ratio_rule("select8", "pyroutine-sel", "threading", 4.0),
    ]
    if free_threading:
        # parallel CPU must actually parallelize on a free threaded build
        rules += [
            _ratio_rule("cpu", "pyroutine", "sequential", 0.7),
            _ratio_rule("words", "pyroutine", "sequential", 0.7),
        ]
    else:
        # on a GIL build, threads must at least not be much worse than
        # sequential (they cannot be much better for pure CPU)
        rules += [
            _ratio_rule("cpu", "pyroutine", "sequential", 1.4),
            _ratio_rule("words", "pyroutine", "sequential", 1.4),
        ]
    return rules


def main():
    if len(sys.argv) != 2:
        sys.exit("usage: python benchmarks/check.py <report.json>")
    with open(sys.argv[1]) as f:
        report = json.load(f)
    results = report["results"]
    build = "free threaded" if report["free_threading"] else "GIL"
    print(f"checking {sys.argv[1]}: python {report['python']} ({build}), "
          f"{report['cpus']} cpus, scale {report['scale']}")

    failed = 0
    for rule in rules_for(report["free_threading"]):
        outcome = rule(results)
        if outcome is None:
            continue
        ok, msg = outcome
        print(("  PASS  " if ok else "  FAIL  ") + msg)
        if not ok:
            failed += 1
    if failed:
        sys.exit(f"{failed} benchmark tripwire(s) fired")
    print("all tripwires clear")


if __name__ == "__main__":
    main()
