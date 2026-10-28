"""Research certification with frozen slippage and published fees; no optimization."""
from __future__ import annotations

import argparse
import contextlib
from dataclasses import asdict
from datetime import date, datetime, timezone
from hashlib import sha256
import io
import json
from pathlib import Path
import subprocess
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from optimization.tsla_production_selection import (
    calibrated_friction, canonical_json, published_fee_fallback,
)

TEST_MODULES = (
    "test_tsla_production_selection", "test_replay_engine", "test_replay_parity",
    "test_optimization", "test_canonical_accounting", "test_canonical_market_sampler",
    "test_renko_golden_vectors", "test_renko_anchor", "test_strategy_engine",
    "test_research_validation", "test_systemwide_renko_contract", "test_broker_protection",
    "test_exit_plan", "test_execution_engine", "test_alpaca_trader",
)
FILES = (
    "optimization/models.py", "optimization/runner.py", "optimization/tsla_production_selection.py",
    "replay/execution.py", "replay/engine.py", "replay/models.py", "replay/config_hash.py",
    "scripts/run_tsla_production_selection.py", "tests/test_optimization.py",
    "tests/test_replay_engine.py", "tests/test_tsla_production_selection.py",
)
CHANGED_FILES = ("replay/execution.py", "tests/test_tsla_production_selection.py",
                 "scripts/run_tsla_production_selection.py")


def load_frozen_calibration(path, holdout_sessions=()):
    path = Path(path)
    raw = path.read_bytes()
    # Pin the already accepted artifact, not a new estimate from market/holdout data.
    if sha256(raw).hexdigest() != "93d5622d4a6434003557a86d79e583e1f513c846c0dd32930596bfe9d537bb9a":
        raise ValueError("Frozen calibration artifact checksum mismatch")
    audit = json.loads(raw)
    calibration = audit["calibration"]
    if holdout_sessions and max(map(date.fromisoformat, calibration["sessions"])) >= min(map(date.fromisoformat, holdout_sessions)):
        raise ValueError("Frozen calibration must precede untouched holdout")
    friction = published_fee_fallback(calibrated_friction(calibration))
    return audit, friction


class EvidenceResult(unittest.TextTestResult):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.passed = []

    def addSuccess(self, test):
        super().addSuccess(test)
        self.passed.append(test.id())


def certification_gates(local_pass, calibration, friction):
    status = "PASS" if local_pass else "FAIL"
    empirical = bool(calibration and friction and friction.slippage_per_share > 0)
    approved_fees = False
    if empirical:
        try:
            approved_fees = friction == published_fee_fallback(calibrated_friction(calibration))
        except (ValueError, KeyError):
            pass
    gates = [
        ("Canonical data parity", status, "Unchanged production sampler; independent sampling/filter/no-flush tests"),
        ("Coverage integrity", status, "Source/progression rejection tests; calibration archive checksum, pagination, order and count validation"),
        ("Determinism", status, "Frozen calibration verified without recalibration; repeated synthetic replay compares fills, trades, identity, P&L and metrics"),
        ("Renko brick semantics", status, "Production conversion; frozen fixed-dollar brick/reference"),
        ("Holdout isolation", status, "Frozen execution assumptions; calibration/holdout overlap rejected"),
        ("Conservative replay execution", status, "Raw exits; stop-first ambiguity; timed force-flat; ideal TP limits unchanged"),
        ("Empirical slippage calibration", "LIMITATION" if empirical and local_pass else "FAIL",
         f"{len(calibration['observations']) if calibration else 0} validated fills; LIMITED confidence; one clipped unweighted mean"),
        ("Stress friction", status if empirical else "FAIL", "Exactly 2x frozen BASE; identical fees; validation only"),
        ("Accounting", status, "Silver gross P&L minus separate per-fill friction and fees exactly once; partial-close allocation"),
        ("Regulatory fees", status if approved_fees else "FAIL", "Verified August 7-September 25 window; 25 experiment dates accepted; September 26 rejected; unchanged daily totals and order TAF cap"),
        ("Commission assumption", "LIMITATION", "Standard direct-retail commission $0; account-specific status UNVERIFIED; no contrary evidence in inspected snapshots"),
        ("Mark-to-market drawdown", status, "Open-position raw marks include incurred entry charges and fees"),
        ("Candidate universe", status, "510 unique specifications generated in tests only; no ranking or optimization"),
        ("Plateau logic", status, "Synthetic known-center/spike/neighborhood tests"),
        ("Distinct regime logic", status, "Deterministic contiguous groups; one representative per regime"),
        ("Production safety", status, "Research-only writes; no broker or remote mutation; ownership-cleanup tests"),
        ("Remote CURRENT truth", "UNVERIFIED", "No SSH/account access; blocks only authoritative incumbent-vs-challenger production decision"),
    ]
    blockers = []
    if not local_pass:
        blockers.append("Relevant regression suite did not pass completely.")
    if not empirical:
        blockers.append("No positive validated empirical fixed-friction calibration.")
    if not approved_fees:
        blockers.append("Approved published-schedule regulatory fees and frozen execution assumptions are not configured.")
    return gates, blockers


def main(argv=None):
    if "--accelerated-readiness" in (argv if argv is not None else sys.argv[1:]):
        return accelerated_readiness(argv)
    if "--scale-readiness" in (sys.argv[1:] if argv is None else argv):
        return scale_readiness(argv)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--certify", action="store_true", required=True)
    parser.add_argument("--output", required=True, help="New directory beneath artifacts/research")
    parser.add_argument("--frozen-calibration", default=str(ROOT / "artifacts/research/tsla_empirical_friction_20260925_final/calibration.json"))
    parser.add_argument("--holdout-session", action="append", default=[])
    parser.add_argument("--remote-detail", default="UNVERIFIED; last read-only SSH attempt failed authentication. No current remote verification in this run.")
    args = parser.parse_args(argv)
    output = Path(args.output).resolve()
    research = (ROOT / "artifacts/research").resolve()
    if not output.is_relative_to(research) or output == research or output.exists():
        parser.error("Output must be a new directory beneath artifacts/research")
    output.mkdir(parents=True, exist_ok=False)
    calibration = friction = audit = None
    calibration_error = None
    try:
        audit, friction = load_frozen_calibration(args.frozen_calibration, args.holdout_session)
        calibration = audit["calibration"]
        audit["frozen_source"] = {"path": str(Path(args.frozen_calibration).resolve()),
                                  "sha256": sha256(Path(args.frozen_calibration).read_bytes()).hexdigest(),
                                  "recalibrated": False}
        audit["frozen_base_configuration"] = asdict(friction)
        audit["frozen_stress_configuration"] = asdict(friction.stress())
        (output / "calibration.json").write_text(json.dumps(audit, indent=2, default=str) + "\n")
    except (ValueError, KeyError, OSError) as exc:
        calibration_error = str(exc)
        friction = None

    loader = unittest.TestLoader()
    suite = unittest.TestSuite(loader.discover(str(ROOT / "tests"), pattern=f"{name}.py") for name in TEST_MODULES)
    log = io.StringIO()
    with contextlib.redirect_stdout(log), contextlib.redirect_stderr(log):
        result = unittest.TextTestRunner(stream=log, verbosity=2, resultclass=EvidenceResult).run(suite)
    (output / "tests.log").write_text(log.getvalue())
    tests = {"tests_run": result.testsRun, "passed": result.passed,
             "failures": [(test.id(), detail) for test, detail in result.failures],
             "errors": [(test.id(), detail) for test, detail in result.errors],
             "skipped": [(test.id(), detail) for test, detail in result.skipped]}
    (output / "test_results.json").write_text(json.dumps(tests, indent=2) + "\n")
    local_pass = result.wasSuccessful() and not result.skipped
    gates, blockers = certification_gates(local_pass, calibration, friction)
    diff_check = subprocess.run(["git", "diff", "--check"], cwd=ROOT, text=True, capture_output=True)
    if diff_check.returncode:
        blockers.append("git diff --check failed: " + diff_check.stdout + diff_check.stderr)
    if calibration_error:
        blockers.append(calibration_error)
    verdict = "BACKTESTER NOT CERTIFIED" if blockers else "BACKTESTER CERTIFIED FOR TSLA STRATEGY SELECTION"
    hashes = {name: sha256((ROOT / name).read_bytes()).hexdigest() for name in FILES}
    manifest = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(), "files": hashes, "python": sys.version,
        "git_head": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
        "test_modules": TEST_MODULES, "tests_run": result.testsRun, "gates": gates,
        "files_changed_this_task": CHANGED_FILES,
        "diff_check": {"exit_code": diff_check.returncode, "output": diff_check.stdout + diff_check.stderr},
        "fee_policy": asdict(friction.regulatory_fees) if friction else None,
        "verdict": verdict, "blockers": blockers, "calibration_error": calibration_error,
        "calibration_identity": calibration["identity"] if calibration else None,
        "calibration_artifact_sha256": sha256((output / "calibration.json").read_bytes()).hexdigest() if (output / "calibration.json").exists() else None,
        "exact_broker_fill_parity": "NOT REQUIRED FOR STRATEGY-SELECTION CERTIFICATION; UNVERIFIED",
        "remote_current_truth": "UNVERIFIED", "remote_detail": args.remote_detail,
        "optimization_run": False, "production_mutations": False,
        "holdout_sessions": args.holdout_session,
        "holdout_policy": "No holdout opened. Future partitions must exclude all calibration sessions and post-calibration data from calibration.",
    }
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2, default=str) + "\n")
    lines = ["# TSLA Fee Model And Backtester Certification", "", f"**{verdict}**", "",
             f"Tests: {result.testsRun}; failures {len(result.failures)}; errors {len(result.errors)}; skipped {len(result.skipped)}.",
             f"Passed: {len(result.passed)}. git diff --check exit code: {diff_check.returncode}.",
             "", "## Fee Window Extension",
             "The previously certified implementation was preserved. The only execution-code change extends valid_through from September 12 to September 25, 2026. "
             "Two focused tests and the old invalid-date fixtures were added/updated, along with this certification report. "
             "Formulas, caps, rounding, commissions, accounting, slippage, replay, universe, plateau and holdout logic are unchanged. "
             "The validity window participates in research fingerprints, so the extended scope intentionally has a different identity. "
             "No market data or actual holdout performance was opened; synthetic date acceptance is not dataset coverage certification.",
             "", "## Calibration"]
    if audit and friction:
        lines += [
            f"Found {audit['found']} TSLA fill activities; {audit['duplicates']} duplicates; {audit['unique']} unique; "
            f"{len(calibration['observations'])} valid; {len(audit['excluded'])} excluded.",
            f"Exclusions: {canonical_json(audit['exclusion_counts'])}.",
            f"BASE: {calibration['base']} USD/share/fill. STRESS: {calibration['stress']} USD/share/fill.",
            f"Confidence: {calibration['confidence']}. Sessions: {', '.join(calibration['sessions'])}.",
            "Unweighted mean of max(signed adverse execution difference, 0). Quantiles use linear interpolation.",
            "", "| Statistic | Signed | Adverse Only |", "|---|---|---|",
        ]
        lines += [f"| {key} | {calibration['signed'][key]} | {calibration['adverse'][key]} |"
                  for key in ("count", "mean", "median", "p75", "p90", "minimum", "maximum", "population_stddev")]
        lines += ["", "Broker provenance: direct live Alpaca HTTPS GET order and FILL activity export, joined by account/order ID. "
                  "Automated origin, side, quantity and price must reconcile. Partial/ambiguous fills are excluded.",
                  "Market entry reference: the first later eligible raw timestamp after the canonical sampler emits the logged signal bucket. "
                  "The triggering timestamp itself cannot fill. Equal-timestamp prices use the adverse endpoint. "
                  "Native TP reference: the pre-existing order limit. Native SL reference: the first eligible crossing after documented parent fill, including gaps. "
                  "No signal-brick price is substituted for a market fill; no reference is selected by fit to the observed fill.",
                  "The old 25-fill July $0.281626/share signed signal-price report is retained for comparison only, not reused as BASE.",
                  "Source paths/checksums, every included reference, every excluded activity and frozen BASE/STRESS configurations are in calibration.json.",
                  "The prior calibration artifact is checksum-pinned and reused, not recalculated. Its historical fee_status remains unchanged as provenance; the new fee policy is frozen separately."]
    else:
        lines += [f"Calibration failed: {calibration_error}"]
    lines += ["", "## Published Fee Fallback",
              "CERTIFIED FEE WINDOW: August 7, 2026 through September 25, 2026. TSLA NMS fills use America/New_York trade dates; other symbols/dates fail closed.",
              "REG: sell principal (ideal replay fill price times exact quantity) times 0.0000206. "
              "TAF: sells times 0.000195/share, capped at $9.79 per account/order across all partial fills, including across dates. "
              "CAT: buys and sells times 0.000003/share. Sell-short counts as a sell. Quantities are never rounded.",
              "Each exact contribution enters its own daily fee-type accumulator. Charge only ceil_cent(new cumulative total) minus ceil_cent(previous total). "
              "This conserves final daily fees without charging a cent per tiny fill. Identical fill IDs are charged once; conflicting duplicates fail. "
              "Intraday/trade allocation is deterministic and causal, not a reconstruction of broker per-trade charges. "
              "TSLA-only daily aggregation may differ slightly from actual account allocation across other symbols.",
              "Commission = $0 under the approved standard direct-retail assumption. ACCOUNT-SPECIFIC COMMISSION STATUS = UNVERIFIED. "
              "No Elite, non-retail, partner-commission or special-routing evidence was found in the previously inspected deployed_camatron_config, "
              "September 9 broker_truth and September 8 remote_snapshot artifacts. No new account or SSH access was attempted. "
              "Missing account fee activities are not evidence of zero actual regulatory fees.",
              "", "### Historical Evidence",
              "[SEC advisory, February 27, 2026](https://www.sec.gov/rules-regulations/fee-rate-advisories/2026-2): "
              "$20.60 per million effective April 4, 2026.",
              "[Alpaca published schedule](https://files.alpaca.markets/disclosures/library/BrokFeeSched.pdf): "
              "REG/TAF/CAT rates, commission exceptions and exact-quantity daily rounding. The currently served revision is September 17, 2026. "
              "The approved historical CAT rate was documented in the June 1 schedule; its indexed June excerpt corroborated the rate during the earlier investigation. "
              "The mutable URL now serves September, so no downloaded June-original PDF is claimed. "
              "Historical applicability for this closed research window follows the user's explicitly approved schedule, not a claim that a later PDF alone proves historical terms.",
              "[Indexed June 1 Alpaca schedule](https://files.alpaca.markets/disclosures/library/BrokFeeSched.pdf?deviceId=a91b8f99-a204-42c3-8cba-4e93eefccfdc) "
              "corroborates the NMS CAT rate of 0.000003/share and exact fractional quantities with daily upward-cent rounding. "
              "The 2026 TAF rate/cap is also shown in the [FINRA rule filing](https://www.sec.gov/files/rules/sro/finra/2026/34-106409-ex5.pdf). "
              "Its later October-December pause is outside this research window and is not applied retroactively.",
              "", "### September 13-25 Superseding-Schedule Check",
              "Authoritative sources checked on September 26, 2026: the SEC Section 31 advisory and "
              "[SEC advisory register](https://www.sec.gov/rules-regulations/fee-rate-advisories), "
              "the September 17 Alpaca schedule, "
              "[FINRA's TAF pause notice](https://www.sec.gov/files/rules/sro/finra/2026/34-106409.pdf), "
              "and [CAT fee alerts](https://www.catnmsplan.com/cat-fee-alerts).",
              "Finding: no applicable REG, TAF or CAT rate/cap change was found for September 13-25. "
              "The September 17 Alpaca revision is a newer schedule within that interval, but retains all three approved rates. "
              "The SEC register still lists the February 27 Section 31 advisory as the latest transaction-fee advisory; its later Section 6(b) filing-fee notice is not a transaction-fee change. "
              "FINRA's September notice sets the TAF pause for October 1-December 31 only, not September transactions. "
              "The CAT alert register lists the April 1 2026-1/2026-2 alerts (0.000001 plus 0.000002) without a later superseding alert. "
              "Together with Alpaca's September 17 NMS rate, these support the unchanged 0.000003 charge through September 25. "
              "No account-specific commission investigation was reopened; the approved zero direct-retail assumption and UNVERIFIED limitation remain frozen.",
              "", "## Certification Gates", "", "| Integrity Area | Status | Evidence |", "|---|---|---|"]
    lines += [f"| {area} | {status} | {evidence} |" for area, status, evidence in gates]
    lines += ["", "EXACT BROKER-FILL PARITY: NOT REQUIRED FOR STRATEGY-SELECTION CERTIFICATION.",
              "REMOTE CURRENT PRODUCTION TRUTH: UNVERIFIED. An authoritative incumbent-vs-challenger decision remains blocked.",
              "", "## Accounting And Compatibility",
              "Opt-in PER_FILL_CHARGE_V1 leaves ideal replay fills and TP limits unchanged. It deducts one fixed per-share charge on every entry and exit, "
              "including TP, SL and force-flat. This is a research economic charge, never an assertion of a broker limit violation. "
              "Pre-friction gross P&L minus execution friction minus regulatory fees minus applicable commissions equals net P&L, with no double deductions. "
              "Fees use ideal fill principal, independent of BASE/STRESS economic friction. Open-position equity pays incurred charges immediately. "
              "Partial exits allocate entry charges proportionally. Silver AccountingLedger remains the sole position/P&L implementation.",
              "Legacy PRICE_ADJUSTMENT defaults and callers remain unchanged. Its existing stress behavior is preserved; the new policy doubles only friction, not fees. "
              "New cost fields change research configuration fingerprints; old results must not be silently treated as the same identity.",
              "The certification CLI now accepts --frozen-calibration instead of raw broker/event/archive inputs; it cannot recalibrate the accepted slippage. "
              "Legacy replay callers remain opt-out by default. Supported fee rates/caps/commission metadata are hashed; unsupported aggregation policies are rejected.",
              "Reused production components remain unchanged: CanonicalOneSecondSipSampler, RenkoStrategyEngine, fixed-brick resolver, "
              "exit-plan builder, pure protection verifier, close resolver and Silver accounting.",
              "BASE/STRESS, provenance, calibration dates, fee assumption and application policy are immutable/hashable and frozen with finalists. "
              "Holdout cannot recalibrate them. STRESS universe generation is rejected; opt-in frozen-finalist stress evaluation does not rank or tune parameters.",
              "", "## Limitations",
              "The small, selected September sample does not establish a tail bound or broad market representativeness. "
              "Trade-only order references condition on observed live activation; they are not full historical strategy replay pairs. "
              "Microsecond timestamp normalization matches the existing canonical implementation. "
              "NBBO, partial fills, rejection/cancellation races and exact acknowledgement timing remain unmodeled operational differences.",
              "No current deployment verification, untouched holdout evaluation, full optimization-period coverage certification or 860-candidate optimization was performed.",
              "The fixed calibration includes September 4 and September 8. Any later requested five-session holdout must begin after September 8; "
              "an overlapping partition is rejected, not silently accepted or used to recalibrate. This certification validates infrastructure, not a specific future dataset or profitability.",
              "", "## Tests Added",
              "Two date-scope tests were added: acceptance of every August 21-September 18 training date and September 21-25 holdout date, "
              "plus September 12/13 and New York midnight boundaries; and deterministic changed-window identity with all other assumptions unchanged. "
              "September 26 and dates before August 7 remain rejected. The fee guard accepts calendar dates, not proof of an exchange session; "
              "the existing market-calendar gate separately excludes weekends. No actual holdout strategy evaluation was performed.",
              "Existing fee tests continue to cover exact buy/sell/short components and fractions; daily increment rounding and turnover; "
              "TAF caps across partial fills/dates; New York daily boundaries; duplicate/conflicting/invalid fills; "
              "long/short wins/losses and single-count costs; partial-close conservation; frozen fee serialization/holdout identity; "
              "approved/nonapproved certification gates; candidate-runner repeat/STRESS integration; and pinned calibration without recalculation.",
              "Two ReplayEngineTests cover long/short TP/SL/force-flat paths with identical BASE/STRESS fees and repeatable fills/metrics; "
              "and rate/cap/commission/source/window fingerprints with unsupported aggregation rejected. "
              "The unchanged default July replay fixture now optionally accepts a date for approved-window tests.",
              "Full suite modules: " + ", ".join(TEST_MODULES) + ". Exact test IDs and results are in test_results.json and tests.log.",
              "", "## Remaining Blockers"] + [f"- {item}" for item in blockers]
    if not blockers:
        lines += ["None under the approved research-only certification standard."]
    lines += ["", "## Files Changed This Task"] + [f"- {name}" for name in CHANGED_FILES]
    lines += ["", "The remaining eight fingerprinted files retain earlier approved changes; they were not edited in this date-extension task.",
              "Generated files: calibration.json, manifest.json, report.md, test_results.json and tests.log in this run directory.",
              "", "## Source Fingerprints"] + [f"- {name}: {digest}" for name, digest in hashes.items()]
    lines += ["", "Only this new research evidence directory and approved source/test files were written. "
              "No archives, live state/configuration, services, broker orders or positions were modified. No cleanup/deletion performed.", "", f"**{verdict}**", ""]
    (output / "report.md").write_text("\n".join(lines))
    print(json.dumps({"report": str(output / "report.md"), "tests_run": result.testsRun,
                      "local_tests_pass": local_pass, "verdict": verdict, "blockers": blockers}))
    return 0 if not blockers else 2 if local_pass else 1


SCALE_FILES = (
    "optimization/tsla_production_selection.py", "optimization/runner.py",
    "optimization/indexed_market.py", "optimization/research_checkpoint.py",
    "replay/metrics.py", "replay/execution.py", "scripts/run_tsla_production_selection.py",
    "tests/test_tsla_production_selection.py", "tests/test_indexed_market.py",
    "tests/test_optimization.py", "tests/test_replay_engine.py",
)


def scale_readiness(argv=None):
    """Full training index plus ONE first-session benchmark, never a final sweep."""
    from dataclasses import replace
    from decimal import Decimal
    import resource
    import time
    from itertools import islice
    from types import SimpleNamespace
    from optimization.indexed_market import build_index, IndexedRows, IndexedDataset
    from optimization.models import OptimizationConfig
    from optimization.runner import load_enabled_renko_traders, _execute_candidate
    from optimization.tsla_production_selection import (
        ResearchPartition, approved_universe_config, generate_challengers, _prepared, fingerprint,
    )

    parser = argparse.ArgumentParser(description=scale_readiness.__doc__)
    parser.add_argument("--scale-readiness", action="store_true", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--tests-only", action="store_true")
    parser.add_argument("--existing-training-index", help="Reverify a completed research index without rebuilding raw data")
    args = parser.parse_args(argv)
    output = Path(args.output).resolve()
    research = ROOT / "artifacts/research"
    if not output.is_relative_to(research) or output == research or output.exists():
        parser.error("Output must be a new run directory under artifacts/research")
    output.mkdir(parents=True)
    manifest = {"status": "TESTING", "holdout_performance_opened": False,
                "final_optimization_run": False, "production_mutations": False,
                "candidate_count": 510, "brick_range": ["0.16", "0.66"], "step": "0.01",
                "source_hashes": {name: sha256((ROOT / name).read_bytes()).hexdigest() for name in SCALE_FILES}}

    def persist():
        (output / "readiness.json").write_text(json.dumps(manifest, indent=2, default=str) + "\n")

    persist()
    loader = unittest.TestLoader()
    modules = (*TEST_MODULES, "test_indexed_market")
    suite = unittest.TestSuite(loader.discover(str(ROOT / "tests"), pattern=f"{name}.py") for name in modules)
    log = io.StringIO()
    print(json.dumps({"status": "TESTING", "modules": modules}), flush=True)
    with contextlib.redirect_stdout(log), contextlib.redirect_stderr(log):
        result = unittest.TextTestRunner(stream=log, verbosity=2, resultclass=EvidenceResult).run(suite)
    (output / "tests.log").write_text(log.getvalue())
    manifest["tests"] = {"run": result.testsRun, "passed": len(result.passed),
        "failures": [(test.id(), detail) for test, detail in result.failures],
        "errors": [(test.id(), detail) for test, detail in result.errors],
        "skipped": [(test.id(), detail) for test, detail in result.skipped]}
    diff = subprocess.run(["git", "diff", "--check"], cwd=ROOT, capture_output=True, text=True)
    manifest["diff_check"] = {"exit_code": diff.returncode, "output": diff.stdout + diff.stderr}
    persist()
    if not result.wasSuccessful() or result.skipped or diff.returncode:
        manifest.update(status="TEST_FAILURE", verdict="OPTIMIZATION ENGINE NOT READY")
        persist()
        print(json.dumps({"status": manifest["status"], "tests": manifest["tests"]}), flush=True)
        return 1
    print(json.dumps({"status": "TESTS_PASSED", "count": result.testsRun}), flush=True)
    if args.tests_only:
        manifest.update(status="TESTS_PASSED", verdict="OPTIMIZATION ENGINE NOT READY",
                        limitation="Tests-only run; full index and measured replay still required")
        persist()
        return 0
    try:
        readiness_path = ROOT / "artifacts/research/tsla_20plus5_20260926_backfill/data_readiness.json"
        ready = json.loads(readiness_path.read_text())
        training = [entry for entry in ready["sessions"] if entry["partition"] == "TRAIN"]
        held = [entry for entry in ready["sessions"] if entry["partition"] == "HOLDOUT"]
        if len(training) != 20 or len(held) != 5 or sum(entry["raw_count"] for entry in training) != 18451233:
            raise ValueError("The approved 20+5 source manifest changed")
        partition = ResearchPartition(tuple(map(date.fromisoformat, ready["training"])),
                                      tuple(map(date.fromisoformat, ready["holdout"])))
        manifest["source_manifest_sha256"] = sha256(readiness_path.read_bytes()).hexdigest()
        manifest["sealed_holdout_source_identity"] = fingerprint({"partition": "HOLDOUT", "sources": held})
        manifest["sealed_holdout_sources"] = held
        manifest["status"] = "BUILDING_TRAINING_INDEX"
        persist()
        start = time.monotonic()
        def progress(entry):
            print(json.dumps({"status": "INDEXED_SESSION", "date": entry["date"],
                "rows": entry["raw_count"], "elapsed_seconds": time.monotonic() - start,
                "process_peak_rss_bytes": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss}), flush=True)
        if args.existing_training_index:
            existing = Path(args.existing_training_index).resolve()
            if not existing.is_relative_to(research):
                raise ValueError("Only a run-local research index may be reused")
            data = IndexedDataset(existing)
            stored = json.loads(data.manifest_json)
            if (data.partition != "TRAIN" or data.sessions != partition.optimization or
                    [(e["date"], e["raw_path"], e["raw_sha256"], e["raw_count"]) for e in stored["sources"]] !=
                    [(e["date"], e["source"], e["raw_sha256"], e["raw_count"]) for e in training]):
                raise ValueError("Reused index differs from the approved training sources")
            manifest.update(reused_training_index=str(existing), index_open_verify_seconds=time.monotonic()-start)
        else:
            data = build_index(training, output / "training", partition="TRAIN", progress=progress)
        manifest.update(index_build_seconds=None if args.existing_training_index else time.monotonic() - start,
                        index_build_process_peak_rss_bytes=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
                        training_identity=data.identity, training_raw_records=len(data.raw_trades),
                        index_disk_bytes=sum(path.stat().st_size for path in data.directory.glob("*.sqlite")))
        audit, friction = load_frozen_calibration(ROOT / "artifacts/research/tsla_empirical_friction_20260925_final/calibration.json", ready["holdout"])
        template_path = ROOT / "artifacts/production_health/20260908/deployed_camatron_config.json"
        selected = [item for item in load_enabled_renko_traders(str(template_path), ("all-enabled-renko",),
                    included_strategy_types=("renko_tp_sl",)) if item.symbol == "TSLA"]
        if len(selected) != 1:
            raise ValueError("Historical research template is ambiguous")
        template = selected[0]
        template = replace(template, strategy_config={**template.strategy_config, "active_start_date": None,
                                                       "active_end_date": None})
        cfg = approved_universe_config(OptimizationConfig("tsla_scale_readiness", str(template_path), data.sessions,
                                                         execution_friction=friction))
        manifest["research_template"] = {"source": str(template_path), "hash": sha256(template_path.read_bytes()).hexdigest(),
            "config": template.strategy_config, "status": "HISTORICAL_TEMPLATE_NOT_CURRENT_TRUTH",
            "date_scope": "Historical deployment activation dates cleared in research memory only to admit the authorized training dates"}
        start = time.monotonic()
        candidates = generate_challengers(cfg, template, data, partition)
        manifest["candidate_freeze_seconds"] = time.monotonic() - start
        (output / "universe.json").write_text(json.dumps([asdict(c) for c in candidates], default=str, indent=2) + "\n")
        candidate = next(c for c in candidates if c.spec().brick_pct == Decimal(".41")
                         and c.spec().strategy_parameters.entry_confirmation == "E1"
                         and c.spec().strategy_parameters.take_profit_bricks == 2
                         and c.spec().strategy_parameters.stop_loss_bricks == 1)
        spec = candidate.spec()
        cache_start = time.monotonic()
        cached = data.renko_stream(cfg, spec)
        manifest["renko_cache_build_seconds"] = time.monotonic() - cache_start
        # A fixed first-session probe is selected before any performance inspection.
        # It is not a completed 20-session candidate and cannot enter ranking/resume.
        raw = IndexedRows(data.raw_trades.entries[:1], "raw")
        observations = IndexedRows(data.observations.entries[:1], "canonical")
        prepared = replace(_prepared(data), session_dates=data.sessions[:1], observation_count=len(observations))
        probe_cfg = replace(cfg, session_dates=data.sessions[:1])
        manifest.update(status="BENCHMARKING", benchmark={"candidate": candidate.identity,
            "brick": "0.41", "entry": "E1", "tp": 2, "sl": 1,
            "session": str(data.sessions[0]), "raw_records": len(raw), "scope": "ONE FIRST-SESSION PROBE; NOT A FULL TRAINING RESULT"})
        persist()
        print(json.dumps({"status": "BENCHMARKING", "session": str(data.sessions[0])}), flush=True)
        start = time.monotonic()
        row = _execute_candidate(config=probe_cfg, spec=spec, prepared=prepared,
            observations=observations, raw_trades=raw, use_cached_renko_stream=False, bounded_memory=True,
            renko_stream=SimpleNamespace(events=islice(cached.events, len(observations))))
        seconds = time.monotonic() - start
        manifest["benchmark"].update(seconds=seconds, raw_records_per_second=len(raw)/seconds,
            session_candidates_per_hour=3600/seconds,
            projected_full_510_training_hours=seconds*len(data.raw_trades)/len(raw)*510/3600,
            process_peak_rss_bytes=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
            projection_limitations="Serial raw-record scaling from one predetermined session/candidate with cached Renko; not a runtime guarantee. One-time index/cache preparation excluded.")
        manifest["cache_disk_bytes"] = sum(p.stat().st_size for p in data.directory.rglob("*") if p.is_file() and "renko" in p.parts)
        (output / "benchmark_result.json").write_text(json.dumps(row, indent=2, default=str) + "\n")
        data.verify()
        if manifest["source_hashes"] != {name: sha256((ROOT / name).read_bytes()).hexdigest() for name in SCALE_FILES}:
            raise ValueError("Code changed during readiness run")
        manifest.update(status="COMPLETE", verdict="OPTIMIZATION ENGINE READY")
    except Exception as exc:
        manifest.update(status="FAILED", verdict="OPTIMIZATION ENGINE NOT READY", error=f"{type(exc).__name__}: {exc}")
    persist()
    print(json.dumps({"status": manifest["status"], "verdict": manifest["verdict"],
                      "report": str(output / "readiness.json"), "error": manifest.get("error")}), flush=True)
    return 0 if manifest["status"] == "COMPLETE" else 1


def accelerated_readiness(argv=None):
    """Fixed training workload only. No rankings, final sweep, or holdout replay."""
    from dataclasses import replace
    from decimal import Decimal
    import time
    from optimization.indexed_market import IndexedDataset
    from optimization.accelerated_replay import ExecutionCache
    from optimization.brick_jobs import run_brick_jobs
    from optimization.models import OptimizationConfig
    from optimization.runner import load_enabled_renko_traders
    from optimization.tsla_production_selection import (
        ResearchPartition, approved_universe_config, generate_challengers,
        evaluate_training, fingerprint, research_code_fingerprint)

    parser = argparse.ArgumentParser(description=accelerated_readiness.__doc__)
    parser.add_argument('--accelerated-readiness', action='store_true', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument('--execution-cache', required=True)
    parser.add_argument('--workers', type=int, nargs='*', default=[4, 8, 12], choices=[4, 8, 12])
    parser.add_argument('--serial-only', action='store_true')
    args = parser.parse_args(argv)
    output = Path(args.output).resolve()
    cache_path = Path(args.execution_cache).resolve()
    research = ROOT / 'artifacts/research'
    if (not output.is_relative_to(research) or output == research or output.exists()
            or not cache_path.is_relative_to(research) or cache_path == research):
        parser.error('Use a new research output directory and a research execution cache')
    output.mkdir(parents=True)
    files = (*SCALE_FILES, 'optimization/models.py', 'optimization/accelerated_replay.py', 'optimization/brick_jobs.py',
             'tests/test_accelerated_replay.py')
    report = {'status': 'TESTING', 'holdout_performance_opened': False, 'final_optimization_run': False,
              'production_mutations': False, 'source_hashes': {p: sha256((ROOT/p).read_bytes()).hexdigest() for p in files}}
    def persist():
        (output/'acceleration.json').write_text(json.dumps(report, indent=2, default=str)+'\n')
    def progress(item):
        print(json.dumps(item), flush=True)
    persist()
    try:
        loader, log = unittest.TestLoader(), io.StringIO()
        modules = (*TEST_MODULES, 'test_indexed_market', 'test_accelerated_replay')
        suite = unittest.TestSuite(loader.discover(str(ROOT/'tests'), pattern=f'{module}.py') for module in modules)
        with contextlib.redirect_stdout(log), contextlib.redirect_stderr(log):
            result = unittest.TextTestRunner(stream=log, verbosity=2, resultclass=EvidenceResult).run(suite)
        (output/'tests.log').write_text(log.getvalue())
        report['tests'] = {'run': result.testsRun, 'passed': len(result.passed),
                          'failures': [(t.id(), d) for t,d in result.failures],
                          'errors': [(t.id(), d) for t,d in result.errors],
                          'skipped': [(t.id(), d) for t,d in result.skipped]}
        diff = subprocess.run(['git','diff','--check'], capture_output=True, text=True, cwd=ROOT)
        report['diff_check'] = {'exit_code': diff.returncode, 'output': diff.stdout+diff.stderr}
        persist()
        if not result.wasSuccessful() or result.skipped or diff.returncode:
            raise ValueError('Final regression/equivalence gate failed')
        progress({'status':'TESTS_PASSED','count':result.testsRun})
        started = time.monotonic()
        data = IndexedDataset(ROOT/'artifacts/research/tsla_510_sqlite_readiness_20260926/training')
        report['index_verification_seconds'] = time.monotonic()-started
        readiness = json.loads((ROOT/'artifacts/research/tsla_20plus5_20260926_backfill/data_readiness.json').read_text())
        partition = ResearchPartition(tuple(map(date.fromisoformat,readiness['training'])),
                                      tuple(map(date.fromisoformat,readiness['holdout'])))
        if data.partition != 'TRAIN' or data.sessions != partition.optimization or len(data.raw_trades) != 18451233:
            raise ValueError('Not the approved full training dataset')
        report.update(training_identity=data.identity, training_raw_records=len(data.raw_trades), training_sessions=20)
        cache = ExecutionCache.prepare(data, cache_path, progress=progress)
        report['execution_cache'] = cache.proof
        _, friction = load_frozen_calibration(ROOT/'artifacts/research/tsla_empirical_friction_20260925_final/calibration.json', readiness['holdout'])
        template_path = ROOT/'artifacts/production_health/20260908/deployed_camatron_config.json'
        traders = [t for t in load_enabled_renko_traders(str(template_path), ('all-enabled-renko',),
                   included_strategy_types=('renko_tp_sl',)) if t.symbol == 'TSLA']
        if len(traders) != 1:
            raise ValueError('Ambiguous historical research template')
        trader = replace(traders[0], strategy_config={**traders[0].strategy_config,
                         'active_start_date': None, 'active_end_date': None})
        cfg = approved_universe_config(OptimizationConfig('tsla_acceleration_readiness', str(template_path),
                                                          data.sessions, execution_friction=friction))
        candidates = generate_challengers(cfg, trader, data, partition)
        report['candidate_universe_count'] = len(candidates)
        (output/'universe.json').write_text(json.dumps([asdict(c) for c in candidates], default=str)+'\n')
        fixed = next(c for c in candidates if c.spec().brick_pct == Decimal('.41')
                     and c.spec().strategy_parameters.entry_confirmation == 'E1'
                     and c.spec().strategy_parameters.take_profit_bricks == 2
                     and c.spec().strategy_parameters.stop_loss_bricks == 1)
        report['status'] = 'FULL_TRAINING_SERIAL_PROBE'
        persist()
        progress({'status':report['status']})
        serial, stats = run_brick_jobs((fixed,), data, cache, cfg, output/'serial.sqlite', progress=progress)
        report['serial'] = stats
        report['serial_result_hash'] = fingerprint(serial[fixed.identity])
        persist()
        progress({'status':'SERIAL_COMPLETE', 'seconds':stats['elapsed_seconds']})
        # One full-training reference check, never the complete universe.
        started = time.monotonic()
        reference = evaluate_training(fixed, data, cfg, use_renko_cache=True)
        reference_row = json.loads(canonical_json(reference.row))
        report['full_training_reference'] = {'seconds': time.monotonic()-started,
            'status': reference.status, 'exact_equal': reference_row == serial[fixed.identity],
            'differing_fields': [k for k in reference_row if reference_row[k] != serial[fixed.identity].get(k)]}
        (output/'reference_result.json').write_text(canonical_json(reference_row)+'\n')
        persist()
        if reference.status != 'COMPLETED' or reference_row != serial[fixed.identity]:
            raise ValueError('Full-training reference/accelerated mismatch')
        progress({'status':'FULL_TRAINING_EXACT_PARITY_PASS'})
        if args.serial_only:
            report.update(status='SERIAL_VALIDATED', verdict='OPTIMIZATION ENGINE NOT READY',
                          limitation='Parallel resource benchmarks not executed by this invocation')
        else:
            bricks = tuple(map(Decimal, ('.16','.20','.25','.30','.35','.40','.41','.46','.51','.56','.61','.66')))
            workload = tuple(c for c in candidates if c.spec().brick_pct in bricks)
            if len(workload) != 120:
                raise ValueError('Predetermined benchmark workload changed')
            report['benchmark_bricks'] = list(map(str,bricks))
            report['parallel'] = {}
            expected = None
            for workers in args.workers:
                report['status'] = f'BENCHMARK_{workers}_WORKERS'
                persist()
                rows, measured = run_brick_jobs(workload, data, cache, cfg,
                    output/f'workers-{workers}.sqlite', workers=workers, progress=progress)
                hashes = {identity:fingerprint(row) for identity,row in rows.items()}
                if expected is not None and expected != hashes:
                    raise ValueError('Parallel workload results changed with worker count')
                expected = hashes
                measured['projected_510_hours'] = measured['elapsed_seconds'] / 120 * 510 / 3600
                measured['result_hashes'] = hashes
                report['parallel'][str(workers)] = measured
                persist()
                if measured['unexpected_failures'] or measured['valid_completed'] + measured['completed_invalid'] != 120:
                    raise ValueError('Unexpected benchmark candidate failures')
                resumed, resume_stats = run_brick_jobs(tuple(reversed(workload)), data, cache, cfg,
                    output/f'workers-{workers}.sqlite', workers=workers)
                if resumed != rows or resume_stats['evaluated'] or resume_stats['skipped'] != 120:
                    raise ValueError('Benchmark resume did not reuse all verified terminal outcomes')
                measured['resume_verified'] = True
                measured['worker_scaling_efficiency_vs_4'] = (
                    report['parallel']['4']['elapsed_seconds'] / measured['elapsed_seconds'] * 4 / workers
                    if '4' in report['parallel'] else None)
                measured['projection_limitation'] = 'Terminal-result throughput for this fixed valid/invalid workload mix; full universe mix may differ.'
                if expected == hashes and workers == args.workers[0]:
                    report['invalid_reference_parity'] = []
                    for candidate in workload:
                        if rows[candidate.identity]['candidate_status'] != 'COMPLETED_INVALID':
                            continue
                        reference = evaluate_training(candidate, data, cfg, use_renko_cache=True)
                        equal = json.loads(canonical_json(reference.row)) == rows[candidate.identity]
                        report['invalid_reference_parity'].append({'candidate_identity': candidate.identity, 'exact_equal': equal})
                        if not equal:
                            raise ValueError('Invalid candidate reference/accelerated mismatch')
                persist()
            if not report['parallel']:
                raise ValueError('No parallel performance measurement')
            selected = min(report['parallel'], key=lambda key:report['parallel'][key]['elapsed_seconds'])
            report['selected_workers'] = int(selected)
            report['projected_510_hours'] = report['parallel'][selected]['projected_510_hours']
            if report['projected_510_hours'] >= 86.83473505683666 / 2:
                raise ValueError('Acceleration has not materially reduced measured runtime')
            report.update(status='COMPLETE', verdict='OPTIMIZATION ENGINE READY')
        if report['source_hashes'] != {p:sha256((ROOT/p).read_bytes()).hexdigest() for p in files}:
            raise ValueError('Source changed during acceleration validation')
    except Exception as exc:
        report.update(status='FAILED', verdict='OPTIMIZATION ENGINE NOT READY', error=f'{type(exc).__name__}: {exc}')
    persist()
    progress({'status':report['status'], 'verdict':report['verdict'], 'report':str(output/'acceleration.json'),
              'error':report.get('error')})
    return 0 if report['status'] in ('COMPLETE','SERIAL_VALIDATED') else 1


if __name__ == "__main__":
    raise SystemExit(main())
