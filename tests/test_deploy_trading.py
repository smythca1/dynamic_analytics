from __future__ import annotations

import os
import re
import shlex
import shutil
import stat
import subprocess
import sys
import tempfile
import unittest
import json
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = REPO_ROOT / "deploy-trading"


class DeployTradingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.repo = self.root / "repo"
        self.repo.mkdir()
        shutil.copy2(SCRIPT, self.repo / "deploy-trading")
        (self.repo / "deploy-trading").chmod(0o755)
        self._git("init")
        self._git("config", "user.email", "deploy-test@example.com")
        self._git("config", "user.name", "Deploy Test")
        self._write_payload("committed")
        self._git("add", ".")
        self._git("commit", "-m", "initial")
        self.commit = self._git("rev-parse", "HEAD").stdout.strip()

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _git(self, *args: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(["git", *args], cwd=self.repo, text=True, capture_output=True, check=True)

    def _run(self, *args: str, check: bool = True, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
        merged_env = os.environ.copy()
        if env:
            merged_env.update(env)
        return subprocess.run(
            ["./deploy-trading", *args],
            cwd=self.repo,
            text=True,
            capture_output=True,
            check=check,
            env=merged_env,
        )

    def _write(self, relative: str, text: str) -> None:
        path = self.repo / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")

    def _write_payload(self, marker: str) -> None:
        for filename in (
            "alpaca_trader.py",
            "broker_protection.py",
            "execution_engine.py",
            "exit_plan.py",
            "fleet_runner.py",
            "mission_control_telemetry.py",
            "monitoring_app.py",
            "renko_builder.py",
            "reversal_protection.py",
            "strategy_engine.py",
            "trading_events.py",
        ):
            self._write(filename, f"# {filename} {marker}\n")
        self._write("requirements.txt", "pandas\n")
        self._write("mission-control.service", "[Service]\nExecStart=mission\n")
        self._write("trading-fleet-live.service", "[Service]\nExecStart=fleet\n")
        self._write("shared/__init__.py", "# shared\n")
        self._write("shared/config/__init__.py", "# config\n")
        self._write(
            "shared/config/trader_config.py",
            "def build_strategy_config(trader):\n    return trader\n\n"
            "def validate_single_strategy_kernel_config(data, *, config_path=None):\n    return None\n",
        )
        self._write("silver/__init__.py", "# silver\n")
        self._write("silver/market/__init__.py", "# market\n")
        self._write("silver/market/one_second_sampler.py", "# sampler\n")
        self._write("silver/evidence/__init__.py", "# evidence\n")
        self._write("silver/evidence/logger.py", "# logger\n")
        self._write("silver/evidence/shadow_sampler.py", "# shadow\n")
        self._write("configs/traders.live.example.json", '{"traders": [{"id": "t1", "enabled": true}]}\n')
        self._write(
            "configs/market-data.live.example.json",
            '{"symbols": ["TSLA"], "contract_fingerprints": {"TSLA": "t"}}\n',
        )
        self._write("replay/engine.py", "# excluded\n")
        self._write("tests/test_example.py", "# excluded\n")
        self._write("artifacts/live/out.jsonl", "{}\n")
        self._write(".env", "SECRET=do-not-deploy\n")
        self._write("venv/bin/python", "venv\n")
        self._write("validated_renko_backtest.cpp", "// excluded unless required\n")

    def test_explicit_commit_resolves_to_full_sha(self) -> None:
        result = self._run("--commit", self.commit[:8], "--dry-run")
        self.assertIn(f"Resolved commit: {self.commit}", result.stdout)

    def test_invalid_commit_fails(self) -> None:
        result = self._run("--commit", "not-a-commit", "--dry-run", check=False)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("invalid commit", result.stderr)

    def test_invalid_commit_fails_before_archive_creation(self) -> None:
        fake_bin = self._fake_git_bin(
            "printf '%s\\n' \"$*\" >> \"$DEPLOY_TEST_GIT_LOG\"\n"
            "if [[ \"$1\" == \"rev-parse\" ]]; then exit 44; fi\n"
        )
        log = self.root / "git-invalid.log"
        result = self._run(
            "--commit",
            "bad-commit",
            "--dry-run",
            check=False,
            env={
                "PATH": f"{fake_bin}:{os.environ['PATH']}",
                "DEPLOY_TEST_GIT_LOG": str(log),
            },
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("invalid commit", result.stderr)
        self.assertNotIn("archive", log.read_text(encoding="utf-8"))

    def test_dirty_working_tree_cannot_affect_payload(self) -> None:
        committed_hash = self._sha256(self.repo / "fleet_runner.py")
        self._write("fleet_runner.py", "event = runtime.execution.execute(intent)\nexecution_status_failed\n")
        result = self._run("--commit", self.commit, "--dry-run")
        self.assertIn("Dirty caller worktree: yes - ignored for payload", result.stdout)
        self.assertIn(f"fleet_runner.py {committed_hash}", result.stdout)
        self.assertNotIn("execution_status_failed", result.stdout)

    def test_clean_source_extraction_uses_git_archive_and_cleans_up_on_success(self) -> None:
        result = self._run("--commit", self.commit, "--dry-run")
        self.assertIn("Source materialization: git archive", result.stdout)
        self.assertIn("Commit source verification: required files present", result.stdout)
        source_path = self._extract_commit_source_path(result.stdout)
        self.assertFalse(source_path.exists())

    def test_archive_source_works_when_git_worktrees_is_not_writable(self) -> None:
        fake_bin = self._fake_git_bin(
            "if [[ \"$1\" == \"worktree\" ]]; then exit 44; fi\n"
        )
        result = self._run(
            "--commit",
            self.commit,
            "--dry-run",
            env={"PATH": f"{fake_bin}:{os.environ['PATH']}"},
        )
        self.assertEqual(result.returncode, 0)
        self.assertIn("Source materialization: git archive", result.stdout)

    def test_shared_and_silver_are_included(self) -> None:
        result = self._run("--commit", self.commit, "--dry-run")
        self.assertIn("  shared", result.stdout)
        self.assertIn("  silver", result.stdout)

    def test_mission_control_telemetry_runtime_file_is_included(self) -> None:
        result = self._run("--commit", self.commit, "--dry-run")
        self.assertIn("  mission_control_telemetry.py", result.stdout)
        self.assertIn("mission_control_telemetry.py", result.stdout.split("Validation commands:", 1)[1])

    def test_excluded_paths_are_not_included(self) -> None:
        result = self._run("--commit", self.commit, "--dry-run")
        includes = self._payload_includes(result.stdout)
        self.assertNotIn("replay", includes)
        self.assertNotIn("tests", includes)
        self.assertNotIn("artifacts", includes)
        self.assertNotIn(".env", includes)
        self.assertNotIn("venv", includes)
        self.assertNotIn("validated_renko_backtest.cpp", includes)

    def test_config_preserved_by_default(self) -> None:
        result = self._run("--commit", self.commit, "--dry-run")
        self.assertIn("Config policy: preserve_remote", result.stdout)
        self.assertNotIn(f"  configs/traders.live.example.json\n", result.stdout)

    def test_deploy_config_explicitly_includes_config(self) -> None:
        result = self._run("--commit", self.commit, "--dry-run", "--deploy-config")
        self.assertIn("Config policy: replace_from_commit", result.stdout)
        self.assertIn("  configs/traders.live.example.json", result.stdout)
        self.assertIn("  configs/market-data.live.example.json", result.stdout)

    def test_dry_run_reports_activation_gates_as_not_evaluated(self) -> None:
        result = self._run("--commit", self.commit, "--dry-run", "--deploy-config")
        self.assertIn("broker and checkpoint gates required; not evaluated in dry-run", result.stdout)
        self.assertIn("blocked pending authenticated broker and runtime checkpoint evidence", result.stdout)

    def test_activation_gate_discovers_all_checkpoint_identities(self) -> None:
        source = self._git("show", f"{self.commit}:deploy-trading").stdout
        self.assertIn("checkpoints[identity] = payload", source)
        self.assertIn("checkpoint identity missing", source)
        self.assertIn("checkpoint path is not identity-specific", source)

    def test_activation_gate_authorizes_independent_spxc_and_megatron_consumers(self) -> None:
        source = self._git("show", f"{self.commit}:deploy-trading").stdout
        self.assertIn('configs/accounts/spxc.live.json', source)
        self.assertIn('configs/accounts/megatron.live.json', source)

    def test_rollback_dry_run_preserves_historical_state(self) -> None:
        result = self._run("--rollback", "deploy-1", "--dry-run")
        self.assertIn("Would preserve artifacts, venv, evidence, logs, and state.", result.stdout)
        self.assertNotIn("delete checkpoints", result.stdout.lower())

    def test_default_deployment_performs_no_restart(self) -> None:
        result = self._run("--commit", self.commit, "--dry-run")
        self.assertIn("Services restarted by this command: none", result.stdout)

    def test_no_restart_flag_is_accepted_and_does_not_restart(self) -> None:
        result = self._run("--commit", self.commit, "--dry-run", "--no-restart")
        self.assertIn("Services restarted by this command: none", result.stdout)

    def test_generated_deployment_id_uses_microseconds_and_process_suffix(self) -> None:
        result = self._run("--commit", self.commit, "--dry-run")
        match = re.search(r"Deployment ID: (\S+)", result.stdout)
        self.assertIsNotNone(match)
        deployment_id = match.group(1)
        self.assertRegex(deployment_id, r"^\d{8}T\d{12}Z-[0-9a-f]{12}-[0-9a-f]+$")

    def test_restart_fleet_targets_only_fleet_service(self) -> None:
        fake_bin = self._fake_bin()
        result = self._run("--restart-fleet", env={"PATH": f"{fake_bin}:{os.environ['PATH']}"})
        self.assertIn("trading-fleet-live.service", (fake_bin / "ssh.log").read_text())
        self.assertNotIn("mission-control.service", (fake_bin / "ssh.log").read_text())
        self.assertEqual(result.returncode, 0)

    def test_dry_run_performs_no_ssh_or_scp(self) -> None:
        fake_bin = self._fake_bin()
        result = self._run("--commit", self.commit, "--dry-run", env={"PATH": f"{fake_bin}:{os.environ['PATH']}"})
        self.assertIn("no SSH, SCP", result.stdout)
        self.assertFalse((fake_bin / "ssh.log").exists())
        self.assertFalse((fake_bin / "scp.log").exists())

    def test_valid_deployment_id_is_accepted_for_rollback_dry_run(self) -> None:
        result = self._run("--rollback", "20260802T010203Z-abcdef123456", "--dry-run")
        self.assertIn("Rollback deployment ID: 20260802T010203Z-abcdef123456", result.stdout)

    def test_invalid_rollback_ids_fail_before_remote_operations(self) -> None:
        invalid_ids = [
            "../../x",
            "/root/x",
            "a/b",
            r"a\b",
            "a'b",
            'a"b',
            "a\nb",
            "a;b",
            "",
            "a" * 129,
            ".",
            "..",
        ]
        for deployment_id in invalid_ids:
            with self.subTest(deployment_id=deployment_id):
                fake_bin = self._fake_bin()
                result = self._run(
                    "--rollback",
                    deployment_id,
                    "--dry-run",
                    check=False,
                    env={"PATH": f"{fake_bin}:{os.environ['PATH']}"},
                )
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("invalid deployment ID", result.stderr)
                self.assertFalse((fake_bin / "ssh.log").exists())
                self.assertFalse((fake_bin / "scp.log").exists())

    def test_unsafe_remote_dir_fails_before_remote_operations(self) -> None:
        fake_bin = self._fake_bin()
        result = self._run(
            "--rollback",
            "deploy-1",
            "--dry-run",
            "--remote-dir",
            "/root/trading;rm -rf",
            check=False,
            env={"PATH": f"{fake_bin}:{os.environ['PATH']}"},
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("unsafe characters", result.stderr)
        self.assertFalse((fake_bin / "ssh.log").exists())
        self.assertFalse((fake_bin / "scp.log").exists())

    def test_remote_dir_newline_and_metacharacters_rejected(self) -> None:
        invalid_remote_dirs = [
            "/root/trading\nserver",
            "/root/trading'server",
            '/root/trading"server',
            "/root/trading$server",
            "/root/trading;server",
        ]
        for remote_dir in invalid_remote_dirs:
            with self.subTest(remote_dir=remote_dir):
                fake_bin = self._fake_bin()
                result = self._run(
                    "--rollback",
                    "deploy-1",
                    "--dry-run",
                    "--remote-dir",
                    remote_dir,
                    check=False,
                    env={"PATH": f"{fake_bin}:{os.environ['PATH']}"},
                )
                self.assertNotEqual(result.returncode, 0)
                self.assertFalse((fake_bin / "ssh.log").exists())
                self.assertFalse((fake_bin / "scp.log").exists())

    def test_generated_python_source_does_not_embed_user_values(self) -> None:
        script = (self.repo / "deploy-trading").read_text(encoding="utf-8")
        self.assertNotIn("Path('${REMOTE_DIR}", script)
        self.assertNotIn("Path('${staging_dir}", script)
        self.assertNotIn('"deployment_id": "$deployment_id"', script)
        self.assertIn("Path(sys.argv[4])", script)
        self.assertIn("Path(sys.argv[5])", script)
        self.assertIn('"deployment_id": sys.argv[1]', script)

    def test_remote_python_source_is_stable_with_different_user_values(self) -> None:
        script = (self.repo / "deploy-trading").read_text(encoding="utf-8")
        self.assertNotIn("Path('/root/trading-server", script)
        self.assertNotIn("20260802T010203Z", script)
        self.assertNotIn("unicode-", script)

    def test_manifest_uses_requested_commit_in_script(self) -> None:
        script = (self.repo / "deploy-trading").read_text()
        self.assertIn('"git_commit": os.environ.get("DEPLOY_TEST_MANIFEST_COMMIT_OVERRIDE", commit)', script)
        self.assertIn('"deployment_id": deployment_id', script)

    def test_generated_manifest_contains_exactly_one_json_object(self) -> None:
        fake_bin = self._fake_bin()
        self._run("--commit", self.commit, "--no-restart", env={"PATH": f"{fake_bin}:{os.environ['PATH']}"})
        manifest = fake_bin / "scp_payload/DEPLOYED_VERSION.json"
        text = manifest.read_text(encoding="utf-8")
        parsed = json.loads(text)
        self.assertEqual(self.commit, parsed["git_commit"])
        self.assertEqual(1, text.count('"git_commit"'))
        self.assertFalse(text.endswith("\\n"))

    def test_manifest_carries_explicit_historical_authorizations_without_remote_inference(self) -> None:
        fake_bin = self._fake_bin()
        authorization = json.dumps([{
            "trader_id": "retired-tsla",
            "checkpoint_path": "artifacts/live/state/retired-tsla.json",
            "sha256": "a" * 64,
            "symbol": "TSLA",
            "strategy_type": "renko_reversal",
            "strategy_identity": "canonical_e1",
            "classification": "verified_historical",
            "restoration": "historical_non_restorable",
            "provenance": {"release_commit": "b" * 40},
            "reviewed_by": "operator",
            "reviewed_at_utc": "2026-10-28T18:00:00Z",
            "broker_evidence": {"position_flat": True, "open_orders": 0, "verified_at_utc": "2026-10-28T18:00:00Z"},
        }])
        self._run("--commit", self.commit, "--no-restart", env={
            "PATH": f"{fake_bin}:{os.environ['PATH']}",
            "DEPLOY_HISTORICAL_CHECKPOINT_AUTHORIZATIONS": authorization,
        })
        manifest = json.loads((fake_bin / "scp_payload/DEPLOYED_VERSION.json").read_text(encoding="utf-8"))
        self.assertEqual(manifest["historical_checkpoint_authorizations"][0]["trader_id"], "retired-tsla")

    def test_generated_manifest_parses_and_has_one_real_trailing_newline(self) -> None:
        fake_bin = self._fake_bin()
        self._run("--commit", self.commit, "--no-restart", env={"PATH": f"{fake_bin}:{os.environ['PATH']}"})
        text = (fake_bin / "scp_payload/DEPLOYED_VERSION.json").read_text(encoding="utf-8")
        self.assertEqual("\n", text[-1])
        self.assertNotEqual("\n\n", text[-2:])
        self.assertEqual("generated", json.loads(text)["validation_status"])

    def test_literal_backslash_n_after_manifest_json_is_rejected(self) -> None:
        fake_bin = self._fake_bin()
        result = self._run(
            "--commit",
            self.commit,
            "--no-restart",
            check=False,
            env={
                "PATH": f"{fake_bin}:{os.environ['PATH']}",
                "DEPLOY_TEST_LITERAL_MANIFEST_SUFFIX": "1",
            },
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse((fake_bin / "ssh.log").exists())
        self.assertFalse((fake_bin / "scp.log").exists())

    def test_concatenated_manifest_json_objects_are_rejected(self) -> None:
        runtime = self.root / "manifest-runtime"
        runtime.mkdir()
        manifest = runtime / "DEPLOYED_VERSION.json"
        manifest.write_text(
            json.dumps(self._manifest_dict("deploy-c", self.commit)) + "\n{}\n",
            encoding="utf-8",
        )
        script = self._internal_script_args(
            "mark-manifest-validated-script",
            runtime,
            "deploy-c",
            self.commit,
            manifest,
        )
        result = self._run_shell_script(script)
        self.assertNotEqual(result.returncode, 0)

    def test_required_manifest_fields_are_validated(self) -> None:
        runtime = self.root / "manifest-required-runtime"
        runtime.mkdir()
        manifest = runtime / "DEPLOYED_VERSION.json"
        data = self._manifest_dict("deploy-required", self.commit)
        del data["payload_groups"]
        manifest.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        script = self._internal_script_args(
            "mark-manifest-validated-script",
            runtime,
            "deploy-required",
            self.commit,
            manifest,
        )
        result = self._run_shell_script(script)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("manifest missing required fields", result.stderr)

    def test_staged_manifest_validation_marks_passed_before_promotion(self) -> None:
        runtime = self.root / "manifest-pass-runtime"
        runtime.mkdir()
        manifest = runtime / "DEPLOYED_VERSION.json"
        manifest.write_text(
            json.dumps(self._manifest_dict("deploy-pass", self.commit), indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        script = self._internal_script_args(
            "mark-manifest-validated-script",
            runtime,
            "deploy-pass",
            self.commit,
            manifest,
        )
        result = self._run_shell_script(script)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual("passed", json.loads(manifest.read_text(encoding="utf-8"))["validation_status"])

    def test_wrong_commit_in_manifest_fails_before_upload_or_promotion(self) -> None:
        fake_bin = self._fake_bin()
        result = self._run(
            "--commit",
            self.commit,
            "--no-restart",
            check=False,
            env={
                "PATH": f"{fake_bin}:{os.environ['PATH']}",
                "DEPLOY_TEST_MANIFEST_COMMIT_OVERRIDE": "badbadbadbadbadbadbadbadbadbadbadbadbadb",
            },
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse((fake_bin / "ssh.log").exists())
        self.assertFalse((fake_bin / "scp.log").exists())

    def test_wrong_deployment_id_in_manifest_fails_before_upload_or_promotion(self) -> None:
        fake_bin = self._fake_bin()
        result = self._run(
            "--commit",
            self.commit,
            "--no-restart",
            check=False,
            env={
                "PATH": f"{fake_bin}:{os.environ['PATH']}",
                "DEPLOY_TEST_MANIFEST_DEPLOYMENT_ID_OVERRIDE": "wrong-id",
            },
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse((fake_bin / "ssh.log").exists())
        self.assertFalse((fake_bin / "scp.log").exists())

    def test_manifest_generation_failure_prevents_promotion(self) -> None:
        fake_bin = self._fake_bin()
        result = self._run(
            "--commit",
            self.commit,
            "--no-restart",
            check=False,
            env={
                "PATH": f"{fake_bin}:{os.environ['PATH']}",
                "DEPLOY_TEST_MANIFEST_STATUS_OVERRIDE": "not_valid",
            },
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertNotIn("Promoting staged runtime files", result.stdout)
        self.assertFalse((fake_bin / "ssh.log").exists())

    def test_final_manifest_install_is_atomic_and_updates_success_marker(self) -> None:
        runtime = self.root / "install-runtime"
        runtime.mkdir()
        staged = runtime / "staged.json"
        final = runtime / "DEPLOYED_VERSION.json"
        data = self._manifest_dict("deploy-install", self.commit)
        data["validation_status"] = "passed"
        staged.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        script = self._internal_script_args(
            "install-manifest-script",
            runtime,
            "deploy-install",
            self.commit,
            staged,
            final,
        )
        result = self._run_shell_script(script)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertFalse(list(runtime.glob("DEPLOYED_VERSION.json.__tmp__.*")))
        installed = json.loads(final.read_text(encoding="utf-8"))
        self.assertEqual("passed", installed["validation_status"])
        self.assertEqual(json.loads(staged.read_text(encoding="utf-8")), installed)
        self.assertEqual("deploy-install", (runtime / ".last_successful_deployment_id").read_text(encoding="utf-8").strip())

    def test_final_manifest_write_failure_reports_incomplete_and_no_success_marker(self) -> None:
        fake_bin = self._fake_bin()
        result = self._run(
            "--commit",
            self.commit,
            "--no-restart",
            check=False,
            env={
                "PATH": f"{fake_bin}:{os.environ['PATH']}",
                "DEPLOY_TEST_FAIL_SSH_CONTAINS": "DEPLOY_TEST_FAIL_FINAL_MANIFEST_WRITE",
            },
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("INCOMPLETE_DEPLOYMENT", result.stderr)
        self.assertNotIn("Cleaning promoted old runtime siblings", result.stdout)

    def test_final_manifest_write_failure_does_not_restart_services(self) -> None:
        fake_bin = self._fake_bin()
        result = self._run(
            "--commit",
            self.commit,
            "--no-restart",
            check=False,
            env={
                "PATH": f"{fake_bin}:{os.environ['PATH']}",
                "DEPLOY_TEST_FAIL_SSH_CONTAINS": "DEPLOY_TEST_FAIL_FINAL_MANIFEST_WRITE",
            },
        )
        self.assertNotEqual(result.returncode, 0)
        ssh_log = (fake_bin / "ssh.log").read_text(encoding="utf-8")
        self.assertNotIn("systemctl restart", ssh_log)

    def test_old_siblings_remain_after_incomplete_deployment(self) -> None:
        runtime, staging = self._runtime_and_staging()
        promote = self._internal_script("promote-script", runtime, "deploy-incomplete", staging)
        self.assertEqual(self._run_shell_script(promote).returncode, 0)
        self.assertTrue((runtime / "shared.__old__.deploy-incomplete").exists())
        self.assertTrue((runtime / "fleet_runner.py.__old__.deploy-incomplete").exists())
        self.assertFalse((runtime / ".last_successful_deployment_id").exists())

    def test_last_attempted_records_promotion_attempt(self) -> None:
        runtime, staging = self._runtime_and_staging()
        promote = self._internal_script("promote-script", runtime, "deploy-attempted", staging)
        self.assertEqual(self._run_shell_script(promote).returncode, 0)
        self.assertEqual("deploy-attempted", (runtime / ".last_attempted_deployment_id").read_text(encoding="utf-8").strip())

    def test_last_successful_deployment_id_updates_only_after_manifest_success(self) -> None:
        runtime, staging = self._runtime_and_staging()
        promote = self._internal_script("promote-script", runtime, "deploy-marker", staging)
        self.assertEqual(self._run_shell_script(promote).returncode, 0)
        self.assertFalse((runtime / ".last_successful_deployment_id").exists())
        staged = runtime / "staged-marker.json"
        final = runtime / "DEPLOYED_VERSION.json"
        data = self._manifest_dict("deploy-marker", self.commit)
        data["validation_status"] = "passed"
        staged.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        install = self._internal_script_args("install-manifest-script", runtime, "deploy-marker", self.commit, staged, final)
        self.assertEqual(self._run_shell_script(install).returncode, 0)
        self.assertEqual("deploy-marker", (runtime / ".last_successful_deployment_id").read_text(encoding="utf-8").strip())

    def test_rerun_after_incomplete_deployment_uses_new_id_and_preserves_prior_failed_artifacts(self) -> None:
        runtime, staging = self._runtime_and_staging()
        first = self._internal_script("promote-script", runtime, "deploy-old", staging)
        self.assertEqual(self._run_shell_script(first).returncode, 0)
        self.assertTrue((runtime / "shared.__old__.deploy-old").exists())

        second_staging = self.root / "staging-rerun"
        second_staging.mkdir()
        self._write_runtime_payload(second_staging, "rerun")
        second = self._internal_script("promote-script", runtime, "deploy-new", second_staging)
        self.assertEqual(self._run_shell_script(second).returncode, 0)

        staged = runtime / "staged-new.json"
        final = runtime / "DEPLOYED_VERSION.json"
        data = self._manifest_dict("deploy-new", self.commit)
        data["validation_status"] = "passed"
        staged.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        install = self._internal_script_args("install-manifest-script", runtime, "deploy-new", self.commit, staged, final)
        self.assertEqual(self._run_shell_script(install).returncode, 0)

        self.assertEqual("rerun-shared", self._marker(runtime / "shared"))
        self.assertEqual("deploy-new", (runtime / ".last_successful_deployment_id").read_text(encoding="utf-8").strip())
        self.assertTrue((runtime / "shared.__old__.deploy-old").exists())

    def test_validation_excludes_execute_real(self) -> None:
        result = self._run("--commit", self.commit, "--dry-run")
        validation = result.stdout.split("Validation commands:", 1)[1]
        self.assertNotIn("--execute-real", validation)
        self.assertIn("python fleet_runner.py --help", validation)
        self.assertIn("validate_production_fleet_config", validation)

    def test_backup_excludes_persistent_artifacts_and_venv(self) -> None:
        script = (self.repo / "deploy-trading").read_text()
        backup_section = script.split("remote_backup_script()", 1)[1].split("remote_promote_script()", 1)[0]
        self.assertNotIn("artifacts", backup_section)
        self.assertNotIn("venv", backup_section)

    def test_rollback_dry_run_restores_only_backed_up_runtime_files(self) -> None:
        result = self._run("--rollback", "deploy-1", "--dry-run")
        self.assertIn("Would restore backed-up runtime files, shared/, silver/", result.stdout)
        self.assertIn("Would preserve artifacts, venv, evidence, logs, and state.", result.stdout)

    def test_rollback_validates_id_before_any_remote_command(self) -> None:
        fake_bin = self._fake_bin()
        result = self._run(
            "--rollback",
            "../../bad",
            check=False,
            env={"PATH": f"{fake_bin}:{os.environ['PATH']}"},
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse((fake_bin / "ssh.log").exists())
        self.assertFalse((fake_bin / "scp.log").exists())

    def test_missing_required_committed_file_fails_locally(self) -> None:
        for filename in ("broker_protection.py", "exit_plan.py", "reversal_protection.py"):
            self._write(filename, "# retained runtime helper\n")
        self._git("add", "broker_protection.py", "exit_plan.py", "reversal_protection.py")
        self._git("rm", "-r", "silver")
        self._git("commit", "-m", "remove silver")
        bad_commit = self._git("rev-parse", "HEAD").stdout.strip()
        result = self._run("--commit", bad_commit, "--dry-run", check=False)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("required payload path missing from commit source: silver", result.stderr)

    def test_temporary_archive_source_cleanup_on_archive_failure(self) -> None:
        fake_bin = self._fake_git_bin(
            "if [[ \"$1\" == \"rev-parse\" ]]; then echo 0123456789abcdef0123456789abcdef01234567; exit 0; fi\n"
            "if [[ \"$1\" == \"archive\" ]]; then exit 44; fi\n"
        )
        tmpdir = self.root / "archive-tmp"
        tmpdir.mkdir()
        result = self._run(
            "--commit",
            "abc123",
            "--dry-run",
            check=False,
            env={"PATH": f"{fake_bin}:{os.environ['PATH']}", "TMPDIR": str(tmpdir)},
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual([], list(tmpdir.glob("deploy-trading.*")))

    def test_no_ssh_or_scp_when_source_extraction_fails(self) -> None:
        fake_bin = self._fake_bin()
        self._fake_git_bin(
            "if [[ \"$1\" == \"rev-parse\" ]]; then echo 0123456789abcdef0123456789abcdef01234567; exit 0; fi\n"
            "if [[ \"$1\" == \"archive\" ]]; then exit 44; fi\n",
            root=fake_bin,
        )
        result = self._run(
            "--commit",
            "abc123",
            "--no-restart",
            check=False,
            env={"PATH": f"{fake_bin}:{os.environ['PATH']}"},
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse((fake_bin / "ssh.log").exists())
        self.assertFalse((fake_bin / "scp.log").exists())

    def test_local_temporary_resources_cleanup_after_upload_failure(self) -> None:
        fake_bin = self._fake_bin()
        tmpdir = self.root / "script-tmp"
        tmpdir.mkdir()
        result = self._run(
            "--commit",
            self.commit,
            "--no-restart",
            check=False,
            env={
                "PATH": f"{fake_bin}:{os.environ['PATH']}",
                "TMPDIR": str(tmpdir),
                "DEPLOY_TEST_FAIL_SCP": "1",
            },
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual([], list(tmpdir.glob("deploy-release.*")))
        self.assertEqual([], list(tmpdir.glob("deploy-trading.*")))

    def test_validation_failure_prevents_promotion(self) -> None:
        fake_bin = self._fake_bin()
        result = self._run(
            "--commit",
            self.commit,
            "--no-restart",
            check=False,
            env={
                "PATH": f"{fake_bin}:{os.environ['PATH']}",
                "DEPLOY_TEST_FAIL_SSH_CONTAINS": "py_compile",
            },
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Validating staged release", result.stdout)
        self.assertNotIn("Promoting staged runtime files", result.stdout)

    def test_promotion_failure_prevents_success_manifest(self) -> None:
        fake_bin = self._fake_bin()
        result = self._run(
            "--commit",
            self.commit,
            "--no-restart",
            check=False,
            env={
                "PATH": f"{fake_bin}:{os.environ['PATH']}",
                "DEPLOY_TEST_FAIL_SSH_CONTAINS": "activate_dir",
            },
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Promoting staged runtime files", result.stdout)
        self.assertNotIn("Writing deployment manifest", result.stdout)

    def test_shared_temporary_copy_failure_prevents_success_manifest(self) -> None:
        fake_bin = self._fake_bin()
        result = self._run(
            "--commit",
            self.commit,
            "--no-restart",
            check=False,
            env={
                "PATH": f"{fake_bin}:{os.environ['PATH']}",
                "DEPLOY_TEST_FAIL_SSH_CONTAINS": 'cp -a /root/trading-server/.deploy-staging',
            },
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertNotIn("Writing deployment manifest", result.stdout)

    def test_partial_promotion_restore_logic_is_present_for_files_and_packages(self) -> None:
        fake_bin = self._fake_bin()
        self._run("--commit", self.commit, "--no-restart", env={"PATH": f"{fake_bin}:{os.environ['PATH']}"})
        ssh_log = (fake_bin / "ssh.log").read_text(encoding="utf-8")
        self.assertIn("trap restore_promoted ERR", ssh_log)
        self.assertIn("txn_kind=()", ssh_log)
        self.assertIn('txn_state+=("prepared")', ssh_log)
        self.assertIn('set_txn_state "$idx" "old_moved"', ssh_log)
        self.assertIn('restore_txn "$idx"', ssh_log)
        self.assertIn("trap - ERR", ssh_log)

    def test_normal_deploy_invokes_upload_validation_backup_and_promotion_without_restart(self) -> None:
        fake_bin = self._fake_bin()
        result = self._run(
            "--commit",
            self.commit,
            "--no-restart",
            env={"PATH": f"{fake_bin}:{os.environ['PATH']}"},
        )
        self.assertEqual(result.returncode, 0)
        self.assertIn("Uploading release", result.stdout)
        self.assertIn("Validating staged release", result.stdout)
        self.assertIn("Validating deployment manifest before promotion", result.stdout)
        self.assertIn("Backing up replaced runtime files", result.stdout)
        self.assertIn("Promoting staged runtime files", result.stdout)
        self.assertLess(result.stdout.index("Validating deployment manifest before promotion"), result.stdout.index("Promoting staged runtime files"))
        ssh_log = (fake_bin / "ssh.log").read_text(encoding="utf-8")
        scp_log = (fake_bin / "scp.log").read_text(encoding="utf-8")
        self.assertIn("py_compile", ssh_log)
        self.assertIn("validate_production_fleet_config", ssh_log)
        self.assertIn("backup_metadata", ssh_log)
        self.assertIn("activate_dir", ssh_log)
        self.assertIn("DEPLOYED_VERSION.json", ssh_log)
        self.assertIn(".deploy-staging", scp_log)
        self.assertNotIn("systemctl restart", ssh_log)

    def test_shared_market_data_preflight_failure_prevents_backup_and_promotion(self) -> None:
        fake_bin = self._fake_bin()
        result = self._run(
            "--commit",
            self.commit,
            "--no-restart",
            check=False,
            env={
                "PATH": f"{fake_bin}:{os.environ['PATH']}",
                "DEPLOY_TEST_FAIL_SSH_CONTAINS": "validate_shared_market_data_config",
            },
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Validating staged release without live execution", result.stdout)
        self.assertNotIn("Backing up replaced runtime files", result.stdout)
        self.assertNotIn("Promoting staged runtime files", result.stdout)
        ssh_log = (fake_bin / "ssh.log").read_text(encoding="utf-8")
        self.assertIn("validate_shared_market_data_config", ssh_log)
        self.assertNotIn("backup_metadata", ssh_log)
        self.assertNotIn("activate_dir", ssh_log)

    def test_safe_promotion_script_uses_staged_activation_for_packages(self) -> None:
        fake_bin = self._fake_bin()
        self._run("--commit", self.commit, "--no-restart", env={"PATH": f"{fake_bin}:{os.environ['PATH']}"})
        ssh_log = (fake_bin / "ssh.log").read_text(encoding="utf-8")
        self.assertIn("restore_promoted", ssh_log)
        self.assertIn("for d in shared silver", ssh_log)
        self.assertIn('activate_dir "$d"', ssh_log)
        self.assertIn(".__new__.", ssh_log)
        self.assertIn(".__old__.", ssh_log)

    def test_successful_directory_activation_executes_on_filesystem(self) -> None:
        runtime, staging = self._runtime_and_staging()
        script = self._internal_script("promote-script", runtime, "deploy-1", staging)
        result = self._run_shell_script(script)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual("new-shared", self._marker(runtime / "shared"))
        self.assertEqual("new-silver", self._marker(runtime / "silver"))

    def test_directory_activation_failure_after_old_moved_restores_old(self) -> None:
        runtime, staging = self._runtime_and_staging()
        script = self._internal_script("promote-script", runtime, "deploy-2", staging)
        result = self._run_shell_script(script, env={"DEPLOY_TEST_FAIL_ACTIVATE_AFTER_OLD_MOVED": "shared"})
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual("old-shared", self._marker(runtime / "shared"))
        self.assertEqual("old-silver", self._marker(runtime / "silver"))
        self.assertFalse((runtime / ".last_deployment_id").exists())

    def test_successful_file_activation_executes_on_filesystem(self) -> None:
        runtime, staging = self._runtime_and_staging()
        script = self._internal_script("promote-script", runtime, "deploy-3", staging)
        result = self._run_shell_script(script)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual("# fleet_runner.py new\n", (runtime / "fleet_runner.py").read_text(encoding="utf-8"))

    def test_file_activation_failure_after_old_moved_restores_old(self) -> None:
        runtime, staging = self._runtime_and_staging()
        script = self._internal_script("promote-script", runtime, "deploy-4", staging)
        result = self._run_shell_script(script, env={"DEPLOY_TEST_FAIL_ACTIVATE_AFTER_OLD_MOVED": "fleet_runner.py"})
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual("# fleet_runner.py old\n", (runtime / "fleet_runner.py").read_text(encoding="utf-8"))
        self.assertEqual("old-shared", self._marker(runtime / "shared"))
        self.assertEqual("old-silver", self._marker(runtime / "silver"))

    def test_shared_success_silver_failure_restores_both_packages(self) -> None:
        runtime, staging = self._runtime_and_staging()
        script = self._internal_script("promote-script", runtime, "deploy-5", staging)
        result = self._run_shell_script(script, env={"DEPLOY_TEST_FAIL_ACTIVATE_AFTER_OLD_MOVED": "silver"})
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual("old-shared", self._marker(runtime / "shared"))
        self.assertEqual("old-silver", self._marker(runtime / "silver"))

    def test_top_level_file_restored_when_later_package_fails(self) -> None:
        runtime, staging = self._runtime_and_staging()
        runtime.joinpath("shared").rename(runtime / "shared.saved")
        runtime.joinpath("silver").rename(runtime / "silver.saved")
        staging.joinpath("silver").rename(staging / "silver.missing")
        script = self._internal_script("promote-script", runtime, "deploy-6", staging)
        result = self._run_shell_script(script, env={"DEPLOY_TEST_FAIL_ACTIVATE_AFTER_OLD_MOVED": "renko_builder.py"})
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual("# alpaca_trader.py old\n", (runtime / "alpaca_trader.py").read_text(encoding="utf-8"))
        self.assertEqual("# renko_builder.py old\n", (runtime / "renko_builder.py").read_text(encoding="utf-8"))
        runtime.joinpath("shared.saved").rename(runtime / "shared")
        runtime.joinpath("silver.saved").rename(runtime / "silver")
        staging.joinpath("silver.missing").rename(staging / "silver")

    def test_active_package_absent_before_failed_deploy_is_not_invented(self) -> None:
        runtime, staging = self._runtime_and_staging()
        shutil.rmtree(runtime / "shared")
        script = self._internal_script("promote-script", runtime, "deploy-7", staging)
        result = self._run_shell_script(script, env={"DEPLOY_TEST_FAIL_ACTIVATE_AFTER_OLD_MOVED": "shared"})
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse((runtime / "shared").exists())
        self.assertEqual("old-silver", self._marker(runtime / "silver"))

    def test_successful_deployment_leaves_shared_and_silver_active(self) -> None:
        runtime, staging = self._runtime_and_staging()
        script = self._internal_script("promote-script", runtime, "deploy-8", staging)
        result = self._run_shell_script(script)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue((runtime / "shared").is_dir())
        self.assertTrue((runtime / "silver").is_dir())
        self.assertEqual("deploy-8", (runtime / ".last_deployment_id").read_text(encoding="utf-8").strip())

    def test_cleanup_old_siblings_runs_after_success(self) -> None:
        runtime, staging = self._runtime_and_staging()
        promote = self._internal_script("promote-script", runtime, "deploy-9", staging)
        self.assertEqual(self._run_shell_script(promote).returncode, 0)
        self.assertTrue((runtime / "shared.__old__.deploy-9").exists())
        cleanup = self._internal_script("cleanup-old-script", runtime, "deploy-9")
        self.assertEqual(self._run_shell_script(cleanup).returncode, 0)
        self.assertFalse((runtime / "shared.__old__.deploy-9").exists())

    def test_success_manifest_absent_on_executable_promotion_failure(self) -> None:
        runtime, staging = self._runtime_and_staging()
        script = self._internal_script("promote-script", runtime, "deploy-10", staging)
        result = self._run_shell_script(script, env={"DEPLOY_TEST_FAIL_ACTIVATE_AFTER_OLD_MOVED": "silver"})
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse((runtime / "DEPLOYED_VERSION.json").exists())

    def test_success_manifest_written_only_after_complete_command_flow(self) -> None:
        fake_bin = self._fake_bin()
        self._run("--commit", self.commit, "--no-restart", env={"PATH": f"{fake_bin}:{os.environ['PATH']}"})
        ssh_log = (fake_bin / "ssh.log").read_text(encoding="utf-8")
        self.assertIn("DEPLOYED_VERSION.json", ssh_log)
        self.assertLess(ssh_log.index("activate_dir"), ssh_log.rindex("DEPLOYED_VERSION.json"))

    def test_successful_rollback_safely_activates_backup_packages(self) -> None:
        runtime, backup = self._runtime_and_backup()
        script = self._internal_script("rollback-script", runtime, "deploy-r1", backup)
        result = self._run_shell_script(script)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual("backup-shared", self._marker(runtime / "shared"))
        self.assertEqual("backup-silver", self._marker(runtime / "silver"))

    def test_rollback_package_copy_failure_preserves_current_active(self) -> None:
        runtime, backup = self._runtime_and_backup()
        script = self._internal_script("rollback-script", runtime, "deploy-r2", backup)
        result = self._run_shell_script(script, env={"DEPLOY_TEST_FAIL_ROLLBACK_COPY": "shared"})
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual("old-shared", self._marker(runtime / "shared"))
        self.assertEqual("old-silver", self._marker(runtime / "silver"))

    def test_rollback_activation_failure_restores_current_active_package(self) -> None:
        runtime, backup = self._runtime_and_backup()
        script = self._internal_script("rollback-script", runtime, "deploy-r3", backup)
        result = self._run_shell_script(script, env={"DEPLOY_TEST_FAIL_ROLLBACK_AFTER_OLD_MOVED": "shared"})
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual("old-shared", self._marker(runtime / "shared"))
        self.assertEqual("old-silver", self._marker(runtime / "silver"))

    def test_partial_rollback_failure_restores_all_changed_items(self) -> None:
        runtime, backup = self._runtime_and_backup()
        script = self._internal_script("rollback-script", runtime, "deploy-r4", backup)
        result = self._run_shell_script(script, env={"DEPLOY_TEST_FAIL_ROLLBACK_AFTER_OLD_MOVED": "silver"})
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual("old-shared", self._marker(runtime / "shared"))
        self.assertEqual("old-silver", self._marker(runtime / "silver"))

    def test_rollback_file_activation_failure_restores_current_file(self) -> None:
        runtime, backup = self._runtime_and_backup()
        script = self._internal_script("rollback-script", runtime, "deploy-r5", backup)
        result = self._run_shell_script(script, env={"DEPLOY_TEST_FAIL_ROLLBACK_AFTER_OLD_MOVED": "fleet_runner.py"})
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual("# fleet_runner.py old\n", (runtime / "fleet_runner.py").read_text(encoding="utf-8"))

    def test_service_restart_never_invoked_on_deploy_or_rollback_failure(self) -> None:
        runtime, staging = self._runtime_and_staging()
        script = self._internal_script("promote-script", runtime, "deploy-11", staging)
        result = self._run_shell_script(script, env={"DEPLOY_TEST_FAIL_ACTIVATE_AFTER_OLD_MOVED": "shared"})
        self.assertNotEqual(result.returncode, 0)
        self.assertNotIn("systemctl restart", script)
        runtime, backup = self._runtime_and_backup()
        rollback = self._internal_script("rollback-script", runtime, "deploy-r6", backup)
        result = self._run_shell_script(rollback, env={"DEPLOY_TEST_FAIL_ROLLBACK_AFTER_OLD_MOVED": "shared"})
        self.assertNotEqual(result.returncode, 0)
        self.assertNotIn("systemctl restart", rollback)

    def test_service_files_are_preserved_without_deploy_services(self) -> None:
        fake_bin = self._fake_bin()
        self._run("--commit", self.commit, "--no-restart", env={"PATH": f"{fake_bin}:{os.environ['PATH']}"})
        ssh_log = (fake_bin / "ssh.log").read_text(encoding="utf-8")
        self.assertNotIn("daemon-reload", ssh_log)
        result = self._run("--commit", self.commit, "--dry-run")
        self.assertIn("Service file policy: preserve_remote", result.stdout)

    def test_deploy_services_explicitly_includes_service_files(self) -> None:
        fake_bin = self._fake_bin()
        result = self._run(
            "--commit",
            self.commit,
            "--no-restart",
            "--deploy-services",
            env={"PATH": f"{fake_bin}:{os.environ['PATH']}"},
        )
        self.assertEqual(result.returncode, 0)
        ssh_log = (fake_bin / "ssh.log").read_text(encoding="utf-8")
        self.assertIn("daemon-reload", ssh_log)
        dry_run = self._run("--commit", self.commit, "--dry-run", "--deploy-services")
        self.assertIn("Service file policy: replace_from_commit", dry_run.stdout)
        self.assertIn("  mission-control.service", dry_run.stdout)
        self.assertIn("  trading-fleet-live.service", dry_run.stdout)

    def test_clean_commit_payload_not_dirty_working_tree_used_for_non_dry_deploy(self) -> None:
        self._write("fleet_runner.py", "dirty execution_status_should_not_deploy\n")
        fake_bin = self._fake_bin()
        self._run("--commit", self.commit, "--no-restart", env={"PATH": f"{fake_bin}:{os.environ['PATH']}"})
        source = fake_bin / "scp_payload"
        deployed_fleet = (source / "fleet_runner.py").read_text(encoding="utf-8")
        self.assertIn("committed", deployed_fleet)
        self.assertNotIn("execution_status_should_not_deploy", deployed_fleet)

    def test_extracted_fleet_runner_matches_git_show_for_requested_commit(self) -> None:
        self._write("fleet_runner.py", "dirty execution_status_should_not_deploy\n")
        fake_bin = self._fake_bin()
        self._run("--commit", self.commit, "--no-restart", env={"PATH": f"{fake_bin}:{os.environ['PATH']}"})
        deployed_fleet = (fake_bin / "scp_payload/fleet_runner.py").read_text(encoding="utf-8")
        committed_fleet = self._git("show", f"{self.commit}:fleet_runner.py").stdout
        self.assertEqual(committed_fleet, deployed_fleet)

    def test_shell_quoting_handles_paths_and_commit_args_safely(self) -> None:
        result = self._run("--commit", self.commit, "--dry-run", "--remote-dir", "/root/path with space")
        self.assertIn("Remote destination: root@157.230.54.46:/root/path with space", result.stdout)
        self.assertIn("source /root/path with space/venv/bin/activate", result.stdout)

    def test_commit_payload_excludes_dirty_execution_status_runtime_hunk(self) -> None:
        committed_fleet_runner = self._git("show", f"{self.commit}:fleet_runner.py").stdout
        self.assertNotIn("TERMINAL_EXECUTION_FAILURE_STATUSES", committed_fleet_runner)
        self.assertNotIn("execution_status_", committed_fleet_runner)

        self._write(
            "fleet_runner.py",
            "TERMINAL_EXECUTION_FAILURE_STATUSES = {'canceled'}\n"
            "def record_failure(status):\n"
            "    return {'error': f'execution_status_{status}'}\n",
        )
        self.assertIn("execution_status_", self._git("diff", "--", "fleet_runner.py").stdout)

        fake_bin = self._fake_bin()
        self._run("--commit", self.commit, "--no-restart", env={"PATH": f"{fake_bin}:{os.environ['PATH']}"})

        deployed_fleet = (fake_bin / "scp_payload/fleet_runner.py").read_text(encoding="utf-8")
        self.assertEqual(committed_fleet_runner, deployed_fleet)
        self.assertNotIn("TERMINAL_EXECUTION_FAILURE_STATUSES", deployed_fleet)
        self.assertNotIn("execution_status_", deployed_fleet)

    def test_existing_remote_config_path_preserved_without_deploy_config(self) -> None:
        result = self._run("--commit", self.commit, "--dry-run")
        self.assertIn("Config path used by service: /root/trading-server/configs/traders.live.example.json", result.stdout)
        self.assertIn("Config policy: preserve_remote", result.stdout)

    def _payload_includes(self, output: str) -> set[str]:
        block = output.split("Payload includes:", 1)[1].split("Payload excludes:", 1)[0]
        return {line.strip() for line in block.splitlines() if line.strip()}

    def _extract_commit_source_path(self, output: str) -> Path:
        match = re.search(r"Extracted source path: (.+)", output)
        self.assertIsNotNone(match)
        return Path(match.group(1).strip())

    def _fake_git_bin(self, git_prefix_script: str, root: Path | None = None) -> Path:
        fake_bin = root if root is not None else self.root / f"fake-git-bin-{len(list(self.root.glob('fake-git-bin-*')))}"
        fake_bin.mkdir(exist_ok=True)
        real_git = shutil.which("git")
        self.assertIsNotNone(real_git)
        git = fake_bin / "git"
        git.write_text(
            "#!/usr/bin/env bash\n"
            f"{git_prefix_script}"
            f"exec {shlex.quote(real_git)} \"$@\"\n",
            encoding="utf-8",
        )
        git.chmod(git.stat().st_mode | stat.S_IXUSR)
        return fake_bin

    def _fake_bin(self) -> Path:
        fake_bin = self.root / "fake-bin"
        fake_bin.mkdir(exist_ok=True)
        ssh = fake_bin / "ssh"
        ssh.write_text(
            "#!/usr/bin/env bash\n"
            f"printf '%s\\n' \"$*\" >> \"{fake_bin}/ssh.log\"\n"
            "if [[ -n \"${DEPLOY_TEST_FAIL_SSH_CONTAINS:-}\" && \"$*\" == *\"${DEPLOY_TEST_FAIL_SSH_CONTAINS}\"* ]]; then\n"
            "  exit 42\n"
            "fi\n",
            encoding="utf-8",
        )
        ssh.chmod(ssh.stat().st_mode | stat.S_IXUSR)
        scp = fake_bin / "scp"
        scp.write_text(
            "#!/usr/bin/env bash\n"
            f"printf '%s\\n' \"$*\" >> \"{fake_bin}/scp.log\"\n"
            "if [[ \"${DEPLOY_TEST_FAIL_SCP:-0}\" == \"1\" ]]; then\n"
            "  exit 43\n"
            "fi\n"
            f"rm -rf \"{fake_bin}/scp_payload\"\n"
            f"mkdir -p \"{fake_bin}/scp_payload\"\n"
            "args=(\"$@\")\n"
            "src=\"${args[$((${#args[@]} - 2))]}\"\n"
            f"cp -a \"$src\" \"{fake_bin}/scp_payload/\" 2>/dev/null || true\n",
            encoding="utf-8",
        )
        scp.chmod(scp.stat().st_mode | stat.S_IXUSR)
        return fake_bin

    def _internal_script(self, command: str, runtime: Path, deployment_id: str, payload: Path | None = None) -> str:
        args = [deployment_id]
        if payload is not None:
            args.append(str(payload))
        return self._internal_script_args(command, runtime, *args)

    def _internal_script_args(self, command: str, runtime: Path, *args: object) -> str:
        result = self._run(
            *(str(arg) for arg in args),
            env={
                "DEPLOY_TRADING_INTERNAL_COMMAND": command,
                "DEPLOY_REMOTE_DIR": str(runtime),
            },
        )
        return result.stdout

    def _run_shell_script(self, script: str, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
        merged_env = os.environ.copy()
        if env:
            merged_env.update(env)
        return subprocess.run(["bash"], input=script, text=True, capture_output=True, env=merged_env)

    def _runtime_and_staging(self) -> tuple[Path, Path]:
        runtime = self.root / f"runtime-{len(list(self.root.glob('runtime-*')))}"
        staging = self.root / f"staging-{len(list(self.root.glob('staging-*')))}"
        runtime.mkdir()
        staging.mkdir()
        self._write_runtime_payload(runtime, "old")
        self._write_runtime_payload(staging, "new")
        return runtime, staging

    def _runtime_and_backup(self) -> tuple[Path, Path]:
        runtime = self.root / f"runtime-r-{len(list(self.root.glob('runtime-r-*')))}"
        backup = self.root / f"backup-{len(list(self.root.glob('backup-*')))}"
        runtime.mkdir()
        backup.mkdir()
        self._write_runtime_payload(runtime, "old")
        self._write_runtime_payload(backup, "backup")
        (runtime / "venv/bin").mkdir(parents=True)
        (runtime / "venv/bin/activate").write_text(
            f"export PATH={runtime / 'venv/bin'}:$PATH\n",
            encoding="utf-8",
        )
        (runtime / "venv/bin/python").symlink_to(Path(sys.executable))
        (runtime / "DEPLOYED_VERSION.json").write_text("{}\n", encoding="utf-8")
        return runtime, backup

    def _write_runtime_payload(self, root: Path, marker: str) -> None:
        for filename in (
            "alpaca_trader.py",
            "broker_protection.py",
            "execution_engine.py",
            "exit_plan.py",
            "fleet_runner.py",
            "mission_control_telemetry.py",
            "monitoring_app.py",
            "renko_builder.py",
            "reversal_protection.py",
            "strategy_engine.py",
            "trading_events.py",
        ):
            path = root / filename
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(f"# {filename} {marker}\n", encoding="utf-8")
        (root / "requirements.txt").write_text(f"# requirements {marker}\n", encoding="utf-8")
        self._write_package(root / "shared", f"{marker}-shared")
        self._write_package(root / "silver", f"{marker}-silver")
        (root / "shared/config").mkdir(parents=True, exist_ok=True)
        (root / "shared/config/__init__.py").write_text("", encoding="utf-8")
        (root / "shared/config/trader_config.py").write_text(
            "def build_strategy_config(trader):\n    return trader\n",
            encoding="utf-8",
        )
        (root / "silver/market").mkdir(parents=True, exist_ok=True)
        (root / "silver/market/__init__.py").write_text("", encoding="utf-8")
        (root / "silver/market/one_second_sampler.py").write_text("", encoding="utf-8")
        (root / "silver/evidence").mkdir(parents=True, exist_ok=True)
        (root / "silver/evidence/__init__.py").write_text("", encoding="utf-8")
        (root / "silver/evidence/logger.py").write_text("", encoding="utf-8")
        (root / "silver/evidence/shadow_sampler.py").write_text("", encoding="utf-8")

    def _write_package(self, path: Path, marker: str) -> None:
        path.mkdir(parents=True, exist_ok=True)
        (path / "__init__.py").write_text("", encoding="utf-8")
        (path / "marker.txt").write_text(marker, encoding="utf-8")

    def _marker(self, path: Path) -> str:
        return (path / "marker.txt").read_text(encoding="utf-8")

    def _sha256(self, path: Path) -> str:
        result = subprocess.run(["shasum", "-a", "256", str(path)], text=True, capture_output=True, check=True)
        return result.stdout.split()[0]

    def _manifest_dict(self, deployment_id: str, commit: str) -> dict[str, object]:
        return {
            "backup_id": deployment_id,
            "backup_path": f"/root/deploy-backups/{deployment_id}",
            "config_hash": None,
            "config_policy": "preserve_remote",
            "deployed_at": "2026-08-03T00:00:00+00:00",
            "deployment_id": deployment_id,
            "fleet_restart_status": "not_requested",
            "git_commit": commit,
            "mission_control_restart_status": "not_requested",
            "payload_groups": {
                "config_deployed": False,
                "package_dirs": ["shared", "silver"],
                "runtime_files": ["fleet_runner.py"],
                "service_files": [],
                "service_files_deployed": False,
            },
            "requirements_hash": "abc123",
            "source_repository": "test-repo",
            "validation_status": "generated",
        }


if __name__ == "__main__":
    unittest.main()
