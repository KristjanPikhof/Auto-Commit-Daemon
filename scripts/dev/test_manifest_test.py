import importlib.util
import pathlib
import unittest
import sys
import json
import os
import subprocess
import tempfile

sys.dont_write_bytecode = True

spec = importlib.util.spec_from_file_location("manifest", pathlib.Path(__file__).with_name("test-manifest.py"))
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class ManifestTests(unittest.TestCase):
    def test_long_tests_spread_and_all_entrypoints_survive(self):
        names = ["TestSlow", "TestOtherSlow", "Example", "FuzzParse", "TestNew"]
        result = module.manifest(names, 2, {"TestSlow": 100, "TestOtherSlow": 90})
        self.assertEqual(result, module.manifest(list(reversed(names)), 2, {"TestSlow": 100, "TestOtherSlow": 90}))
        self.assertEqual(result["test_count"], 5)
        self.assertEqual(result["unmeasured_count"], 3)
        self.assertEqual(sorted(n for s in result["shards"] for n in s["tests"]), sorted(names))
        self.assertNotEqual(next(i for i, s in enumerate(result["shards"]) if "TestSlow" in s["tests"]), next(i for i, s in enumerate(result["shards"]) if "TestOtherSlow" in s["tests"]))

    def test_duplicates_fail_and_empty_shards_stay_addressable(self):
        with self.assertRaises(ValueError):
            module.manifest(["TestA", "TestA"], 2, {})
        self.assertEqual(len(module.manifest(["TestA"], 4, {})["shards"]), 4)

    def test_parallel_cost_keeps_serial_work_from_overloading_a_shard(self):
        durations = {"TestParallelA": 100, "TestParallelB": 100,
                     "TestSerialA": 60, "TestSerialB": 40}
        names = list(durations)
        parallel = {"TestParallelA", "TestParallelB"}
        naive = module.manifest(names, 2, durations)
        effective = lambda shard: sum(durations[name] / (2 if name in parallel else 1)
                                      for name in shard["tests"])
        self.assertEqual(max(map(effective, naive["shards"])), 110)
        balanced = module.manifest(names, 2, durations, parallel, 2)
        self.assertEqual([shard["estimated_seconds"] for shard in balanced["shards"]], [100, 100])
        self.assertEqual(sorted(name for shard in balanced["shards"] for name in shard["tests"]), sorted(names))
        self.assertEqual(balanced, module.manifest(list(reversed(names)), 2, durations, parallel, 2))
        self.assertEqual(naive, module.manifest(names, 2, durations, parallel))
        with self.assertRaises(ValueError):
            module.manifest(names, 2, durations, parallel, 0)

    def test_timings_keep_slowest_repetition_without_counting_subtests(self):
        with tempfile.TemporaryDirectory() as root:
            source = pathlib.Path(root) / "events.jsonl"
            source.write_text("\n".join(json.dumps(event) for event in [
                {"Action": "pass", "Package": "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git", "Test": "FuzzParse", "Elapsed": 3},
                {"Action": "pass", "Package": "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git", "Test": "FuzzParse", "Elapsed": 1},
                {"Action": "pass", "Package": "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git", "Test": "FuzzParse/seed", "Elapsed": 8},
            ]))
            self.assertEqual(module.timings([source]), {"./internal/git": {"FuzzParse": 3}})

    def test_timings_classify_only_completed_top_level_parallel_tests(self):
        with tempfile.TemporaryDirectory() as root:
            source = pathlib.Path(root) / "events.jsonl"
            package = "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git"
            events = [
                {"Action": "pause", "Package": package, "Test": "TestParallel"},
                {"Action": "pause", "Package": package, "Test": "TestParked"},
                {"Action": "pause", "Package": package, "Test": "TestSerial/child"},
                {"Action": "pass", "Package": package, "Test": "TestParallel", "Elapsed": 20},
                {"Action": "pass", "Package": package, "Test": "TestSerial", "Elapsed": 10},
            ]
            source.write_text("\n".join(map(json.dumps, events)))
            self.assertEqual(module.timings([source]), {
                "./internal/git": {"TestParallel": 20, "TestSerial": 10},
                "_parallel_tests": {"./internal/git": ["TestParallel"]},
            })

    def test_balance_cli_reads_parallel_metadata_at_actual_concurrency(self):
        with tempfile.TemporaryDirectory() as root:
            names = pathlib.Path(root) / "names"
            names.write_text("TestParallel\nTestSerial\n")
            durations = pathlib.Path(root) / "timings.json"
            durations.write_text(json.dumps({
                "./example": {"TestParallel": 80, "TestSerial": 20},
                "_parallel_tests": {"./example": ["TestParallel"]},
            }))
            command = [sys.executable, str(pathlib.Path(module.__file__)), "balance", "./example", "2",
                       str(names), "--timings", str(durations), "--parallelism", "4"]
            result = subprocess.run(command, capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual([shard["estimated_seconds"] for shard in json.loads(result.stdout)["shards"]], [20, 20])

    def test_discovery_failure_propagates_and_empty_shard_selects_nothing(self):
        checkout = pathlib.Path(__file__).resolve().parents[2]
        with tempfile.TemporaryDirectory() as root:
            go = pathlib.Path(root) / "go"
            go.write_text('#!/bin/sh\nexit 42\n')
            go.chmod(0o755)
            env = dict(os.environ, PATH=root + os.pathsep + os.environ["PATH"])
            command = ["bash", "scripts/dev/test-package-shards.sh", "./example", "4"]
            failure = subprocess.run(command, cwd=checkout, env=env, capture_output=True)
            self.assertEqual(failure.returncode, 42)
            go.write_text('''#!/bin/sh
case "$*" in
  *-list*) printf 'TestOnly\\nExample\\nFuzzParse\\n';;
  *) printf '%s\\n' "$*" > "$FAKE_GO_ARGS";;
esac
''')
            env.update(ACD_TEST_SHARD_INDEX="3", FAKE_GO_ARGS=root + "/args")
            success = subprocess.run(command, cwd=checkout, env=env, capture_output=True)
            self.assertEqual(success.returncode, 0, success.stderr)
            self.assertIn("-run ^$", pathlib.Path(env["FAKE_GO_ARGS"]).read_text())

    def test_shard_runner_passes_both_go_parallel_flag_forms(self):
        checkout = pathlib.Path(__file__).resolve().parents[2]
        with tempfile.TemporaryDirectory() as root:
            go = pathlib.Path(root) / "go"
            go.write_text('#!/bin/sh\ncase "$*" in *-list*) echo TestOnly;; esac\n')
            go.chmod(0o755)
            python = pathlib.Path(root) / "python3"
            python.write_text('#!/bin/sh\nif [ "$2" = balance ]; then printf "%s\\n" "$*" > "$FAKE_MANIFEST_ARGS"; fi\nexec "$REAL_PYTHON" "$@"\n')
            python.chmod(0o755)
            env = dict(os.environ, PATH=root + os.pathsep + os.environ["PATH"],
                       FAKE_MANIFEST_ARGS=root + "/args", REAL_PYTHON=sys.executable)
            env.pop("ACD_TEST_SHARD_INDEX", None)
            env.pop("ACD_TEST_RESULTS_DIR", None)
            for flags, expected in [([], 1), (["-parallel", "2"], 2), (["-parallel=4"], 4),
                                    (["-parallel", "2", "-parallel=4"], 4)]:
                with self.subTest(flags=flags):
                    result = subprocess.run(["bash", "scripts/dev/test-package-shards.sh", "./example", "1"] + flags,
                                            cwd=checkout, env=env, capture_output=True, text=True)
                    self.assertEqual(result.returncode, 0, result.stderr)
                    self.assertIn("--parallelism " + str(expected), pathlib.Path(env["FAKE_MANIFEST_ARGS"]).read_text())
            for flags in [["-parallel"], ["-parallel", "0"], ["-parallel=invalid"]]:
                with self.subTest(flags=flags):
                    result = subprocess.run(["bash", "scripts/dev/test-package-shards.sh", "./example", "1"] + flags,
                                            cwd=checkout, env=env, capture_output=True, text=True)
                    self.assertEqual(result.returncode, 2)

    def test_support_failure_keeps_events_output_exit_and_wall_time(self):
        checkout = pathlib.Path(__file__).resolve().parents[2]
        with tempfile.TemporaryDirectory() as root:
            go = pathlib.Path(root) / "go"
            events = [
                {"Action": "build-output", "Output": "compiler diagnostic\n"},
                {"Action": "output", "Package": "example", "Test": "TestBroken", "Output": "useful failure details\n"},
                {"Action": "fail", "Package": "example", "Test": "TestBroken", "Elapsed": 1.5},
            ]
            fixture = pathlib.Path(root) / "fixture.jsonl"
            fixture.write_text("\n".join(json.dumps(event) for event in events) + "\n")
            go.write_text('#!/bin/sh\nif [ "$1" = list ]; then echo example; exit 0; fi\ncat "$FAKE_EVENTS"\nexit 42\n')
            go.chmod(0o755)
            results = pathlib.Path(root) / "results"
            env = dict(os.environ, PATH=root + os.pathsep + os.environ["PATH"],
                       ACD_TEST_RESULTS_DIR=str(results), FAKE_EVENTS=str(fixture))
            run = subprocess.run(["bash", "scripts/dev/test.sh", "support"], cwd=checkout, env=env, capture_output=True, text=True)
            self.assertEqual(run.returncode, 42, run.stderr)
            self.assertIn("useful failure details", run.stdout)
            self.assertIn("compiler diagnostic", run.stdout)
            self.assertEqual((results / "support.jsonl").read_bytes(), fixture.read_bytes())
            summary = json.loads((results / "support-all.summary.json").read_text())
            self.assertEqual(summary["exit_code"], 42)
            self.assertGreaterEqual(summary["wall_seconds"], 0)


if __name__ == "__main__":
    unittest.main()
