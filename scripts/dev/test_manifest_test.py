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

    def test_timings_keep_slowest_repetition_without_counting_subtests(self):
        with tempfile.TemporaryDirectory() as root:
            source = pathlib.Path(root) / "events.jsonl"
            source.write_text("\n".join(json.dumps(event) for event in [
                {"Action": "pass", "Package": "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git", "Test": "FuzzParse", "Elapsed": 3},
                {"Action": "pass", "Package": "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git", "Test": "FuzzParse", "Elapsed": 1},
                {"Action": "pass", "Package": "github.com/KristjanPikhof/Auto-Commit-Daemon/internal/git", "Test": "FuzzParse/seed", "Elapsed": 8},
            ]))
            self.assertEqual(module.timings([source]), {"./internal/git": {"FuzzParse": 3}})

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
