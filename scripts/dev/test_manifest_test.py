import importlib.util
import pathlib
import unittest
import sys

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


if __name__ == "__main__":
    unittest.main()
