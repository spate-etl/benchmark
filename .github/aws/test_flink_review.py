import importlib.util
from pathlib import Path
import tomllib
import unittest

spec = importlib.util.spec_from_file_location("review", Path(__file__).with_name("flink-review.py"))
review = importlib.util.module_from_spec(spec)
spec.loader.exec_module(review)


class ReviewTests(unittest.TestCase):
    def setUp(self):
        self.original = review.DESCRIPTOR.read_text()
        self.defaults = tomllib.loads(self.original)["variants"][0]["knobs"]

    def test_every_case_is_schedulable_and_buffers_exceed_batch_cap(self):
        for count in (1, 4):
            cases = review.initial_cases(self.defaults)
            if count == 4:
                cases = [(name, dict(knobs, slots=8, process_mib=21504)) for name, knobs in cases]
            doc = tomllib.loads(review.descriptor_text(self.original, cases, count))
            containers = [c for c in doc["envelope"]["container"] if c["role"] == "data-plane"]
            self.assertEqual(sum(int(c["cpus"]) for c in containers), 32)
            self.assertEqual(sum(int(c["memory"][:-1]) for c in containers), 96)
            self.assertEqual(len(containers), count)
            for variant in doc["variants"]:
                knobs = variant["knobs"]
                self.assertGreater(knobs["buffered_rows"], knobs["max_rows"])
                self.assertGreaterEqual(count * knobs["slots"], knobs["parallelism"])
                self.assertEqual(knobs["taskmanagers"], count)

    def test_matching_spate_control_does_not_silently_use_flink_defaults(self):
        cases = dict(review.initial_cases(self.defaults))
        control = cases["spate-rowbinary-settings"]
        self.assertEqual((control["max_rows"], control["inflight"], control["linger_ms"]), (262144, 4, 500))
        self.assertGreater(control["max_batch_bytes"], self.defaults["max_batch_bytes"])

    def test_confirmation_requires_bilateral_noise_throughput_and_correctness(self):
        def measurements(name, values, throughput=1000, duplicates=0):
            return [dict(kind="measurement", status="ok", sut={"variant_id": name}, flags=[],
                         metrics={key: {"value": value} for key, value in dict(
                             rows_per_s_per_core=v, rows_per_s=throughput, duplicate_rows=duplicates,
                             throttled_us=1, ch_cpu_us_per_row=2).items()}) for v in values]
        def controls(name, spread=0.01):
            return [dict(kind="verdict", sut={"variant_id": name},
                         metrics={"aa_spread": {"value": spread}}) for _ in range(3)]
        baseline = measurements("baseline", [100, 101, 100])
        candidate = measurements("candidate", [150, 151, 150])
        both = controls("baseline") + controls("candidate")
        self.assertFalse(review.confirmation_summary(baseline + candidate + controls("candidate"))["accepted"])
        self.assertFalse(review.confirmation_summary(baseline + candidate + controls("candidate") + controls("baseline", .18))["accepted"])
        self.assertFalse(review.confirmation_summary(baseline + measurements("candidate", [150, 151, 150], throughput=800) + both)["accepted"])
        self.assertFalse(review.confirmation_summary(baseline + measurements("candidate", [150, 151, 150], duplicates=1) + both)["accepted"])
        self.assertFalse(review.confirmation_summary(baseline + candidate[:2] + both)["accepted"])
        self.assertTrue(review.confirmation_summary(baseline + candidate + both)["accepted"])

    def test_followup_is_predeclared_width32_zgc_and_refuses_short_budget(self):
        from unittest.mock import patch
        spec = importlib.util.spec_from_file_location("confirmation", Path(__file__).with_name("flink-confirm.py"))
        confirmation = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(confirmation)
        knobs = confirmation.candidate_knobs(self.defaults)
        self.assertEqual((knobs["parallelism"], knobs["slots"]), (32, 32))
        self.assertEqual(knobs["runtime_image"], "flink:2.2.1-java21")
        self.assertIn("-XX:+ZGenerational", knobs["jvm_opts"])
        # Refusal happens before builds, descriptor writes or a benchmark call.
        with patch.dict("os.environ", {"FLINK_REVIEW_REMAINING_SECONDS": "14400"}):
            with self.assertRaisesRegex(RuntimeError, "undersized"):
                confirmation.execute(None)

    def test_requested_gc_worker_flags_need_matching_runtime_logs(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            cases = [("workers", dict(jvm_opts="-XX:ParallelGCThreads=8 -XX:ConcGCThreads=2"))]
            self.assertFalse(review.verify_gc_flags(directory, cases))
            log = directory / "123-spate-bench-sut-tm-gc.log"
            log.write_text("Parallel Workers: 23\nConcurrent Workers: 6\n")
            self.assertFalse(review.verify_gc_flags(directory, cases))
            log.write_text("Parallel Workers: 8\nConcurrent Workers: 2\n")
            self.assertTrue(review.verify_gc_flags(directory, cases))

    def test_recording_is_refreshed_after_empty_destination_and_preserved_on_failure(self):
        import subprocess
        import tempfile
        from unittest.mock import patch
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "early.jfr"
            contents = iter([b"", b"completed recording"])
            def copy(args, **kwargs):
                Path(args[-1]).write_bytes(next(contents))
                return subprocess.CompletedProcess(args, 0)
            with patch.object(review.subprocess, "run", side_effect=copy):
                review.copy_recording("tm", "early.jfr", target)
                self.assertFalse(target.exists())
                review.copy_recording("tm", "early.jfr", target)
                self.assertEqual(target.read_bytes(), b"completed recording")
            with patch.object(review.subprocess, "run", return_value=subprocess.CompletedProcess([], 1)):
                review.copy_recording("tm", "early.jfr", target)
                self.assertEqual(target.read_bytes(), b"completed recording")


if __name__ == "__main__":
    unittest.main()
