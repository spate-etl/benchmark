import importlib.util
from pathlib import Path
import tomllib
import unittest

spec = importlib.util.spec_from_file_location("review", Path(__file__).with_name("flink-review.py"))
review = importlib.util.module_from_spec(spec)
spec.loader.exec_module(review)

spec = importlib.util.spec_from_file_location("confirm", Path(__file__).with_name("flink-confirm.py"))
confirm = importlib.util.module_from_spec(spec)
spec.loader.exec_module(confirm)


class ReviewTests(unittest.TestCase):
    def setUp(self):
        self.original = review.DESCRIPTOR.read_text()
        self.defaults = tomllib.loads(self.original)["variants"][0]["knobs"]

    def test_every_case_is_schedulable_and_buffers_exceed_batch_cap(self):
        # The cells the next session will actually run, not a historical
        # matrix: a case that cannot schedule costs a drain to discover, and
        # `buffered_rows <= max_rows` is a job that refuses to start rather
        # than a slow one.
        cases = ([("candidate", confirm.candidate_knobs(self.defaults)),
                  ("baseline", confirm.baseline_knobs(self.defaults))]
                 + confirm.screen_cases(self.defaults))
        doc = tomllib.loads(review.descriptor_text(self.original, cases))
        containers = [c for c in doc["envelope"]["container"] if c["role"] == "data-plane"]
        self.assertEqual(sum(int(c["cpus"]) for c in containers), 32)
        self.assertEqual(sum(int(c["memory"][:-1]) for c in containers), 96)
        self.assertEqual(len(containers), 1)
        for variant in doc["variants"]:
            knobs = variant["knobs"]
            self.assertGreater(knobs["buffered_rows"], knobs["max_rows"])
            self.assertGreaterEqual(knobs["slots"], knobs["parallelism"])
            # One TaskManager is the contract, so consumer width is sized to the
            # 32-partition topic; reduced widths are deferred under issue #78.
            self.assertEqual(knobs["parallelism"], 32)
        tuned = tomllib.loads(review.descriptor_text(self.original, [("zgc", self.defaults)], tuned=True))
        self.assertEqual([v["id"] for v in tuned["variants"] if v["default"]], ["rowbinary-nt"])
        self.assertEqual(tuned["variants"][0]["approach"], "tuned")

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

    def test_followup_screens_at_contract_width_and_refuses_a_short_budget(self):
        from unittest.mock import patch
        for name, knobs in confirm.screen_cases(self.defaults):
            self.assertEqual((knobs["parallelism"], knobs["slots"]), (32, 32), name)
            self.assertGreater(knobs["buffered_rows"], knobs["max_rows"], name)
        probes = dict(confirm.screen_cases(self.defaults))
        self.assertIn("-XX:+ZGenerational", probes["java21-zgc"]["jvm_opts"])
        self.assertEqual(probes["java21-zgc"]["runtime_image"], "flink:2.2.1-java21")
        self.assertEqual(probes[confirm.REFERENCE]["runtime_image"], "flink:2.2.1-java17")
        # Refusal happens before builds, descriptor writes or a benchmark call.
        with patch.dict("os.environ", {"FLINK_REVIEW_REMAINING_SECONDS": "14400"}):
            with self.assertRaisesRegex(RuntimeError, "undersized"):
                confirm.execute(None)

    def test_confirmation_is_reserved_before_the_screen_is_offered_anything(self):
        self.assertGreaterEqual(
            confirm.REQUIRED_SECONDS,
            confirm.SETUP_S + confirm.SCREEN_S + confirm.RECOVERY_S
            + confirm.CONFIRM_S + confirm.CONTROLS_S,
            "the declared budget must hold every phase cap it promises",
        )

    def test_a_screening_cell_is_selected_on_margin_rather_than_on_being_largest(self):
        def measured(name, value, flags=()):
            return dict(kind="measurement", status="ok", sut={"variant_id": name},
                        flags=list(flags), metrics={"rows_per_s_per_core": {"value": value}})
        aa = [dict(kind="verdict", metrics={"aa_spread": {"value": 0.01}})]
        reference = measured(confirm.REFERENCE, 100)

        # Largest, but inside the floor: one repetition cannot separate it.
        noise = confirm.select_candidate([reference, measured("network256m", 104)] + aa, self.defaults)
        self.assertEqual(noise["name"], confirm.REFERENCE)
        self.assertEqual(noise["image"], None)

        # Clear of both the floor and the sweep's own A/A spread.
        won = confirm.select_candidate(
            [reference, measured("network256m", 104), measured("java21-zgc", 130)] + aa, self.defaults)
        self.assertEqual(won["name"], "java21-zgc")
        self.assertEqual(won["image"], confirm.IMAGE)
        self.assertNotIn("network256m", won["margins"])

        # A noisy sweep raises the bar rather than lowering the candidate's.
        loud = confirm.select_candidate(
            [reference, measured("java21-zgc", 130)]
            + [dict(kind="verdict", metrics={"aa_spread": {"value": 0.5}})], self.defaults)
        self.assertEqual(loud["name"], confirm.REFERENCE)

        # An A/A twin is not a repetition of the arm it shadows.
        self.assertNotIn("control", confirm.medians(
            [reference, measured("control", 900, flags=["aa_control"])]))

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
