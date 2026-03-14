import json
import unittest

from etl import StateManager, normalize_api_timestamp


class FakeS3Client:
    def __init__(self, objects=None):
        self.objects = dict(objects or {})

    def get_object(self, key):
        return self.objects.get(key)

    def put_object(self, key, body, content_type):
        self.objects[key] = body


class NormalizeApiTimestampTests(unittest.TestCase):
    def test_truncates_microseconds_to_milliseconds(self):
        self.assertEqual(
            normalize_api_timestamp("2020-02-06T03:02:50.248067Z"),
            "2020-02-06T03:02:50.248Z",
        )

    def test_normalized_values_match_across_summary_and_detail_precision(self):
        summary_timestamp = "2020-02-06T03:02:50.248067Z"
        detail_timestamp = "2020-02-06T03:02:50.248000Z"

        self.assertEqual(
            normalize_api_timestamp(summary_timestamp),
            normalize_api_timestamp(detail_timestamp),
        )

    def test_normalizes_whole_second_timestamps_to_milliseconds(self):
        self.assertEqual(
            normalize_api_timestamp("2020-02-06T03:02:50Z"),
            "2020-02-06T03:02:50.000Z",
        )

    def test_preserves_unparseable_timestamps(self):
        self.assertEqual(normalize_api_timestamp("not-a-timestamp"), "not-a-timestamp")

    def test_returns_none_for_missing_values(self):
        self.assertIsNone(normalize_api_timestamp(None))


class StateManagerTests(unittest.TestCase):
    def test_load_normalizes_project_timestamps(self):
        s3_client = FakeS3Client(
            {
                "state.v3.json": json.dumps(
                    {
                        "version": 3,
                        "projects": {
                            "100": "2020-02-06T03:02:50.248067Z",
                            "101": "2020-02-06T03:02:50Z",
                        },
                        "aggregateDirty": True,
                    }
                ).encode("utf-8")
            }
        )
        state_manager = StateManager(s3_client)

        state_manager.load()

        self.assertEqual(
            state_manager.project_timestamps["100"],
            "2020-02-06T03:02:50.248Z",
        )
        self.assertEqual(
            state_manager.project_timestamps["101"],
            "2020-02-06T03:02:50.000Z",
        )
        self.assertTrue(state_manager.aggregate_dirty)

    def test_needs_update_ignores_precision_differences(self):
        state_manager = StateManager(FakeS3Client())
        state_manager.project_timestamps["100"] = "2020-02-06T03:02:50.248Z"

        self.assertFalse(
            state_manager.needs_update(100, "2020-02-06T03:02:50.248067Z")
        )

    def test_mark_updated_stores_normalized_timestamp(self):
        state_manager = StateManager(FakeS3Client())

        state_manager.mark_updated(100, "2020-02-06T03:02:50.248067Z")

        self.assertEqual(
            state_manager.project_timestamps["100"],
            "2020-02-06T03:02:50.248Z",
        )

    def test_remove_projects_returns_removed_count(self):
        state_manager = StateManager(FakeS3Client())
        state_manager.project_timestamps = {
            "100": "2020-02-06T03:02:50.248Z",
            "101": "2020-02-06T03:02:50.249Z",
        }

        removed = state_manager.remove_projects({101, 999})

        self.assertEqual(removed, 1)
        self.assertEqual(
            state_manager.project_timestamps,
            {"100": "2020-02-06T03:02:50.248Z"},
        )


if __name__ == "__main__":
    unittest.main()
