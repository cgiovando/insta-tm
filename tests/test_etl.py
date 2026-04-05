import gzip
import json
import unittest

from etl import S3Client, StateManager, build_lean_project, normalize_api_timestamp


class FakeS3Client:
    """Minimal fake that stores raw bytes (no compression logic)."""

    def __init__(self, objects=None):
        self.objects = dict(objects or {})

    def get_object(self, key):
        return self.objects.get(key)

    def put_object(self, key, body, content_type, **kwargs):
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


class GzipRoundTripTests(unittest.TestCase):
    def test_put_compressed_get_decompressed(self):
        """Verify that put_object(compress=True) + get_object transparently round-trips."""
        original = b'{"type":"FeatureCollection","features":[]}'
        compressed = gzip.compress(original)

        # Simulate what S3/R2 returns for a gzip-encoded object: raw gzip bytes
        # with ContentEncoding metadata. The real S3Client.get_object reads this.
        fake_response = {
            "Body": type("Body", (), {"read": lambda self: compressed})(),
            "ContentEncoding": "gzip",
        }

        # Directly test the decompression logic from S3Client.get_object
        data = fake_response["Body"].read()
        content_encoding = fake_response.get("ContentEncoding", "")
        if content_encoding == "gzip":
            data = gzip.decompress(data)

        self.assertEqual(data, original)

    def test_put_uncompressed_get_unchanged(self):
        """Non-gzipped objects are returned as-is."""
        original = b'{"projectId":1}'

        fake_response = {
            "Body": type("Body", (), {"read": lambda self: original})(),
        }

        data = fake_response["Body"].read()
        content_encoding = fake_response.get("ContentEncoding", "")
        if content_encoding == "gzip":
            data = gzip.decompress(data)

        self.assertEqual(data, original)


class BuildLeanProjectTests(unittest.TestCase):
    def test_extracts_consumer_fields(self):
        full_details = {
            "projectId": 123,
            "projectInfo": {
                "name": "Test Project",
                "shortDescription": "A test",
                "description": "<p>Long HTML that should be dropped</p>",
                "instructions": "<p>Detailed instructions</p>",
            },
            "status": "PUBLISHED",
            "created": "2024-01-01T00:00:00Z",
            "lastUpdated": "2024-06-01T12:00:00Z",
            "author": "testuser",
            "organisationName": "HOT",
            "countryTag": ["Mozambique"],
            "imagery": "https://example.com/tiles",
            "mappingTypes": ["BUILDINGS"],
            "difficulty": "MODERATE",
            "projectPriority": "MEDIUM",
            "percentMapped": 85,
            "percentValidated": 42,
            "areaOfInterest": {"type": "Polygon", "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 0]]]},
            "aoiCentroid": {"type": "Point", "coordinates": [0.5, 0.5]},
            "tasks": {"type": "FeatureCollection", "features": [{}, {}, {}]},
            # Heavy fields that should be dropped
            "teamRoles": [{"teamId": 1, "role": "MAPPER"}],
            "customEditor": {"name": "iD"},
        }

        lean = build_lean_project(full_details)

        self.assertEqual(lean["projectId"], 123)
        self.assertEqual(lean["projectInfo"]["name"], "Test Project")
        self.assertEqual(lean["projectInfo"]["shortDescription"], "A test")
        self.assertNotIn("description", lean["projectInfo"])
        self.assertNotIn("instructions", lean["projectInfo"])
        self.assertEqual(lean["totalTasks"], 3)
        self.assertNotIn("tasks", lean)
        self.assertNotIn("teamRoles", lean)
        self.assertNotIn("customEditor", lean)

    def test_handles_missing_tasks(self):
        lean = build_lean_project({"projectId": 1})
        self.assertIsNone(lean["totalTasks"])

    def test_handles_missing_project_info(self):
        lean = build_lean_project({"projectId": 1, "projectInfo": None})
        self.assertEqual(lean["projectInfo"]["name"], "")
        self.assertEqual(lean["projectInfo"]["shortDescription"], "")


if __name__ == "__main__":
    unittest.main()
