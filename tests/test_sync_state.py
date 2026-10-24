import json
import unittest
from datetime import datetime, timezone
from unittest import mock

import requests

import etl
from etl import DiscoveryError, HOTApiClient, StateManager, finish_report


def make_response(status_code, payload=None):
    response = requests.Response()
    response.status_code = status_code
    response._content = json.dumps(payload or {}).encode("utf-8")
    response.url = "https://example.test/"
    return response


def page(results, pages):
    return make_response(200, {"results": results, "pagination": {"pages": pages}})


class FakeS3Client:
    def __init__(self, objects=None):
        self.objects = dict(objects or {})
        self.deleted = []

    def get_object(self, key):
        return self.objects.get(key)

    def put_object(self, key, body, content_type, **kwargs):
        self.objects[key] = body

    def delete_objects(self, keys):
        self.deleted.extend(keys)
        for key in keys:
            self.objects.pop(key, None)
        return len(keys)

    def list_objects(self, prefix):
        return [key for key in self.objects if key.startswith(prefix)]


class FakeApiClient:
    """Serves a fixed project list; IDs in `broken` answer 500 on detail fetch."""

    def __init__(self, projects, broken=()):
        self.projects = projects
        self.broken = set(broken)

    def get_projects_summary(self, last_updated_from=None):
        return [
            {"projectId": pid, "lastUpdated": details["lastUpdated"]}
            for pid, details in self.projects.items()
        ]

    def get_project_details(self, project_id):
        if project_id in self.broken:
            raise requests.HTTPError(response=make_response(500))
        return self.projects[project_id]


def project(pid, last_updated="2026-10-01T00:00:00.000Z"):
    return {
        "projectId": pid,
        "lastUpdated": last_updated,
        "projectInfo": {"name": f"Project {pid}"},
        "areaOfInterest": {
            "type": "MultiPolygon",
            "coordinates": [[[[0, 0], [0, 1], [1, 1], [1, 0], [0, 0]]]],
        },
    }


def fake_pmtiles(geojson_path, output_path):
    output_path.write_bytes(b"pmtiles")
    return True


def run_etl_with(s3_client, api_client):
    with mock.patch.object(etl, "S3Client", return_value=s3_client), mock.patch.object(
        etl, "HOTApiClient", return_value=api_client
    ), mock.patch.object(etl, "generate_pmtiles", side_effect=fake_pmtiles):
        return etl.run_etl()


def load_state(s3_client):
    return json.loads(s3_client.objects[etl.STATE_FILE_KEY])


class FailedProjectStateTests(unittest.TestCase):
    def test_failures_round_trip_and_count_attempts(self):
        s3_client = FakeS3Client()
        state_manager = StateManager(s3_client)
        state_manager.record_failure(214, "2026-10-01T00:00:00.000Z", "HTTP 500", "2026-10-22T00:00:00Z")
        state_manager.record_failure(214, "2026-10-01T00:00:00.000Z", "HTTP 500", "2026-10-23T00:00:00Z")
        state_manager.save()

        reloaded = StateManager(s3_client)
        reloaded.load()

        failure = reloaded.failed_projects["214"]
        self.assertEqual(failure["attempts"], 2)
        self.assertEqual(failure["firstFailedAt"], "2026-10-22T00:00:00Z")
        self.assertEqual(failure["lastFailedAt"], "2026-10-23T00:00:00Z")
        self.assertEqual(reloaded.get_failed_project_ids(), {214})

    def test_clear_and_remove_drop_failures(self):
        state_manager = StateManager(FakeS3Client())
        state_manager.record_failure(1, "t", "HTTP 500", "now")
        state_manager.record_failure(2, "t", "HTTP 500", "now")

        state_manager.clear_failure(1)
        state_manager.remove_projects({2})

        self.assertEqual(state_manager.failed_projects, {})


class PaginationTests(unittest.TestCase):
    def setUp(self):
        self.client = HOTApiClient()

    def test_non_400_error_raises_instead_of_truncating(self):
        with mock.patch.object(
            self.client.session, "get", side_effect=[page([{"projectId": 1}], 3), make_response(503)]
        ):
            with self.assertRaises(DiscoveryError):
                self.client._paginate_projects()

    def test_400_pages_are_skipped_and_counted(self):
        with mock.patch.object(
            self.client.session,
            "get",
            side_effect=[page([{"projectId": 1}], 3), make_response(400), page([{"projectId": 3}], 3)],
        ):
            projects, skipped = self.client._paginate_projects()

        self.assertEqual([p["projectId"] for p in projects], [1, 3])
        self.assertEqual(skipped, 1)

    def test_endless_400s_stop(self):
        with mock.patch.object(self.client.session, "get", return_value=make_response(400)):
            with self.assertRaises(DiscoveryError):
                self.client._paginate_projects()

    def test_full_discovery_only_requeries_years_with_skipped_pages(self):
        calls = []

        def fake_paginate(last_updated_from=None, last_updated_to=None, order_by="last_updated"):
            calls.append((last_updated_from[:4], order_by))
            if last_updated_from.startswith("2020") and order_by == "last_updated":
                return [{"projectId": 1}], 1
            if last_updated_from.startswith("2020"):
                return [{"projectId": 1}, {"projectId": 2}], 0
            return [], 0

        with mock.patch.object(self.client, "_paginate_projects", side_effect=fake_paginate):
            projects = self.client.get_projects_summary()

        self.assertEqual(sorted(p["projectId"] for p in projects), [1, 2])
        self.assertEqual([c for c in calls if c[1] == "id"], [("2020", "id")])


class RunEtlTests(unittest.TestCase):
    def test_broken_project_does_not_block_discovery_or_aggregate(self):
        s3_client = FakeS3Client()
        api_client = FakeApiClient({1: project(1), 214: project(214)}, broken={214})

        report = run_etl_with(s3_client, api_client)

        state = load_state(s3_client)
        self.assertIn("lastFullDiscoveryAt", state)
        self.assertFalse(state["aggregateDirty"])
        self.assertEqual(list(state["failedProjects"]), ["214"])
        self.assertEqual(report["features"], 1)
        self.assertEqual(report["problems"], [])

        # Next run is incremental: the failed project is retried and recovers.
        api_client.broken.clear()
        report = run_etl_with(s3_client, api_client)

        state = load_state(s3_client)
        self.assertEqual(report["discovery"], "incremental")
        self.assertNotIn("failedProjects", state)
        self.assertIn("214", state["projects"])
        self.assertEqual(report["features"], 2)
        self.assertFalse(state["aggregateDirty"])

    def test_mass_removal_is_refused(self):
        known = {str(pid): "2026-10-01T00:00:00.000Z" for pid in range(1, 302)}
        s3_client = FakeS3Client(
            {
                etl.STATE_FILE_KEY: json.dumps(
                    {"version": 3, "projects": known, "aggregateDirty": False}
                ).encode("utf-8"),
                **{f"api/v2/projects/{pid}": json.dumps(project(int(pid))).encode("utf-8") for pid in known},
            }
        )
        api_client = FakeApiClient({1: project(1)})

        report = run_etl_with(s3_client, api_client)

        state = load_state(s3_client)
        self.assertEqual(s3_client.deleted, [])
        self.assertEqual(len(state["projects"]), 301)
        self.assertNotIn("lastFullDiscoveryAt", state)
        self.assertTrue(any("would remove 300 projects" in p for p in report["problems"]))


class FinishReportTests(unittest.TestCase):
    NOW = datetime(2026, 10, 23, tzinfo=timezone.utc)

    def test_fresh_state_has_no_problems(self):
        state_manager = StateManager(FakeS3Client())
        state_manager.last_full_discovery_at = "2026-10-20T00:00:00Z"
        state_manager.mark_aggregate_clean("2026-10-23T00:00:00Z")

        self.assertEqual(finish_report({}, state_manager, self.NOW)["problems"], [])

    def test_stale_aggregate_and_overdue_discovery_are_flagged(self):
        state_manager = StateManager(FakeS3Client())
        state_manager.last_full_discovery_at = "2026-08-27T17:31:25Z"
        state_manager.last_aggregate_build_at = "2026-09-10T11:01:15Z"
        state_manager.mark_aggregate_dirty()

        problems = finish_report({}, state_manager, self.NOW)["problems"]

        self.assertEqual(len(problems), 2)


if __name__ == "__main__":
    unittest.main()
