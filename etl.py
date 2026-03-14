#!/usr/bin/env python3
"""
HOT Tasking Manager Cloud Native Mirror ETL Script

Fetches project data from the HOT Tasking Manager API, transforms it into
cloud-native formats, and uploads to S3 (or S3-compatible storage like Source.coop).
"""

import json
import logging
import os
import re
import subprocess
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import boto3
import requests
from botocore.config import Config
from botocore.exceptions import ClientError
from pyproj import Geod
from shapely.geometry import shape

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Constants
HOT_API_BASE = "https://tasking-manager-tm4-production-api.hotosm.org/api/v2"
PROJECTS_ENDPOINT = f"{HOT_API_BASE}/projects/"
STATE_FILE_KEY = "state.v3.json"
LEGACY_STATE_FILE_KEY = "state.json"
ALL_PROJECTS_GEOJSON = "all_projects.geojson"
PMTILES_OUTPUT = "projects.pmtiles"
PROJECTS_SUMMARY = "projects_summary.json"
STATE_SCHEMA_VERSION = 3
STATE_CHECKPOINT_INTERVAL = 100
DISCOVERY_OVERLAP_DAYS = 1
FULL_DISCOVERY_INTERVAL_DAYS = 7

# Geodesic calculator for area computation
GEOD = Geod(ellps="WGS84")

# Imagery normalization patterns
IMAGERY_PATTERNS = [
    (re.compile(r"bing", re.IGNORECASE), "Bing"),
    (re.compile(r"esri|arcgis|world.imagery", re.IGNORECASE), "Esri"),
    (re.compile(r"mapbox", re.IGNORECASE), "Mapbox"),
    (re.compile(r"maxar|digitalglobe|vivid|securewatch", re.IGNORECASE), "Maxar"),
    (re.compile(r"openaerialmap|oam|open.aerial", re.IGNORECASE), "Custom"),
    (re.compile(r"custom", re.IGNORECASE), "Custom"),
]


def normalize_imagery(raw: str | None) -> str:
    """Normalize raw imagery value to a standard category."""
    if not raw or raw.strip() == "":
        return "Not specified"

    raw_stripped = raw.strip()

    for pattern, category in IMAGERY_PATTERNS:
        if pattern.search(raw_stripped):
            return category

    # If it looks like a URL or TMS spec but doesn't match known patterns
    if raw_stripped.startswith(("http://", "https://", "tms[")):
        return "Other"

    return "Other"


def compute_area_sqkm(geojson_geometry: dict) -> float | None:
    """Compute geodesic area in square kilometers from a GeoJSON geometry."""
    try:
        geom = shape(geojson_geometry)
        # Geod.geometry_area_perimeter returns (area, perimeter) in sq meters
        area_sqm, _ = GEOD.geometry_area_perimeter(geom)
        return round(abs(area_sqm) / 1_000_000, 2)  # Convert to sq km
    except Exception as e:
        logger.debug(f"Area computation failed: {e}")
        return None


def compute_centroid(geojson_geometry: dict) -> tuple[float, float] | None:
    """Compute centroid [lon, lat] from a GeoJSON geometry."""
    try:
        geom = shape(geojson_geometry)
        centroid = geom.centroid
        return (round(centroid.x, 4), round(centroid.y, 4))
    except Exception as e:
        logger.debug(f"Centroid computation failed: {e}")
        return None


def parse_iso8601_timestamp(value: str | None) -> datetime | None:
    """Parse API timestamps into timezone-aware UTC datetimes."""
    if not value or not isinstance(value, str):
        return None

    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        logger.warning("Could not parse timestamp: %s", value)
        return None

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)

    return parsed.astimezone(timezone.utc)


class S3Client:
    """S3 client wrapper that supports custom endpoints for S3-compatible storage."""

    def __init__(self):
        self.bucket_name = os.environ["AWS_BUCKET_NAME"]
        self.region = os.environ.get("AWS_REGION", "us-east-1")
        endpoint_url = os.environ.get("S3_ENDPOINT_URL")

        # Configure boto3 client
        client_kwargs = {
            "service_name": "s3",
            "region_name": self.region,
            "aws_access_key_id": os.environ["AWS_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["AWS_SECRET_ACCESS_KEY"],
            "config": Config(signature_version="s3v4"),
        }

        # Use custom endpoint if provided (for Source.coop or other S3-compatible storage)
        if endpoint_url:
            client_kwargs["endpoint_url"] = endpoint_url
            logger.info(f"Using custom S3 endpoint: {endpoint_url}")
        else:
            logger.info("Using standard AWS S3")

        self.client = boto3.client(**client_kwargs)

    def get_object(self, key: str) -> bytes | None:
        """Get an object from S3, returns None if not found."""
        try:
            response = self.client.get_object(Bucket=self.bucket_name, Key=key)
            return response["Body"].read()
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code")
            if error_code in {"404", "NoSuchKey", "NotFound"}:
                return None
            raise

    def put_object(self, key: str, body: bytes, content_type: str) -> None:
        """Upload an object to S3 with specified content type."""
        self.client.put_object(
            Bucket=self.bucket_name,
            Key=key,
            Body=body,
            ContentType=content_type,
        )
        logger.debug(f"Uploaded: {key} ({content_type})")

    def list_objects(self, prefix: str) -> list[str]:
        """List all object keys with a given prefix."""
        keys = []
        paginator = self.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket_name, Prefix=prefix):
            for obj in page.get("Contents", []):
                keys.append(obj["Key"])
        return keys


class HOTApiClient:
    """Client for the HOT Tasking Manager API."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Accept": "application/json",
                "User-Agent": "HOT-TM-CloudNativeMirror/1.0",
            }
        )

    def get_projects_list(
        self, page: int = 1, last_updated_from: str | None = None
    ) -> dict[str, Any]:
        """Fetch a page of projects from the API."""
        params = {
            "orderBy": "last_updated",
            "orderByType": "DESC",
            "projectStatuses": "PUBLISHED,ARCHIVED",
            "omitMapResults": "true",
            "page": page,
        }
        if last_updated_from:
            params["lastUpdatedFrom"] = last_updated_from
        response = self.session.get(PROJECTS_ENDPOINT, params=params, timeout=60)
        response.raise_for_status()
        return response.json()

    def get_projects_summary(
        self, last_updated_from: str | None = None
    ) -> list[dict[str, Any]]:
        """Fetch project summary pages, optionally filtered by last-updated date."""
        all_projects = []
        page = 1
        mode = (
            f"incremental discovery from {last_updated_from}"
            if last_updated_from
            else "full discovery"
        )
        logger.info("Starting %s", mode)

        while True:
            logger.info(f"Fetching projects list page {page}...")
            try:
                data = self.get_projects_list(page, last_updated_from=last_updated_from)
            except requests.HTTPError as e:
                # TM API returns 400 at high page numbers — treat as end
                logger.warning(f"API returned {e.response.status_code} at page {page}, stopping pagination")
                break

            results = data.get("results", [])

            if not results:
                break

            all_projects.extend(results)
            pagination = data.get("pagination", {})

            if page >= pagination.get("pages", 1):
                break

            page += 1

        logger.info(f"Found {len(all_projects)} projects total")
        return all_projects

    def get_project_details(self, project_id: int) -> dict[str, Any]:
        """Fetch full details for a specific project."""
        url = f"{HOT_API_BASE}/projects/{project_id}/"
        response = self.session.get(url, timeout=60)
        response.raise_for_status()
        return response.json()


class StateManager:
    """Manages incremental sync state."""

    def __init__(self, s3_client: S3Client):
        self.s3_client = s3_client
        self.project_timestamps: dict[str, str] = {}
        self.aggregate_dirty = False
        self.last_aggregate_build_at: str | None = None
        self.last_full_discovery_at: str | None = None

    @staticmethod
    def _is_legacy_state(payload: Any) -> bool:
        """Legacy state files are a flat project_id -> last_updated mapping."""
        return isinstance(payload, dict) and "projects" not in payload

    def load(self) -> None:
        """Load state from S3 or reconstruct from existing files."""
        data = self.s3_client.get_object(STATE_FILE_KEY)
        source_key = STATE_FILE_KEY if data else None
        if not data:
            data = self.s3_client.get_object(LEGACY_STATE_FILE_KEY)
            source_key = LEGACY_STATE_FILE_KEY if data else None

        if data:
            payload = json.loads(data.decode("utf-8"))
            if not isinstance(payload, dict):
                raise ValueError("State file must decode to a JSON object")

            if self._is_legacy_state(payload):
                self.project_timestamps = {str(k): v for k, v in payload.items()}
                self.aggregate_dirty = False
                self.last_aggregate_build_at = None
                self.last_full_discovery_at = None
                logger.info(
                    "Loaded legacy state from %s with %s projects",
                    source_key,
                    len(self.project_timestamps),
                )
                return

            projects = payload.get("projects", {})
            if not isinstance(projects, dict):
                raise ValueError("State file is missing a valid 'projects' mapping")

            self.project_timestamps = {str(k): v for k, v in projects.items()}
            self.aggregate_dirty = bool(payload.get("aggregateDirty", False))

            last_aggregate_build_at = payload.get("lastAggregateBuildAt")
            self.last_aggregate_build_at = (
                last_aggregate_build_at
                if isinstance(last_aggregate_build_at, str)
                else None
            )
            last_full_discovery_at = payload.get("lastFullDiscoveryAt")
            self.last_full_discovery_at = (
                last_full_discovery_at
                if isinstance(last_full_discovery_at, str)
                else None
            )

            logger.info(
                "Loaded state from %s with %s projects (aggregate dirty: %s, last full discovery: %s)",
                source_key,
                len(self.project_timestamps),
                self.aggregate_dirty,
                self.last_full_discovery_at or "never",
            )
        else:
            logger.info(
                "No existing state found at %s or %s, starting fresh",
                STATE_FILE_KEY,
                LEGACY_STATE_FILE_KEY,
            )
            self.project_timestamps = {}
            self.aggregate_dirty = False
            self.last_aggregate_build_at = None
            self.last_full_discovery_at = None

    def save(self) -> None:
        """Save state to S3."""
        payload: dict[str, Any] = {
            "version": STATE_SCHEMA_VERSION,
            "projects": self.project_timestamps,
            "aggregateDirty": self.aggregate_dirty,
        }
        if self.last_aggregate_build_at:
            payload["lastAggregateBuildAt"] = self.last_aggregate_build_at
        if self.last_full_discovery_at:
            payload["lastFullDiscoveryAt"] = self.last_full_discovery_at

        self.s3_client.put_object(
            STATE_FILE_KEY,
            json.dumps(payload, indent=2, sort_keys=True).encode("utf-8"),
            "application/json",
        )
        logger.info(
            "Saved state to %s with %s projects (aggregate dirty: %s, last full discovery: %s)",
            STATE_FILE_KEY,
            len(self.project_timestamps),
            self.aggregate_dirty,
            self.last_full_discovery_at or "never",
        )

    def needs_update(self, project_id: int, last_updated: str) -> bool:
        """Check if a project needs to be fetched based on last_updated timestamp."""
        project_key = str(project_id)
        stored_timestamp = self.project_timestamps.get(project_key)

        if not stored_timestamp:
            return True

        return last_updated != stored_timestamp

    def mark_updated(self, project_id: int, last_updated: str) -> None:
        """Mark a project as updated in state."""
        self.project_timestamps[str(project_id)] = last_updated

    def get_known_project_ids(self) -> set[int]:
        """Return project IDs currently tracked in sync state."""
        project_ids = set()
        for project_id in self.project_timestamps:
            try:
                project_ids.add(int(project_id))
            except ValueError:
                continue
        return project_ids

    def get_incremental_sync_start(self) -> str | None:
        """Return the inclusive date window start for incremental discovery."""
        timestamps = [
            parsed
            for parsed in (
                parse_iso8601_timestamp(timestamp)
                for timestamp in self.project_timestamps.values()
            )
            if parsed is not None
        ]
        if not timestamps:
            return None

        latest_timestamp = max(timestamps)
        start_date = latest_timestamp.date() - timedelta(days=DISCOVERY_OVERLAP_DAYS)
        return start_date.isoformat()

    def full_discovery_due(self, now: datetime) -> bool:
        """Run a full discovery periodically to reconcile removals and drift."""
        if not self.project_timestamps or not self.last_full_discovery_at:
            return True

        last_full_discovery = parse_iso8601_timestamp(self.last_full_discovery_at)
        if last_full_discovery is None:
            return True

        return now - last_full_discovery >= timedelta(days=FULL_DISCOVERY_INTERVAL_DAYS)

    def mark_full_discovery(self, discovered_at: str) -> None:
        """Track when a full discovery scan completed successfully."""
        self.last_full_discovery_at = discovered_at

    def needs_aggregate_rebuild(self) -> bool:
        """Whether aggregate artifacts need to be rebuilt from cached project data."""
        return self.aggregate_dirty

    def mark_aggregate_dirty(self) -> None:
        """Mark aggregate artifacts as stale until a full rebuild succeeds."""
        self.aggregate_dirty = True

    def mark_aggregate_clean(self, built_at: str) -> None:
        """Mark aggregate artifacts as fresh after all uploads succeed."""
        self.aggregate_dirty = False
        self.last_aggregate_build_at = built_at


def build_feature(details: dict[str, Any]) -> dict[str, Any] | None:
    """Build a GeoJSON feature from project details with enriched properties."""
    project_id = details.get("projectId")
    aoi = details.get("areaOfInterest")
    if not aoi:
        return None

    imagery_raw = details.get("imagery")
    country_tag = details.get("countryTag", []) or []
    project_info = details.get("projectInfo", {}) or {}

    area_sqkm = compute_area_sqkm(aoi)
    centroid = compute_centroid(aoi)

    return {
        "type": "Feature",
        "geometry": aoi,
        "properties": {
            "projectId": project_id,
            "name": project_info.get("name", ""),
            "status": details.get("status"),
            "imagery": normalize_imagery(imagery_raw),
            "imageryRaw": imagery_raw or "",
            "countryTag": country_tag,
            "country": country_tag[0] if country_tag else "",
            "organisationName": details.get("organisationName", ""),
            "created": details.get("created"),
            "mappingTypes": details.get("mappingTypes", []),
            "areaSqKm": area_sqkm,
            "centroidLon": centroid[0] if centroid else None,
            "centroidLat": centroid[1] if centroid else None,
            "difficulty": details.get("difficulty"),
            "projectPriority": details.get("projectPriority"),
            "percentMapped": details.get("percentMapped"),
            "percentValidated": details.get("percentValidated"),
            "lastUpdated": details.get("lastUpdated"),
        },
    }


def build_summary_entry(feature: dict[str, Any]) -> dict[str, Any]:
    """Build a lightweight summary entry from a GeoJSON feature (no geometry)."""
    props = feature["properties"]
    centroid = None
    if props.get("centroidLon") is not None and props.get("centroidLat") is not None:
        centroid = [props["centroidLon"], props["centroidLat"]]

    return {
        "id": props["projectId"],
        "name": props.get("name", ""),
        "status": props.get("status"),
        "imagery": props.get("imagery"),
        "imageryRaw": props.get("imageryRaw", ""),
        "country": props.get("countryTag", []),
        "org": props.get("organisationName", ""),
        "created": (props.get("created") or "")[:10],  # Date only
        "mappingTypes": props.get("mappingTypes", []),
        "areaSqKm": props.get("areaSqKm"),
        "centroid": centroid,
        "pctMapped": props.get("percentMapped"),
        "pctValidated": props.get("percentValidated"),
        "difficulty": props.get("difficulty"),
        "priority": props.get("projectPriority"),
    }


def load_cached_feature_map(s3_client: S3Client) -> dict[int, dict[str, Any]]:
    """Load cached aggregate features keyed by project ID."""
    cached = s3_client.get_object(ALL_PROJECTS_GEOJSON)
    if not cached:
        logger.info("No cached aggregate GeoJSON found; full rebuild required")
        return {}

    payload = json.loads(cached.decode("utf-8"))
    features = payload.get("features")
    if payload.get("type") != "FeatureCollection" or not isinstance(features, list):
        raise ValueError("Cached aggregate GeoJSON is not a valid FeatureCollection")

    feature_map: dict[int, dict[str, Any]] = {}
    for feature in features:
        if not isinstance(feature, dict):
            continue
        properties = feature.get("properties", {})
        if not isinstance(properties, dict):
            continue

        project_id = properties.get("projectId")
        if isinstance(project_id, int):
            feature_map[project_id] = feature

    logger.info(
        "Loaded %s cached aggregate features from %s",
        len(feature_map),
        ALL_PROJECTS_GEOJSON,
    )
    return feature_map


def get_project_details_for_rebuild(
    s3_client: S3Client,
    api_client: HOTApiClient,
    project_id: int,
    updated_projects: dict[int, dict[str, Any]],
    expected_last_updated: str | None = None,
) -> dict[str, Any]:
    """Get project details for rebuild, preferring in-memory updates then S3 cache."""
    if project_id in updated_projects:
        return updated_projects[project_id]

    cached = s3_client.get_object(f"api/v2/projects/{project_id}")
    if cached:
        cached_details = json.loads(cached.decode("utf-8"))
        if (
            not expected_last_updated
            or cached_details.get("lastUpdated") == expected_last_updated
        ):
            return cached_details

    return api_client.get_project_details(project_id)


def generate_pmtiles(geojson_path: Path, output_path: Path) -> bool:
    """Generate PMTiles from GeoJSON using tippecanoe."""
    logger.info("Generating PMTiles with tippecanoe...")

    cmd = [
        "tippecanoe",
        "-o",
        str(output_path),
        "-z",
        "12",  # Max zoom
        "-Z",
        "0",  # Min zoom
        "--force",  # Overwrite existing
        "--no-feature-limit",
        "--no-tile-size-limit",
        "-l",
        "projects",  # Layer name
        str(geojson_path),
    ]

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        logger.info("PMTiles generation complete")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"tippecanoe failed: {e.stderr}")
        return False
    except FileNotFoundError:
        logger.error("tippecanoe not found. Please install it first.")
        return False


def run_etl():
    """Main ETL process."""
    logger.info("Starting HOT Tasking Manager Cloud Native Mirror ETL")
    run_started_at = datetime.now(timezone.utc)
    run_started_at_str = run_started_at.strftime("%Y-%m-%dT%H:%M:%SZ")

    # Initialize clients
    s3_client = S3Client()
    api_client = HOTApiClient()
    state_manager = StateManager(s3_client)

    # Load existing state
    state_manager.load()

    incremental_start = state_manager.get_incremental_sync_start()
    full_discovery = state_manager.full_discovery_due(run_started_at)

    if full_discovery:
        logger.info(
            "Running full discovery to reconcile the full public project corpus"
        )
        projects_summary = api_client.get_projects_summary()
        state_manager.mark_full_discovery(run_started_at_str)
    else:
        logger.info(
            "Running incremental discovery from %s with %s-day overlap",
            incremental_start,
            DISCOVERY_OVERLAP_DAYS,
        )
        projects_summary = api_client.get_projects_summary(
            last_updated_from=incremental_start
        )

    # Track which projects need updates
    projects_to_update = []
    for project in projects_summary:
        project_id = project.get("projectId")
        last_updated = project.get("lastUpdated")

        if project_id and last_updated:
            if state_manager.needs_update(project_id, last_updated):
                projects_to_update.append((project_id, last_updated))

    logger.info(f"{len(projects_to_update)} projects need updating")
    if state_manager.needs_aggregate_rebuild():
        logger.info("Aggregate artifacts are marked dirty and will be rebuilt")

    # Fetch and upload updated project details
    updated_projects = {}  # project_id -> details
    successful_uploads = 0
    failed_project_updates = 0
    uploads_since_checkpoint = 0
    for i, (project_id, last_updated) in enumerate(projects_to_update, 1):
        try:
            logger.info(
                f"Fetching project {project_id}... ({i}/{len(projects_to_update)})"
            )
            details = api_client.get_project_details(project_id)

            # Upload project JSON
            s3_key = f"api/v2/projects/{project_id}"
            s3_client.put_object(
                s3_key,
                json.dumps(details, indent=2).encode("utf-8"),
                "application/json",
            )

            state_manager.mark_updated(project_id, last_updated)
            state_manager.mark_aggregate_dirty()
            updated_projects[project_id] = details
            successful_uploads += 1
            uploads_since_checkpoint += 1
            logger.info(f"Uploaded project {project_id}")

            # Checkpoint early, then every N uploads, so retries resume near the failure.
            if successful_uploads == 1 or uploads_since_checkpoint >= STATE_CHECKPOINT_INTERVAL:
                state_manager.save()
                uploads_since_checkpoint = 0
                logger.info(
                    "Checkpointed state after %s project uploads", successful_uploads
                )

        except requests.RequestException as e:
            logger.error(f"Failed to fetch project {project_id}: {e}")
            failed_project_updates += 1
            continue

    if uploads_since_checkpoint:
        state_manager.save()
        logger.info(
            "Checkpointed state after %s project uploads", successful_uploads
        )

    # Build master GeoJSON from all projects
    logger.info("Building master GeoJSON FeatureCollection...")

    try:
        feature_map = load_cached_feature_map(s3_client)
    except (ValueError, json.JSONDecodeError) as e:
        logger.warning(f"Could not load cached aggregate GeoJSON: {e}")
        feature_map = {}

    cached_feature_count = len(feature_map)

    if full_discovery:
        current_project_ids = {
            project["projectId"]
            for project in projects_summary
            if isinstance(project.get("projectId"), int)
        }
        summary_last_updated = {
            project["projectId"]: project.get("lastUpdated")
            for project in projects_summary
            if isinstance(project.get("projectId"), int)
        }
        feature_map = {
            project_id: feature
            for project_id, feature in feature_map.items()
            if project_id in current_project_ids
        }
        removed_projects = cached_feature_count - len(feature_map)
        if removed_projects:
            logger.info(
                "Full discovery removed %s cached projects no longer present in the public corpus",
                removed_projects,
            )

        project_ids_to_refresh = set(updated_projects)
        for project_id in current_project_ids:
            if project_id in project_ids_to_refresh:
                continue

            cached_feature = feature_map.get(project_id)
            if not cached_feature:
                project_ids_to_refresh.add(project_id)
                continue

            feature_last_updated = (
                cached_feature.get("properties", {}) or {}
            ).get("lastUpdated")
            if feature_last_updated != summary_last_updated.get(project_id):
                project_ids_to_refresh.add(project_id)

        expected_last_updated = {
            project_id: summary_last_updated.get(project_id)
            for project_id in current_project_ids
        }
        aggregate_rebuild_required = bool(project_ids_to_refresh) or bool(
            removed_projects
        ) or state_manager.needs_aggregate_rebuild()
    else:
        known_project_ids = state_manager.get_known_project_ids()
        project_ids_to_refresh = set(updated_projects)
        for project_id in known_project_ids:
            if project_id in project_ids_to_refresh:
                continue

            cached_feature = feature_map.get(project_id)
            if not cached_feature:
                project_ids_to_refresh.add(project_id)
                continue

            feature_last_updated = (
                cached_feature.get("properties", {}) or {}
            ).get("lastUpdated")
            if feature_last_updated != state_manager.project_timestamps.get(
                str(project_id)
            ):
                project_ids_to_refresh.add(project_id)

        expected_last_updated = {
            project_id: state_manager.project_timestamps.get(str(project_id))
            for project_id in known_project_ids
        }
        aggregate_rebuild_required = bool(project_ids_to_refresh) or state_manager.needs_aggregate_rebuild()

    if not projects_to_update and not aggregate_rebuild_required:
        if full_discovery:
            state_manager.save()
        logger.info("No changes detected, skipping uploads")
        logger.info("ETL complete!")
        return

    if not updated_projects and not aggregate_rebuild_required:
        logger.warning("No project updates were uploaded successfully, skipping rebuild")
        logger.info("ETL complete!")
        return

    if project_ids_to_refresh:
        logger.info(
            "Refreshing %s aggregate features from project detail cache",
            len(project_ids_to_refresh),
        )

    refresh_failures = 0
    for project_id in sorted(project_ids_to_refresh):
        try:
            details = get_project_details_for_rebuild(
                s3_client,
                api_client,
                project_id,
                updated_projects,
                expected_last_updated=expected_last_updated.get(project_id),
            )
            feature = build_feature(details)
            if feature:
                feature_map[project_id] = feature
            else:
                feature_map.pop(project_id, None)

        except (requests.RequestException, json.JSONDecodeError) as e:
            logger.warning(f"Could not process project {project_id}: {e}")
            refresh_failures += 1
            continue

    features = [feature_map[project_id] for project_id in sorted(feature_map)]

    feature_collection = {"type": "FeatureCollection", "features": features}
    logger.info(f"Created FeatureCollection with {len(features)} features")

    # Build projects summary JSON (no geometries, for dashboard)
    logger.info("Building projects summary JSON...")
    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    summary_projects = [build_summary_entry(f) for f in features]
    summary = {
        "generated": generated_at,
        "totalProjects": len(summary_projects),
        "projects": summary_projects,
    }

    # Use temp directory for intermediate files
    aggregate_success = False
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)
        geojson_path = tmpdir_path / ALL_PROJECTS_GEOJSON
        pmtiles_path = tmpdir_path / PMTILES_OUTPUT

        # Write GeoJSON
        with open(geojson_path, "w") as f:
            json.dump(feature_collection, f)

        # Upload GeoJSON
        s3_client.put_object(
            ALL_PROJECTS_GEOJSON,
            json.dumps(feature_collection).encode("utf-8"),
            "application/geo+json",
        )
        logger.info(f"Uploaded {ALL_PROJECTS_GEOJSON}")

        # Upload projects summary
        s3_client.put_object(
            PROJECTS_SUMMARY,
            json.dumps(summary).encode("utf-8"),
            "application/json",
        )
        logger.info(f"Uploaded {PROJECTS_SUMMARY}")

        # Generate and upload PMTiles
        if generate_pmtiles(geojson_path, pmtiles_path):
            with open(pmtiles_path, "rb") as f:
                pmtiles_data = f.read()

            s3_client.put_object(
                PMTILES_OUTPUT,
                pmtiles_data,
                "application/vnd.pmtiles",
            )
            logger.info(f"Uploaded {PMTILES_OUTPUT}")
            if refresh_failures == 0 and failed_project_updates == 0:
                aggregate_success = True
            else:
                logger.warning(
                    "Aggregate rebuild remains dirty because %s project updates failed and %s project features could not be refreshed",
                    failed_project_updates,
                    refresh_failures,
                )
        else:
            logger.warning("PMTiles generation failed, skipping upload")

    if aggregate_success:
        state_manager.mark_aggregate_clean(generated_at)
        state_manager.save()
    else:
        logger.warning("Aggregate rebuild incomplete; retry will resume from cached state")

    logger.info("ETL complete!")


def validate_env():
    """Validate required environment variables are set."""
    required = ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_BUCKET_NAME"]
    missing = [var for var in required if not os.environ.get(var)]

    if missing:
        logger.error(f"Missing required environment variables: {', '.join(missing)}")
        sys.exit(1)


if __name__ == "__main__":
    validate_env()
    run_etl()
