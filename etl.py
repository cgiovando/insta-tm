#!/usr/bin/env python3
"""
HOT Tasking Manager Cloud Native Mirror ETL Script

Fetches project data from the HOT Tasking Manager API, transforms it into
cloud-native formats, and uploads to S3 (or S3-compatible storage like Source.coop).
"""

import json
import logging
import os
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import boto3
import requests
from botocore.config import Config

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
STATE_FILE_KEY = "state.json"
ALL_PROJECTS_GEOJSON = "all_projects.geojson"
PMTILES_OUTPUT = "projects.pmtiles"


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
        except self.client.exceptions.NoSuchKey:
            return None
        except Exception as e:
            logger.warning(f"Error fetching {key}: {e}")
            return None

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

    def get_projects_list(self, page: int = 1) -> dict[str, Any]:
        """Fetch a page of projects from the API."""
        params = {
            "orderBy": "last_updated",
            "orderByType": "DESC",
            "page": page,
        }
        response = self.session.get(PROJECTS_ENDPOINT, params=params, timeout=60)
        response.raise_for_status()
        return response.json()

    def get_all_projects_summary(self) -> list[dict[str, Any]]:
        """Fetch all projects summary (paginated)."""
        all_projects = []
        page = 1

        while True:
            logger.info(f"Fetching projects list page {page}...")
            data = self.get_projects_list(page)
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
        self.state: dict[str, str] = {}  # project_id -> last_updated timestamp

    def load(self) -> None:
        """Load state from S3 or reconstruct from existing files."""
        data = self.s3_client.get_object(STATE_FILE_KEY)
        if data:
            self.state = json.loads(data.decode("utf-8"))
            logger.info(f"Loaded state with {len(self.state)} projects")
        else:
            logger.info("No existing state found, starting fresh")
            self.state = {}

    def save(self) -> None:
        """Save state to S3."""
        self.s3_client.put_object(
            STATE_FILE_KEY,
            json.dumps(self.state, indent=2).encode("utf-8"),
            "application/json",
        )
        logger.info(f"Saved state with {len(self.state)} projects")

    def needs_update(self, project_id: int, last_updated: str) -> bool:
        """Check if a project needs to be fetched based on last_updated timestamp."""
        project_key = str(project_id)
        stored_timestamp = self.state.get(project_key)

        if not stored_timestamp:
            return True

        return last_updated != stored_timestamp

    def mark_updated(self, project_id: int, last_updated: str) -> None:
        """Mark a project as updated in state."""
        self.state[str(project_id)] = last_updated


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

    # Initialize clients
    s3_client = S3Client()
    api_client = HOTApiClient()
    state_manager = StateManager(s3_client)

    # Load existing state
    state_manager.load()

    # Fetch all projects summary
    projects_summary = api_client.get_all_projects_summary()

    # Track which projects need updates
    projects_to_update = []
    for project in projects_summary:
        project_id = project.get("projectId")
        last_updated = project.get("lastUpdated")

        if project_id and last_updated:
            if state_manager.needs_update(project_id, last_updated):
                projects_to_update.append((project_id, last_updated))

    logger.info(f"{len(projects_to_update)} projects need updating")

    # Fetch and upload updated project details
    updated_projects = []
    for project_id, last_updated in projects_to_update:
        try:
            logger.info(f"Fetching project {project_id}...")
            details = api_client.get_project_details(project_id)

            # Upload project JSON (no .json extension, but with correct Content-Type)
            s3_key = f"api/v2/projects/{project_id}"
            s3_client.put_object(
                s3_key,
                json.dumps(details, indent=2).encode("utf-8"),
                "application/json",
            )

            state_manager.mark_updated(project_id, last_updated)
            updated_projects.append(details)
            logger.info(f"Uploaded project {project_id}")

        except requests.RequestException as e:
            logger.error(f"Failed to fetch project {project_id}: {e}")
            continue

    # Build master GeoJSON from all projects
    logger.info("Building master GeoJSON FeatureCollection...")

    features = []
    all_project_ids = set()

    # Collect all project IDs from summary
    for project in projects_summary:
        project_id = project.get("projectId")
        if project_id:
            all_project_ids.add(project_id)

    # Fetch AOI from each project (use cached data where possible)
    for project_id in all_project_ids:
        try:
            # Check if we just updated this project
            updated_detail = next(
                (p for p in updated_projects if p.get("projectId") == project_id), None
            )

            if updated_detail:
                details = updated_detail
            else:
                # Fetch from API (or could fetch from S3, but API is authoritative)
                details = api_client.get_project_details(project_id)

            aoi = details.get("areaOfInterest")
            if aoi:
                # Create a feature with project metadata
                feature = {
                    "type": "Feature",
                    "geometry": aoi,
                    "properties": {
                        "projectId": project_id,
                        "name": details.get("projectInfo", {}).get("name", ""),
                        "status": details.get("status"),
                        "percentMapped": details.get("percentMapped"),
                        "percentValidated": details.get("percentValidated"),
                        "lastUpdated": details.get("lastUpdated"),
                    },
                }
                features.append(feature)

        except requests.RequestException as e:
            logger.warning(f"Could not fetch AOI for project {project_id}: {e}")
            continue

    feature_collection = {"type": "FeatureCollection", "features": features}
    logger.info(f"Created FeatureCollection with {len(features)} features")

    # Use temp directory for intermediate files
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
        else:
            logger.warning("PMTiles generation failed, skipping upload")

    # Save updated state
    state_manager.save()

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
