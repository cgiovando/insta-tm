# CLAUDE.md - Insta-TM Project Guide

## Project Summary
Cloud-native mirror of the HOT Tasking Manager API. Fetches project data, transforms to JSON + PMTiles, uploads to S3.

## Key Files
- `etl.py` - Main ETL script (fetch, transform, upload)
- `requirements.txt` - Python dependencies (boto3, requests)
- `.github/workflows/sync.yml` - Hourly GitHub Actions workflow

## Architecture
```
HOT TM API → GitHub Actions (hourly) → S3 Bucket
                    ↓
              tippecanoe
                    ↓
              PMTiles
```

## Environment Variables
| Variable | Required | Description |
|----------|----------|-------------|
| `AWS_ACCESS_KEY_ID` | Yes | AWS credentials |
| `AWS_SECRET_ACCESS_KEY` | Yes | AWS credentials |
| `AWS_BUCKET_NAME` | Yes | Target bucket (`insta-tm`) |
| `AWS_REGION` | Yes | AWS region (`us-east-1`) |
| `S3_ENDPOINT_URL` | No | Custom endpoint for Source.coop |

## GitHub Secrets
All env vars above are configured as secrets in the repo.

## S3 Structure
```
insta-tm/
├── state.json                 # Sync state (lastUpdated timestamps)
├── all_projects.geojson       # All project boundaries
├── projects.pmtiles           # Vector tiles (z0-12)
└── api/v2/projects/
    └── {id}                   # Individual project JSON (no extension)
```

## Key Design Decisions
- No `.json` extension on project files (REST-like URLs)
- `Content-Type: application/json` set explicitly
- Incremental sync via `lastUpdated` comparison
- Skip rebuild if no changes (cost optimization)
- S3 client supports custom endpoint for future Source.coop migration

## Common Commands
```bash
# Run locally
python etl.py

# Trigger workflow manually
gh workflow run sync.yml --repo cgiovando/insta-tm

# Check S3 contents
aws s3 ls s3://insta-tm/ --recursive --human-readable

# View workflow logs
gh run list --repo cgiovando/insta-tm
gh run view <run-id> --repo cgiovando/insta-tm --log
```

## Pending: Source.coop Migration
See MEMORY.md for details on IAM Role + OIDC setup needed.
