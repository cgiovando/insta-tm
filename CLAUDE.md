# CLAUDE.md - Insta-TM Project Guide

## Project Summary
Cloud-native mirror of the HOT Tasking Manager API. Fetches project data, transforms to lean JSON + compressed GeoJSON + PMTiles, uploads to S3-compatible storage (AWS S3 or Cloudflare R2).

## Key Files
- `etl.py` - Main ETL script (fetch, transform, upload, summary generation)
- `requirements.txt` - Python dependencies (boto3, requests, shapely, pyproj)
- `.github/workflows/sync.yml` - Daily GitHub Actions workflow (timeout: 240min for backfill)

## Architecture
```
HOT TM API --> GitHub Actions (daily) --> S3-compatible storage (R2 or S3)
                    |                            |
              tippecanoe                   CDN (Cloudflare)
                    |                            |
              PMTiles                      Public consumers
```

## Environment Variables
| Variable | Required | Description |
|----------|----------|-------------|
| `AWS_ACCESS_KEY_ID` | Yes | S3-compatible credentials |
| `AWS_SECRET_ACCESS_KEY` | Yes | S3-compatible credentials |
| `AWS_BUCKET_NAME` | Yes | Target bucket (`insta-tm`) |
| `AWS_REGION` | Yes | AWS region (auto-detected as `auto` for R2) |
| `S3_ENDPOINT_URL` | No | Custom endpoint for R2 or other S3-compatible storage |

## GitHub Secrets
All env vars above are configured as secrets in the repo.

## Storage Structure
```
insta-tm/
├── state.v3.json              # Canonical sync state (project timestamps + aggregate dirty flag)
├── all_projects.geojson       # All project boundaries, gzip compressed (Content-Encoding: gzip)
├── projects.pmtiles           # Vector tiles (z0-12), curated properties only
├── projects_summary.json      # Lightweight summary for dashboard, gzip compressed
└── api/v2/projects/
    └── {id}                   # Lean project JSON (curated fields only, no extension)
```

## Key Design Decisions
- Individual project files store only consumer-needed fields (not full API responses)
- `totalTasks` stored as scalar instead of full task geometry array
- GeoJSON and summary uploaded with gzip `Content-Encoding` for smaller transfers
- PMTiles NOT gzipped (has internal compression, relies on HTTP range requests)
- Tippecanoe uses `-y` property whitelist for lean tiles (no `--no-feature-limit`/`--no-tile-size-limit`)
- All public objects include `Cache-Control: public, max-age=3600`
- JSON minified with `separators=(",", ":")` instead of `indent=2`
- R2 auto-detected from `S3_ENDPOINT_URL` containing `cloudflarestorage.com`
- Incremental sync via `lastUpdated` comparison with 1-day overlap
- Full discovery every 14 days to reconcile removals and drift
- Skip rebuild if no changes (cost optimization)
- State checkpoints during project uploads so failed runs resume near the last successful upload
- Aggregate artifacts stay dirty until GeoJSON, summary, and PMTiles all finish uploading

## Dependent Projects
- `cgiovando/hot-imagery-stats` - reads `api/v2/projects/` via boto3
- `cgiovando/osm-carbon-date` - reads GeoJSON, PMTiles, and individual project JSONs via HTTP

## Common Commands
```bash
# Run locally
python etl.py

# Trigger workflow manually
gh workflow run sync.yml --repo cgiovando/insta-tm

# View workflow logs
gh run list --repo cgiovando/insta-tm
gh run view <run-id> --repo cgiovando/insta-tm --log
```
