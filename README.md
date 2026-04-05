# Insta-TM

A cloud-native mirror of the [HOT Tasking Manager](https://tasks.hotosm.org/) API. This project fetches project data from the official HOT API, transforms it into cloud-native formats, and serves it as a read-only REST-like API.

## Why?

The HOT Tasking Manager API is essential for humanitarian mapping coordination, but direct API access can be slow for bulk operations and lacks cloud-native formats like PMTiles for map visualization. Insta-TM provides:

- **Fast, cached access** to project metadata via CDN
- **REST-like URLs** that mimic the original API structure
- **PMTiles output** for efficient vector tile rendering
- **GeoJSON export** of all project boundaries with gzip compression
- **Incremental sync** that only fetches updated projects

## Data Endpoints

| Resource | Description |
|----------|-------------|
| **Project JSON** | `{base_url}/api/v2/projects/{id}` |
| **All Projects GeoJSON** | `{base_url}/all_projects.geojson` |
| **Projects PMTiles** | `{base_url}/projects.pmtiles` |
| **Projects Summary** | `{base_url}/projects_summary.json` |

> **Note:** The GeoJSON and summary files are served with gzip `Content-Encoding`.
> Most HTTP clients (curl, browsers, fetch API) decompress automatically.

## What's in the Project JSON?

Each individual project file contains a curated subset of the HOT API response - the fields most commonly used by downstream consumers:

```json
{
  "projectId": 17823,
  "projectInfo": { "name": "...", "shortDescription": "..." },
  "status": "PUBLISHED",
  "created": "2024-01-15T10:00:00Z",
  "lastUpdated": "2024-06-01T12:00:00Z",
  "author": "username",
  "organisationName": "HOT",
  "countryTag": ["Mozambique"],
  "imagery": "https://...",
  "mappingTypes": ["BUILDINGS"],
  "difficulty": "MODERATE",
  "projectPriority": "MEDIUM",
  "percentMapped": 85,
  "percentValidated": 42,
  "areaOfInterest": { "type": "Polygon", "coordinates": [...] },
  "aoiCentroid": { "type": "Point", "coordinates": [35.0, -15.0] },
  "totalTasks": 500
}
```

**Need a field that's not included?** Open a [GitHub issue](https://github.com/cgiovando/insta-tm/issues) describing your use case, and we can review adding it.

## API Examples

### Fetch a single project

```bash
curl https://insta-tm.s3.us-east-1.amazonaws.com/api/v2/projects/17823
```

### Get project name with jq

```bash
curl -s https://insta-tm.s3.us-east-1.amazonaws.com/api/v2/projects/17823 | jq '.projectInfo.name'
```

### Get project status and progress

```bash
curl -s https://insta-tm.s3.us-east-1.amazonaws.com/api/v2/projects/17823 | jq '{
  status: .status,
  percentMapped: .percentMapped,
  percentValidated: .percentValidated,
  totalTasks: .totalTasks
}'
```

### Download all project boundaries

The GeoJSON and summary files are served with gzip compression. Use `--compressed` so curl decompresses automatically:

```bash
curl --compressed -O https://insta-tm.s3.us-east-1.amazonaws.com/all_projects.geojson
```

### Use PMTiles in MapLibre GL JS

```javascript
import maplibregl from 'maplibre-gl';
import { Protocol } from 'pmtiles';

const protocol = new Protocol();
maplibregl.addProtocol('pmtiles', protocol.tile);

const map = new maplibregl.Map({
  container: 'map',
  style: {
    version: 8,
    sources: {
      projects: {
        type: 'vector',
        url: 'pmtiles://https://insta-tm.s3.us-east-1.amazonaws.com/projects.pmtiles'
      }
    },
    layers: [{
      id: 'projects-fill',
      type: 'fill',
      source: 'projects',
      'source-layer': 'projects',
      paint: {
        'fill-color': '#d73f3f',
        'fill-opacity': 0.5
      }
    }]
  }
});
```

## How It Works

1. **GitHub Actions** runs the ETL script daily (06:00 UTC), with a concurrency guard to prevent overlapping runs
2. Most daily runs query the HOT Tasking Manager API with `lastUpdatedFrom` and a 1-day safety overlap instead of crawling the full project corpus
3. A periodic full discovery pass (every 14 days) reconciles removals and long-missed drift
4. Changed projects are fetched, trimmed to essential fields, and uploaded as lean JSON files
5. If changes were detected, the existing aggregate GeoJSON is patched in place and only stale or missing features are rebuilt from the project detail cache
6. **Tippecanoe** generates PMTiles with a curated set of map-rendering properties
7. Aggregate files are uploaded with gzip compression and Cache-Control headers
8. If no changes are detected and aggregate artifacts are already current, the script exits early

## Tech Stack

- **Python 3.11+** - ETL script
- **GitHub Actions** - Scheduled compute
- **S3-compatible storage** - Storage and hosting (supports AWS S3, Cloudflare R2)
- **Tippecanoe** - Vector tile generation
- **boto3** - S3 client with custom endpoint support

## Running Locally

```bash
# Install dependencies
pip install -r requirements.txt

# Install tippecanoe (macOS)
brew install tippecanoe

# Set environment variables
export AWS_ACCESS_KEY_ID=your_key
export AWS_SECRET_ACCESS_KEY=your_secret
export AWS_BUCKET_NAME=your_bucket
export AWS_REGION=us-east-1

# Optional: for S3-compatible storage (e.g., Cloudflare R2)
export S3_ENDPOINT_URL=https://your-endpoint.com

# Run ETL
python etl.py
```

## Configuration

| Environment Variable | Required | Description |
|---------------------|----------|-------------|
| `AWS_ACCESS_KEY_ID` | Yes | S3-compatible credentials |
| `AWS_SECRET_ACCESS_KEY` | Yes | S3-compatible credentials |
| `AWS_BUCKET_NAME` | Yes | Target bucket |
| `AWS_REGION` | Yes | AWS region (auto-detected as `auto` for Cloudflare R2) |
| `S3_ENDPOINT_URL` | No | Custom endpoint for S3-compatible storage |

## AI-assisted development

> This project was developed with significant assistance from AI coding tools.

- **[Claude Code](https://claude.ai/claude-code)** (Anthropic) - code generation, architecture, debugging, and documentation
- All functionality has been tested and verified to work as intended
- Features and infrastructure choices have been reviewed and approved by the maintainer

This disclosure follows emerging best practices for transparency in AI-assisted software development.

## License

MIT

## Credits

- [HOT Tasking Manager](https://github.com/hotosm/tasking-manager) - Source API
- [Humanitarian OpenStreetMap Team](https://www.hotosm.org/) - Project data
