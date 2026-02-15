# Insta-TM

A cloud-native mirror of the [HOT Tasking Manager](https://tasks.hotosm.org/) API. This project fetches project data from the official HOT API, transforms it into cloud-native formats, and serves it as a read-only REST-like API via S3.

## Why?

The HOT Tasking Manager API is essential for humanitarian mapping coordination, but direct API access can be slow for bulk operations and lacks cloud-native formats like PMTiles for map visualization. Insta-TM provides:

- **Fast, cached access** to project data via S3/CloudFront
- **REST-like URLs** that mimic the original API structure
- **PMTiles output** for efficient vector tile rendering
- **GeoJSON export** of all project boundaries
- **Incremental sync** that only fetches updated projects

## Data Endpoints

| Resource | URL |
|----------|-----|
| **Project JSON** | `https://insta-tm.s3.us-east-1.amazonaws.com/api/v2/projects/{id}` |
| **All Projects GeoJSON** | https://insta-tm.s3.us-east-1.amazonaws.com/all_projects.geojson |
| **Projects PMTiles** | https://insta-tm.s3.us-east-1.amazonaws.com/projects.pmtiles |

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
  percentValidated: .percentValidated
}'
```

### Download all project boundaries

```bash
curl -O https://insta-tm.s3.us-east-1.amazonaws.com/all_projects.geojson
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

1. **GitHub Actions** runs the ETL script daily (06:00 UTC)
2. The script fetches the project list from the HOT Tasking Manager API
3. **Incremental sync** compares `lastUpdated` timestamps against stored state
4. Only changed projects are fetched in full and uploaded to S3
5. If changes were detected, all project geometries are combined into a single GeoJSON file
6. **Tippecanoe** generates PMTiles (zoom 0-12) for vector tile access
7. If no changes are detected, the script exits early to minimize S3 PUT requests

## Tech Stack

- **Python 3.11+** - ETL script
- **GitHub Actions** - Scheduled compute
- **AWS S3** - Storage and hosting
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

# Optional: for S3-compatible storage (e.g., Source.coop)
export S3_ENDPOINT_URL=https://your-endpoint.com

# Run ETL
python etl.py
```

## Configuration

| Environment Variable | Required | Description |
|---------------------|----------|-------------|
| `AWS_ACCESS_KEY_ID` | Yes | AWS credentials |
| `AWS_SECRET_ACCESS_KEY` | Yes | AWS credentials |
| `AWS_BUCKET_NAME` | Yes | Target S3 bucket |
| `AWS_REGION` | Yes | AWS region |
| `S3_ENDPOINT_URL` | No | Custom endpoint for S3-compatible storage |

## AI-Generated Code Disclaimer

**A significant portion of this application's code was generated with assistance from AI tools.**

### Tools Used
- **Claude** (Anthropic) - Code generation, architecture design, and documentation

### What This Means
- The codebase was developed with AI assistance based on requirements and iterative prompts
- All functionality has been tested and verified to work as intended
- The code has undergone human review for usability and correctness

## License

MIT

## Credits

- [HOT Tasking Manager](https://github.com/hotosm/tasking-manager) - Source API
- [Humanitarian OpenStreetMap Team](https://www.hotosm.org/) - Project data
