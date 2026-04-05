# Cloudflare R2 Migration Guide

## Why R2?

Cloudflare R2 has **zero egress fees** and an S3-compatible API. The free tier covers this project's usage:

| Resource | Free tier | Our usage |
|----------|-----------|-----------|
| Storage | 10 GB/month | ~300 MB (after optimization) |
| Class A ops (writes) | 1M/month | ~177K/month |
| Class B ops (reads) | 10M/month | ~384K/month |
| Egress | Unlimited (free) | Was costing $13.78/month on S3 |

## Setup Steps

### 1. Create R2 bucket

1. Go to [Cloudflare Dashboard](https://dash.cloudflare.com/) > R2 Object Storage
2. Click **Create bucket**
3. Name: `insta-tm`
4. Location hint: **Eastern North America (ENAM)** (closest to GitHub Actions runners)
5. Default storage class: **Standard**
6. Click **Create bucket**

### 2. Enable public access

1. Open the `insta-tm` bucket in the dashboard
2. Go to **Settings** > **Public access**
3. Option A (recommended): **Custom domain**
   - Add a custom domain like `insta-tm.yourdomain.com`
   - Cloudflare automatically provisions SSL and CDN caching
   - This gives you bot protection, rate limiting, and analytics for free
4. Option B: **R2.dev subdomain**
   - Enable the `*.r2.dev` subdomain for quick testing
   - Not recommended for production (no custom caching rules)

### 3. Configure CORS (if using custom domain)

In the bucket settings, add a CORS rule:

```json
[
  {
    "AllowedOrigins": ["*"],
    "AllowedMethods": ["GET", "HEAD"],
    "AllowedHeaders": ["*"],
    "MaxAgeSeconds": 86400
  }
]
```

### 4. Create API token

1. Go to **R2 Object Storage** > **Manage R2 API Tokens**
2. Click **Create API token**
3. Permissions: **Object Read & Write**
4. Specify bucket: `insta-tm`
5. TTL: No expiration (or set a long TTL)
6. Click **Create API Token**
7. Save the **Access Key ID** and **Secret Access Key**
8. Note the **S3 API endpoint** shown (format: `https://<account_id>.r2.cloudflarestorage.com`)

### 5. Update GitHub secrets

In the `cgiovando/insta-tm` repo settings, update these secrets:

| Secret | New value |
|--------|-----------|
| `AWS_ACCESS_KEY_ID` | R2 Access Key ID |
| `AWS_SECRET_ACCESS_KEY` | R2 Secret Access Key |
| `AWS_BUCKET_NAME` | `insta-tm` |
| `AWS_REGION` | `auto` (auto-detected by etl.py for R2) |
| `S3_ENDPOINT_URL` | `https://<account_id>.r2.cloudflarestorage.com` |

### 6. Seed the bucket

Run the ETL once to populate R2. The first run will be a full rebuild since there's no existing state:

```bash
gh workflow run sync.yml --repo cgiovando/insta-tm
```

This will take longer than usual (~30-60 min) as it fetches all ~14K projects.

### 7. Update dependent projects

After R2 is live and verified:

**hot-imagery-stats** (`scripts/generate_summary.py`):
- Update to use R2 endpoint instead of direct S3 access
- Or: fetch individual project JSONs via the public CDN URL instead of boto3

**osm-carbon-date** (`js/config.js`):
- Update `CONFIG.tmApi.s3Base` to the new R2 public URL

### 8. Configure Cloudflare caching and protection

In the Cloudflare dashboard for your domain:

1. **Cache Rules**: Create a rule for `insta-tm.yourdomain.com/*`
   - Cache everything, respect origin Cache-Control headers
   - The ETL sets `Cache-Control: public, max-age=3600` on all objects

2. **Bot Fight Mode**: Enable under Security > Bots
   - Automatically challenges known bad bots

3. **Rate Limiting** (optional): Under Security > WAF
   - Example: Block IPs that request `all_projects.geojson` more than 10 times/hour

### 9. Decommission AWS S3

After verifying R2 works for at least a week:

1. Remove the S3 bucket policy (stops public access)
2. Wait another week to confirm nothing breaks
3. Delete the S3 bucket contents and bucket
4. Remove old AWS credentials from GitHub secrets

## Rollback

If something goes wrong, revert the GitHub secrets to the original AWS S3 values. The ETL code supports both S3 and R2 without changes.
