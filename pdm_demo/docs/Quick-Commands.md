# Quick Commands Reference

## Essential DAB Commands

### Bundle Operations
```bash
# Navigate to bundle directory
cd /Users/andrew.hahn/Development/dbdemos_navy_predictive_maintenance/pdm_demo

# Validate bundle configuration
databricks bundle validate --target dev

# Deploy bundle to development environment
databricks bundle deploy --target dev

# Deploy to production
databricks bundle deploy --target prod

# View bundle summary
databricks bundle summary --target dev
```

### Job Operations
```bash
# Run the complete PDM workflow
databricks bundle run pdm_job --target dev

# List all jobs
databricks jobs list | grep pdm

# Monitor job execution
databricks jobs get --job-id <job-id>
```

### Pipeline Operations  
```bash
# List pipelines
databricks pipelines list | grep pdm

# Get pipeline status
databricks pipelines get --pipeline-id <pipeline-id>
```

### Authentication & Setup
```bash
# Configure CLI authentication
databricks configure --host https://e2-demo-field-eng.cloud.databricks.com

# Test authentication
databricks auth env

# Check CLI version
databricks --version
```

## Environment Variables

### Development Target
- **workspace_host**: `https://e2-demo-field-eng.cloud.databricks.com`
- **schema**: `predictive_maintenance_dev`
- **instance_pool_id**: `0727-104344-hauls13-pool-uftxk0r6`

### Bundle Variables
- **catalog**: `public_sector`
- **demo_type**: `navy`
- **volume_name**: `raw_landing`

## Troubleshooting

### Common Issues
```bash
# Path not found errors
# Solution: Verify paths use ../src/ format

# Authentication errors  
# Solution: Run databricks configure

# Variable resolution errors
# Solution: Check databricks.yml target definitions
```

### File Locations
- **Bundle Config**: `databricks.yml`
- **Job Definition**: `resources/pdm_job.job.yml`  
- **Pipeline Config**: `resources/pdm_pipeline.pipelines.yml`
- **Source Code**: `src/` directory
- **Documentation**: `docs/` directory
