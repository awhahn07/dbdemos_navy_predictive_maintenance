# PDM Demo Documentation

This directory contains documentation for the Navy Predictive Maintenance Databricks Asset Bundle.

## Available Documentation

### [DAB-Cleanup-Summary.md](./DAB-Cleanup-Summary.md)
**Complete technical summary of DAB structure cleanup and implementation**

- **What it covers**: Comprehensive overview of all changes made to convert the legacy project into a proper DAB
- **Key sections**: Issues resolved, technical changes, validation results, deployment workflow
- **Use case**: Reference for understanding the DAB structure, troubleshooting, and future maintenance

### Quick Reference

**Bundle Status**: ✅ Production Ready  
**Validation**: `databricks bundle validate --target dev` → `Validation OK!`  
**Deployment**: `databricks bundle deploy --target dev`  
**Execution**: `databricks bundle run pdm_job --target dev`

### Key Files Modified
- `databricks.yml` - Main bundle configuration with variables
- `resources/pdm_job.job.yml` - Job workflow definition  
- `resources/pdm_pipeline.pipelines.yml` - DLT pipeline configurations (ingestion and inference)

### Project Structure
```
pdm_demo/
├── databricks.yml           # Bundle configuration
├── docs/                    # This documentation
├── resources/               # DAB resource definitions
└── src/                     # Source code (notebooks & scripts)
```

For detailed technical information, see [DAB-Cleanup-Summary.md](./DAB-Cleanup-Summary.md).
