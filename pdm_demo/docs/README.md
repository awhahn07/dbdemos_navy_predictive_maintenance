# PDM Demo Documentation

This directory contains documentation for the Navy Predictive Maintenance Databricks Asset Bundle.

## Available Documentation

### [DAB-Cleanup-Summary.md](./DAB-Cleanup-Summary.md)
**Complete technical summary of DAB structure cleanup and implementation**

- **What it covers**: Comprehensive overview of all changes made to convert the legacy project into a proper DAB
- **Key sections**: Issues resolved, technical changes, validation results, deployment workflow  
- **Use case**: Reference for understanding the DAB structure, troubleshooting, and future maintenance
- **Latest Update**: Job workflow optimization with model endpoint validation (Oct 21, 2025)

### [Job-Workflow-Changes.md](./Job-Workflow-Changes.md) 
**Summary of job workflow optimization and model endpoint validation**

- **What it covers**: Recent changes to job configuration, validation system, and retry logic
- **Key features**: Model endpoint validation, retry mechanisms, streamlined workflow
- **Use case**: Quick reference for understanding validation features and troubleshooting endpoint issues

### Quick Reference

**Bundle Status**: ✅ Production Ready  
**Validation**: `databricks bundle validate --target dev` → `Validation OK!`  
**Deployment**: `databricks bundle deploy --target dev`  
**Execution**: `databricks bundle run pdm_job --target dev`

### Key Files Modified
- `databricks.yml` - Main bundle configuration with variables
- `resources/pdm_job.job.yml` - Job workflow definition with model endpoint validation
- `resources/pdm_pipeline.pipelines.yml` - DLT pipeline configurations (ingestion and inference)
- `src/_resources/validate-model-endpoint.py` - Model serving endpoint validation notebook

### Project Structure
```
pdm_demo/
├── databricks.yml           # Bundle configuration
├── docs/                    # This documentation
├── resources/               # DAB resource definitions
└── src/                     # Source code (notebooks & scripts)
```

For detailed technical information, see [DAB-Cleanup-Summary.md](./DAB-Cleanup-Summary.md).
