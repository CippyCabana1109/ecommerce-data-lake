# Data Lineage Logs

This directory contains comprehensive lineage logs for the ecommerce data lake pipeline, providing audit trails and governance information.

## Log Types

### Transformation Logs
- **Format**: `{job_name}_{timestamp}.json`
- **Content**: Complete transformation details including input/output paths, transformations applied, and metrics
- **Purpose**: Track data movement and processing steps

### Data Quality Logs
- **Format**: `quality_{job_name}_{timestamp}.json`
- **Content**: Data quality check results and compliance status
- **Purpose**: Monitor data quality and validation results

### Schema Change Logs
- **Format**: `schema_{table_name}_{timestamp}.json`
- **Content**: Schema evolution details and impact analysis
- **Purpose**: Track structural changes to data tables

## Central Log File

- **File**: `log.txt`
- **Content**: Consolidated logging of all pipeline activities
- **Purpose**: Quick reference and troubleshooting

## Usage

### Viewing Recent Lineage
```bash
# View recent transformation logs
ls -la data/lineage/*transform*.json | tail -5

# View recent quality logs
ls -la data/lineage/quality_*.json | tail -5

# View central log
tail -f data/lineage/log.txt
```

### Querying Lineage Data
```python
from src.utils.lineage_logger import get_lineage_logger

# Get lineage logger
logger = get_lineage_logger()

# Get summary for last 7 days
summary = logger.get_lineage_summary(days=7)
print(summary)
```

## Retention Policy

- **Transformation Logs**: Retained for 90 days
- **Quality Logs**: Retained for 1 year
- **Schema Logs**: Retained indefinitely
- **Central Log**: Rotated weekly, retained for 30 days

## Compliance

These logs support:
- **Data Governance**: Complete audit trail of data transformations
- **Regulatory Compliance**: Track data processing and quality validation
- **Troubleshooting**: Detailed error tracking and performance metrics
- **Impact Analysis**: Schema change tracking and compatibility checks
