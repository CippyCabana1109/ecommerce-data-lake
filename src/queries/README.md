# AWS Athena Queries for Ecommerce Data Lake

This directory contains SQL queries and setup scripts for analyzing the ecommerce data lake using AWS Athena.

## Files

- `setup_catalog.py` - Python script to set up AWS Glue catalog with database and crawlers
- `insights.sql` - Athena SQL queries for business insights and analysis

## Setup Instructions

### 1. Configure AWS Credentials

Make sure you have AWS credentials configured:
```bash
aws configure
```

Or set environment variables:
```bash
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
export AWS_DEFAULT_REGION=us-east-1
```

### 2. Run Glue Catalog Setup

```bash
# Navigate to the queries directory
cd src/queries

# Update the bucket name in setup_catalog.py
# Replace [your-bucket] with your actual S3 bucket name

# Run the setup script
python setup_catalog.py
```

This script will:
- Create Glue database `ecommerce_db`
- Create IAM role for Glue crawlers
- Set up crawlers for raw, refined, and curated zones
- Schedule crawlers to run daily at 2 AM UTC

### 3. Start Crawlers (Optional)

The setup script creates crawlers but doesn't start them automatically. You can:

**Option A: Start manually in AWS Console**
1. Go to AWS Glue console
2. Navigate to Crawlers
3. Select and start each crawler

**Option B: Start via script**
Uncomment the `setup.start_all_crawlers()` line in `setup_catalog.py` and rerun

**Option C: Wait for scheduled execution**
Crawlers will run automatically daily at 2 AM UTC

### 4. Run Athena Queries

#### Using AWS Athena Console:

1. **Open Athena Console**
   - Go to AWS Management Console
   - Search for "Athena"
   - Open the Athena service

2. **Set Query Result Location**
   - In Settings, set a query result location in S3
   - Example: `s3://[your-bucket]/athena-results/`

3. **Select Database**
   - In the dropdown, select `ecommerce_db`
   - Tables should appear after crawlers complete

4. **Run Queries**
   - Open `insights.sql` in a text editor
   - Copy and paste queries into Athena console
   - Execute queries one by one

#### Using AWS CLI:

```bash
# Set up Athena workgroup (if needed)
aws athena create-work-group --name ecommerce_queries --configuration 'ResultConfiguration={OutputLocation=s3://[your-bucket]/athena-results/}'

# Run a query
aws athena start-query-execution \
    --query-string "SELECT county, SUM(total_revenue) as total FROM ecommerce_db.comprehensive_view GROUP BY county ORDER BY total DESC LIMIT 10;" \
    --query-execution-context Database=ecommerce_db \
    --result-configuration OutputLocation=s3://[your-bucket]/athena-results/
```

## Available Tables

After crawlers run, you'll have these tables in `ecommerce_db`:

### From Curated Zone:
- `comprehensive_view` - Multi-dimensional data (county × category × product)
- `sales_by_county` - Aggregated sales by county
- `sales_by_product` - Aggregated sales by product
- `sales_by_category` - Aggregated sales by category
- `top_items_per_county` - Top 10 items per county

### From Refined Zone:
- `refined_sales` - Processed sales data
- `refined_products` - Processed products data

### From Raw Zone:
- `raw_sales` - Original sales CSV data
- `raw_products` - Original products CSV data

## Key Queries in insights.sql

### Business Intelligence:
- **Top 10 counties by revenue**
- **Top 10 product categories**
- **Top selling products nationwide**
- **Revenue distribution by county and category**

### Advanced Analytics:
- **County-specific top products**
- **Category performance analysis**
- **Price range analysis**
- **Market penetration metrics**

### Data Quality:
- **Negative revenue detection**
- **Zero quantity checks**
- **Null value validation**

## Performance Tips

1. **Partition Pruning**: Filter on partition columns (county, category)
   ```sql
   SELECT * FROM comprehensive_view WHERE county = 'Nairobi';
   ```

2. **Column Pruning**: Select only needed columns
   ```sql
   SELECT product_name, total_revenue FROM sales_by_product;
   ```

3. **Result Caching**: Use workgroup settings for result caching

4. **Query Optimization**: Use CTAS for complex transformations
   ```sql
   CREATE TABLE temp_results AS SELECT ...;
   ```

## Troubleshooting

### Common Issues:

1. **Tables not visible in Athena**
   - Check if crawlers completed successfully
   - Verify S3 paths are correct
   - Run `MSCK REPAIR TABLE table_name;`

2. **Permission errors**
   - Ensure IAM role has S3 and Glue permissions
   - Check bucket policies

3. **Query timeouts**
   - Increase workgroup timeout settings
   - Optimize queries with partition filters

4. **Delta Lake issues**
   - Ensure Delta format is supported in your Athena version
   - Use proper SERDE configurations

### Monitoring:

- **CloudWatch Logs**: Check Glue crawler logs
- **Athena Query History**: Monitor query performance
- **S3 Inventory**: Verify data availability

## Cost Optimization

1. **Athena**: Use result caching and compression
2. **Glue**: Schedule crawlers appropriately
3. **S3**: Use appropriate storage classes
4. **Data Lifecycle**: Implement S3 lifecycle policies

## Next Steps

1. Set up automated dashboards (QuickSight, Grafana)
2. Create scheduled reports
3. Implement alerting for data quality issues
4. Add more sophisticated analytics
5. Set up data governance policies
