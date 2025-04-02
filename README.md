# Typeform ETL Pipeline for Lead Analysis

## Overview
This project creates an automated data pipeline to extract lead data from Typeform, transform it into structured formats, and prepare it for business intelligence and analytics applications. The pipeline helps businesses better understand client acquisition trends, build comprehensive lead profiles, and optimize their sales pipeline.

## Motivation
Typeform is an excellent tool for collecting lead data, but the raw information needs significant processing to become truly valuable for business insights. This project combines Typeform data with other potential sources to create a comprehensive view of the sales funnel and lead journey.

By transforming unstructured survey responses into analytics-ready datasets, businesses can:
- Track where potential clients are discovering their services
- Analyze demographic information like location and income levels
- Understand customer price sensitivity and budget constraints
- Identify bottlenecks in the conversion process

## Project Scope
The ETL pipeline focuses on extracting valuable customer acquisition data for an online service, including:
- Geographic distribution of potential clients
- Income demographics
- Budget allocations for services
- Lead sources and marketing channel effectiveness

## Technical Implementation
The current implementation follows the medallion architecture pattern:
1. **Data Ingestion**: Establishing API connection to Typeform and saving raw data to CSV
2. **Data Transformation**: 
   - Unwrangling JSON responses into separate columns
   - Filtering to business-relevant fields
   - Field renaming and data type standardization
   - Normalizing into dimension and fact tables
   - Denormalizing for downstream analytics
3. **Orchestration**: Building an Airflow pipeline to automate the entire process
4. **Storage**: Saving processed data as Parquet files

## Next Steps
The following enhancements are planned:
1. Replace CSV storage with Snowflake or BigQuery integration
2. Migrate Airflow pipeline to cloud services like Astronomer
3. Connect the data warehouse to BI tools like Looker
4. Expand data sources to include Calendly and social media platforms for comprehensive funnel analysis

## Technical Skills Demonstrated
- Airflow configuration and pipeline orchestration
- PySpark data processing
- JSON parsing and transformation
- Data modeling (normalization and denormalization)
- ETL pipeline design and implementation