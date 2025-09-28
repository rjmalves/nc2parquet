#!/bin/bash
# nc2parquet CLI Examples
# 
# This script demonstrates various ways to use the nc2parquet command-line interface.
# Run these examples to see the different features in action.

echo "=== NC2PARQUET CLI EXAMPLES ==="

# Basic conversion
echo "1. Basic conversion (local files)"
nc2parquet convert examples/data/simple_xy.nc output/basic.parquet --variable data

# S3 to S3 conversion (requires AWS credentials)
echo "2. S3 to S3 conversion" 
# nc2parquet convert s3://input-bucket/data.nc s3://output-bucket/result.parquet --variable temperature

# Conversion with filtering
echo "3. Conversion with range and list filters"
nc2parquet convert examples/data/pres_temp_4D.nc output/filtered.parquet \
  --variable temperature \
  --range "latitude:25:45" \
  --list "level:850,700,500"

# Post-processing with CLI arguments
echo "4. Post-processing with column renaming and unit conversion"
nc2parquet convert examples/data/simple_xy.nc output/processed.parquet \
  --variable data \
  --rename "data:temperature,x:longitude,y:latitude" \
  --kelvin-to-celsius temperature \
  --formula "temp_f:temperature*1.8+32"

# Using environment variables
echo "5. Using environment variables for configuration"
export NC2PARQUET_INPUT="examples/data/simple_xy.nc"
export NC2PARQUET_OUTPUT="output/env_example.parquet"  
export NC2PARQUET_VARIABLE="data"
export NC2PARQUET_RANGE_FILTERS="x:1:4"
nc2parquet convert --rename "data:measurement"

# Configuration file processing
echo "6. Using configuration files"
nc2parquet convert --config examples/postprocessing/complex_pipeline.json

# File information and validation
echo "7. Getting file information"
nc2parquet info examples/data/simple_xy.nc --format json --detailed

echo "8. Configuration validation"
nc2parquet validate examples/postprocessing/weather_analysis.json --detailed

# Template generation 
echo "9. Generating configuration templates"
nc2parquet template basic -o output/basic_template.json
nc2parquet template s3 --format yaml -o output/s3_template.yaml
nc2parquet template multi-filter -o output/multi_filter_template.json

# Shell completions
echo "10. Generate shell completions"
nc2parquet completions bash > output/nc2parquet_completions.bash
echo "Source the completions file: source output/nc2parquet_completions.bash"

# Complex real-world example
echo "11. Complex real-world processing"
nc2parquet convert examples/data/pres_temp_4D.nc output/weather_analysis.parquet \
  --variable temperature \
  --range "latitude:30:50,longitude:-120:-80" \
  --list "level:1000,850,500" \
  --rename "temperature:temp_k,latitude:lat,longitude:lon" \
  --kelvin-to-celsius temp_k \
  --formula "temp_f:temp_k*1.8+32" \
  --formula "is_freezing:temp_k<0" \
  --verbose

# Dry run mode
echo "12. Dry run (preview without execution)"
nc2parquet convert examples/data/simple_xy.nc output/dry_run.parquet \
  --variable data \
  --dry-run \
  --verbose

echo "=== Examples completed! Check the output/ directory for results ==="