# nc2parquet Configuration Examples

This directory contains example configuration files demonstrating various use cases for nc2parquet.

## Files

### `simple_local.json`
Basic local file processing with a single range filter.
- **Input**: Local NetCDF file (simple_xy.nc)
- **Output**: Local Parquet file
- **Filters**: Range filter on x dimension

**Usage:**
```bash
nc2parquet examples/configs/simple_local.json
```

### `s3_weather.json`
S3-to-S3 processing for weather data with geographical filtering.
- **Input**: S3 NetCDF file
- **Output**: S3 Parquet file
- **Filters**: Latitude and longitude range filters

**Usage:**
```bash
# Requires AWS credentials configured
nc2parquet examples/configs/s3_weather.json
```

### `multi_filter.json`
Complex filtering with multiple filter types working together.
- **Input**: Local NetCDF file (4D weather data)
- **Output**: Local Parquet file
- **Filters**: Range, list, and 2D point filters combined

**Usage:**
```bash
nc2parquet examples/configs/multi_filter.json
```

### `mixed_storage_ocean.json`
Mixed storage example: S3 input to local output for ocean data.
- **Input**: S3 NetCDF file (ocean temperature)
- **Output**: Local Parquet file
- **Filters**: Geographic ranges + 3D spatiotemporal points

**Usage:**
```bash
# Requires AWS credentials for S3 input
nc2parquet examples/configs/mixed_storage_ocean.json
```

## Running Examples

1. **Local examples** (simple_local.json, multi_filter.json):
   ```bash
   # Ensure you have the test data
   ls examples/data/
   
   # Run local processing
   cargo run examples/configs/simple_local.json
   ```

2. **S3 examples** (s3_weather.json, mixed_storage_ocean.json):
   ```bash
   # Configure AWS credentials first
   export AWS_ACCESS_KEY_ID=your_key
   export AWS_SECRET_ACCESS_KEY=your_secret
   export AWS_DEFAULT_REGION=us-east-1
   
   # Update bucket names in config files to your buckets
   # Then run
   cargo run examples/configs/s3_weather.json
   ```

## Modifying Examples

Feel free to modify these configuration files:

- **Change paths**: Update nc_key and parquet_key to your files
- **Adjust filters**: Modify ranges, add/remove filter types
- **Test different variables**: Change variable_name to other NetCDF variables

## Common Patterns

### Geographic Filtering
```json
{
    "kind": "range", 
    "params": {
        "dimension_name": "latitude",
        "min_value": 30.0,
        "max_value": 60.0
    }
}
```

### Time Series Extraction  
```json
{
    "kind": "list",
    "params": {
        "dimension_name": "time",
        "values": [0.0, 6.0, 12.0, 18.0]
    }
}
```

### Point-based Sampling
```json
{
    "kind": "2d_point",
    "params": {
        "lat_dimension_name": "latitude",
        "lon_dimension_name": "longitude", 
        "points": [[40.7, -74.0], [51.5, -0.1]],
        "tolerance": 0.1
    }
}
```