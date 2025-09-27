# nc2parquet

A high-performance Rust library and tool for converting NetCDF files to Parquet format with advanced filtering capabilities and cloud storage support.

## Features

🚀 **High Performance**: Built in Rust with efficient processing of large NetCDF datasets  
🔄 **Advanced Filtering**: Multiple filter types with intersection logic for precise data extraction  
☁️ **Cloud Storage**: Native Amazon S3 support for input and output files  
📊 **Multiple Filter Types**: Range, list, 2D point, and 3D point filters  
🔧 **Flexible Configuration**: JSON-based configuration with comprehensive options  
🧪 **Well Tested**: Comprehensive test suite with real NetCDF test data  

## Quick Start

### Installation

Add nc2parquet to your Cargo.toml:

```toml
[dependencies]
nc2parquet = "0.1.0"
```

Or install as a command-line tool:

```bash
cargo install nc2parquet
```

### Basic Usage

**Library Usage:**

```rust
use nc2parquet::{JobConfig, process_netcdf_job_async};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = JobConfig::from_json(r#"
    {
        "nc_key": "data/temperature.nc",
        "variable_name": "temperature",
        "parquet_key": "output/temperature.parquet",
        "filters": [
            {
                "kind": "range",
                "params": {
                    "dimension_name": "latitude",
                    "min_value": 30.0,
                    "max_value": 60.0
                }
            }
        ]
    }
    "#)?;
    
    process_netcdf_job_async(&config).await?;
    Ok(())
}
```

**Command Line Usage:**

```bash
# Convert with local files
nc2parquet config.json

# Convert with S3 input and local output
nc2parquet s3://my-bucket/data.nc output.parquet

# Convert with S3 input and S3 output
nc2parquet s3://input-bucket/data.nc s3://output-bucket/result.parquet
```

## Storage Support

nc2parquet supports both local filesystem and Amazon S3 storage:

### Local Files
```json
{
    "nc_key": "/path/to/input.nc",
    "parquet_key": "/path/to/output.parquet"
}
```

### Amazon S3
```json
{
    "nc_key": "s3://my-bucket/path/to/input.nc",
    "parquet_key": "s3://my-bucket/path/to/output.parquet"
}
```

### Mixed Storage
```json
{
    "nc_key": "s3://input-bucket/data.nc",
    "parquet_key": "/local/path/output.parquet"
}
```

## AWS Configuration

For S3 support, configure AWS credentials using any of these methods:

### Environment Variables
```bash
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
export AWS_DEFAULT_REGION=us-east-1
```

### AWS Credentials File
```ini
# ~/.aws/credentials
[default]
aws_access_key_id = your_access_key
aws_secret_access_key = your_secret_key

# ~/.aws/config
[default]
region = us-east-1
```

### IAM Roles
When running on AWS infrastructure (EC2, Lambda, ECS), IAM roles are automatically used.

## Filter Types

nc2parquet supports four types of filters that can be combined for precise data extraction:

### 1. Range Filter
Selects values within a numeric range:

```json
{
    "kind": "range",
    "params": {
        "dimension_name": "temperature",
        "min_value": -10.0,
        "max_value": 35.0
    }
}
```

### 2. List Filter
Selects specific discrete values:

```json
{
    "kind": "list",
    "params": {
        "dimension_name": "pressure_level",
        "values": [850.0, 500.0, 200.0]
    }
}
```

### 3. 2D Point Filter
Selects spatial coordinates with tolerance:

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

### 4. 3D Point Filter
Selects spatiotemporal coordinates:

```json
{
    "kind": "3d_point",
    "params": {
        "time_dimension_name": "time",
        "lat_dimension_name": "latitude",
        "lon_dimension_name": "longitude",
        "steps": [0.0, 6.0, 12.0],
        "points": [[40.7, -74.0], [51.5, -0.1]],
        "tolerance": 0.1
    }
}
```

## Configuration Examples

### Simple Weather Data Extraction
```json
{
    "nc_key": "weather_data.nc",
    "variable_name": "temperature",
    "parquet_key": "temperature_filtered.parquet",
    "filters": [
        {
            "kind": "range",
            "params": {
                "dimension_name": "latitude",
                "min_value": 30.0,
                "max_value": 45.0
            }
        }
    ]
}
```

### Multi-Filter Climate Analysis
```json
{
    "nc_key": "s3://climate-data/global_temps.nc",
    "variable_name": "temperature",
    "parquet_key": "s3://results/urban_temps.parquet",
    "filters": [
        {
            "kind": "range",
            "params": {
                "dimension_name": "time",
                "min_value": 20200101.0,
                "max_value": 20231231.0
            }
        },
        {
            "kind": "2d_point",
            "params": {
                "lat_dimension_name": "latitude",
                "lon_dimension_name": "longitude",
                "points": [
                    [40.7128, -74.0060], 
                    [34.0522, -118.2437],
                    [41.8781, -87.6298]
                ],
                "tolerance": 0.5
            }
        }
    ]
}
```

### Ocean Data Processing
```json
{
    "nc_key": "s3://ocean-data/sst_2023.nc",
    "variable_name": "sea_surface_temperature", 
    "parquet_key": "atlantic_sst.parquet",
    "filters": [
        {
            "kind": "range",
            "params": {
                "dimension_name": "longitude",
                "min_value": -80.0,
                "max_value": -10.0
            }
        },
        {
            "kind": "range", 
            "params": {
                "dimension_name": "latitude",
                "min_value": 0.0,
                "max_value": 70.0
            }
        },
        {
            "kind": "list",
            "params": {
                "dimension_name": "depth",
                "values": [0.0, 5.0, 10.0]
            }
        }
    ]
}
```

## Performance Tips

1. **Use S3 Transfer Acceleration** for faster uploads to S3
2. **Apply filters early** to reduce data transfer and processing time
3. **Use specific coordinates** rather than large ranges when possible
4. **Consider data locality** - process data in the same AWS region as your S3 buckets

## Error Handling

The library provides detailed error messages for common issues:

- **File not found**: Clear indication of missing input files (local or S3)
- **Invalid NetCDF**: Detailed validation errors for malformed files
- **Permission errors**: Specific AWS permission or filesystem access issues
- **Configuration errors**: JSON parsing and validation errors with context

## Testing

Run the full test suite:

```bash
cargo test
```

Run only local tests (no AWS required):

```bash
cargo test --lib
```

Run S3 integration tests (requires AWS credentials and TEST_S3_BUCKET environment variable):

```bash
export TEST_S3_BUCKET=your-test-bucket
cargo test test_end_to_end_s3_pipeline -- --ignored
```

## Architecture

nc2parquet uses a modular architecture:

- **Storage Layer**: Unified interface for local and S3 operations
- **Filter System**: Composable filters with intersection logic  
- **Processing Pipeline**: Efficient async processing with minimal memory usage
- **Configuration**: Type-safe JSON configuration with validation

## Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/amazing-feature`
3. Make your changes and add tests
4. Ensure tests pass: `cargo test`
5. Submit a pull request

## Roadmap

- **Sprint 2**: CLI application with argument parsing and configuration management
- **Sprint 3**: Advanced post-processing with data aggregation and statistics
- **Sprint 4**: Performance optimizations and streaming processing
- **Sprint 5**: Extended cloud support (GCS, Azure Blob Storage)
- **Sprint 6**: Monitoring and logging improvements
- **Sprint 7**: Advanced filtering and data transformation capabilities

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Examples

The `examples/` directory contains sample NetCDF files and configuration examples:

- `examples/data/simple_xy.nc`: Simple 2D test data
- `examples/data/pres_temp_4D.nc`: 4D weather data with time series
- `examples/configs/`: Sample configuration files for various use cases
