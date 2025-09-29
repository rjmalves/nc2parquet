# nc2parquet

A high-performance Rust library and CLI tool for converting NetCDF files to Parquet format with advanced filtering, cloud storage, and post-processing capabilities.

## Features

🚀 **High Performance**: Built in Rust with efficient processing of large NetCDF datasets  
🔄 **Advanced Filtering**: Multiple filter types with intersection logic for precise data extraction  
☁️ **Cloud Storage**: Native Amazon S3 support for input and output files with async operations  
📊 **Multiple Filter Types**: Range, list, 2D point, and 3D point filters with spatial/temporal support  
🔧 **Multi-Source Configuration**: CLI arguments, environment variables, and JSON/YAML configuration files  
🛠️ **Post-Processing Framework**: Built-in DataFrame transformations including column renaming, unit conversion, and formula application  
🖥️ **Professional CLI**: Comprehensive command-line interface with progress indicators, logging, and shell completions  
🧪 **Well Tested**: Comprehensive test suite with 80+ tests covering all features

## Installation

### Command-Line Tool

```bash
# Install from source
cargo install --path .

# Or install from crates.io (when published)
cargo install nc2parquet
```

### Library Dependency

Add to your `Cargo.toml`:

```toml
[dependencies]
nc2parquet = "0.1.0"
```

## Quick Start

### Command-Line Interface

The CLI provides comprehensive functionality with multiple subcommands:

```bash
# Basic conversion
nc2parquet convert input.nc output.parquet --variable temperature

# S3 to S3 conversion
nc2parquet convert s3://input-bucket/data.nc s3://output-bucket/result.parquet --variable pressure

# Conversion with filtering
nc2parquet convert data.nc result.parquet \
  --variable temperature \
  --range "latitude:30:60" \
  --list "pressure:1000,850,500"

# Conversion with post-processing
nc2parquet convert data.nc result.parquet \
  --variable temperature \
  --rename "temperature:temp_k" \
  --kelvin-to-celsius temp_k \
  --formula "temp_f:temp_k*1.8+32"

# Generate configuration templates
nc2parquet template basic -o config.json
nc2parquet template s3 --format yaml -o s3-config.yaml

# Validate configurations
nc2parquet validate config.json --detailed

# File information and inspection
nc2parquet info data.nc                           # Basic file info (human-readable)
nc2parquet info data.nc --detailed                # Include global attributes
nc2parquet info data.nc --variable temperature    # Show specific variable info
nc2parquet info data.nc --format json             # JSON output for scripting
nc2parquet info data.nc --format yaml             # YAML output
nc2parquet info data.nc --format csv              # CSV output (variables table)
nc2parquet info s3://bucket/data.nc --detailed    # Works with S3 files too

# Generate shell completions
nc2parquet completions bash > ~/.bash_completion.d/nc2parquet
```

### Library Usage

**Basic Conversion:**

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

**S3 and Post-Processing:**

```rust
use nc2parquet::{JobConfig, process_netcdf_job_async};
use nc2parquet::postprocess::*;
use std::collections::HashMap;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = JobConfig {
        nc_key: "s3://my-bucket/weather-data.nc".to_string(),
        variable_name: "temperature".to_string(),
        parquet_key: "s3://output-bucket/processed-temp.parquet".to_string(),
        filters: vec![
            // Spatial filter for North America
            FilterConfig::Range {
                params: RangeParams {
                    dimension_name: "latitude".to_string(),
                    min_value: 25.0,
                    max_value: 70.0,
                }
            },
            FilterConfig::Range {
                params: RangeParams {
                    dimension_name: "longitude".to_string(),
                    min_value: -140.0,
                    max_value: -60.0,
                }
            },
        ],
        postprocessing: Some(ProcessingPipelineConfig {
            name: Some("Weather Data Processing".to_string()),
            processors: vec![
                // Rename columns
                ProcessorConfig::RenameColumns {
                    mappings: {
                        let mut map = HashMap::new();
                        map.insert("temperature".to_string(), "temp_kelvin".to_string());
                        map
                    },
                },
                // Convert Kelvin to Celsius
                ProcessorConfig::UnitConvert {
                    column: "temp_kelvin".to_string(),
                    from_unit: "kelvin".to_string(),
                    to_unit: "celsius".to_string(),
                },
                // Add computed column
                ProcessorConfig::ApplyFormula {
                    target_column: "temp_fahrenheit".to_string(),
                    formula: "temp_kelvin * 1.8 - 459.67".to_string(),
                    source_columns: vec!["temp_kelvin".to_string()],
                },
            ],
        }),
    };

    process_netcdf_job_async(&config).await?;
    Ok(())
}
```

## File Information and Inspection

The `info` subcommand provides comprehensive NetCDF file analysis capabilities:

### Basic Usage

```bash
# Display file structure and metadata
nc2parquet info temperature_data.nc
```

**Output:**

```
NetCDF File Information:
  Path: temperature_data.nc
  File Size: 2.73 MB
  Dimensions: 4 total
    level (2)
    latitude (6)
    longitude (12)
    time (2, unlimited)
  Variables: 4 total
    latitude (Float(F32)) - dimensions: [latitude]
      @units: Str("degrees_north")
    longitude (Float(F32)) - dimensions: [longitude]
      @units: Str("degrees_east")
    pressure (Float(F32)) - dimensions: [time, level, latitude, longitude]
      @units: Str("hPa")
    temperature (Float(F32)) - dimensions: [time, level, latitude, longitude]
      @units: Str("celsius")
```

### Advanced Features

**Detailed Information:**

```bash
# Include global attributes and extended metadata
nc2parquet info data.nc --detailed
```

**Variable-Specific Analysis:**

```bash
# Focus on a specific variable
nc2parquet info ocean_data.nc --variable sea_surface_temperature
```

**Multiple Output Formats:**

```bash
# JSON format for programmatic use
nc2parquet info data.nc --format json > file_info.json

# YAML format for human-readable structured output
nc2parquet info data.nc --format yaml

# CSV format for variable analysis (tabular data)
nc2parquet info data.nc --format csv > variables.csv
```

**Cloud Storage Support:**

```bash
# Analyze S3-hosted NetCDF files directly
nc2parquet info s3://climate-data/global_temperature.nc --detailed
```

### JSON Output Structure

The JSON output provides a complete machine-readable representation:

```json
{
  "path": "temperature_data.nc",
  "file_size": 2784,
  "total_dimensions": 4,
  "total_variables": 4,
  "dimensions": [
    {
      "name": "level",
      "length": 2,
      "is_unlimited": false
    },
    {
      "name": "time",
      "length": 2,
      "is_unlimited": true
    }
  ],
  "variables": [
    {
      "name": "temperature",
      "data_type": "Float(F32)",
      "dimensions": ["time", "level", "latitude", "longitude"],
      "shape": [2, 2, 6, 12],
      "attributes": {
        "units": "Str(\"celsius\")"
      }
    }
  ],
  "global_attributes": {}
}
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
    "points": [
      [40.7, -74.0],
      [51.5, -0.1]
    ],
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
    "points": [
      [40.7, -74.0],
      [51.5, -0.1]
    ],
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
          [40.7128, -74.006],
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

## Configuration Sources

nc2parquet supports multiple configuration sources with clear precedence:

**Priority (highest to lowest):**

1. CLI arguments
2. Environment variables
3. Configuration files

### Environment Variables

All CLI options can be set via environment variables with the `NC2PARQUET_` prefix:

```bash
# Core configuration
export NC2PARQUET_INPUT="s3://my-bucket/data.nc"
export NC2PARQUET_OUTPUT="s3://output-bucket/result.parquet"
export NC2PARQUET_VARIABLE="temperature"
export NC2PARQUET_CONFIG="/path/to/config.json"

# Processing options
export NC2PARQUET_FORCE=true
export NC2PARQUET_DRY_RUN=true

# Filter configuration
export NC2PARQUET_RANGE_FILTERS="lat:30:60,lon:-120:-80"
export NC2PARQUET_LIST_FILTERS="pressure:1000,850,500;level:1,2,3"
export NC2PARQUET_POINT2D_FILTERS="lat,lon:40.7,-74.0:0.5"
export NC2PARQUET_POINT3D_FILTERS="time,lat,lon:0.0,40.7,-74.0:0.1"

# Override paths for specific scenarios
export NC2PARQUET_INPUT_OVERRIDE="/alternative/input.nc"
export NC2PARQUET_OUTPUT_OVERRIDE="/alternative/output.parquet"
```

### Configuration Files

Support both JSON and YAML formats with automatic detection:

```bash
nc2parquet convert --config config.json
nc2parquet convert --config config.yaml
```

## Post-Processing Framework

Transform DataFrames after extraction with built-in processors:

### Available Processors

1. **Column Renaming**

   ```bash
   --rename "old_name:new_name,temperature:temp_k"
   ```

2. **Unit Conversion**

   ```bash
   --unit-convert "temperature:kelvin:celsius"
   --kelvin-to-celsius temperature  # Shortcut for Kelvin→Celsius
   ```

3. **Formula Application**

   ```bash
   --formula "temp_f:temp_c * 1.8 + 32"
   --formula "heat_index:temp + humidity * 0.1"
   ```

4. **DateTime Conversion** (configuration only)
5. **Data Aggregation** (configuration only)

### Post-Processing Configuration

```json
{
  "nc_key": "weather.nc",
  "variable_name": "temperature",
  "parquet_key": "processed.parquet",
  "postprocessing": {
    "name": "Weather Data Pipeline",
    "processors": [
      {
        "type": "RenameColumns",
        "mappings": {
          "temperature": "temp_k",
          "lat": "latitude",
          "lon": "longitude"
        }
      },
      {
        "type": "UnitConvert",
        "column": "temp_k",
        "from_unit": "kelvin",
        "to_unit": "celsius"
      },
      {
        "type": "ApplyFormula",
        "target_column": "temp_fahrenheit",
        "formula": "temp_k * 1.8 - 459.67",
        "source_columns": ["temp_k"]
      },
      {
        "type": "DatetimeConvert",
        "column": "time",
        "base": "2000-01-01T00:00:00Z",
        "unit": "hours"
      }
    ]
  }
}
```

### Pipeline Chaining

Processors are executed sequentially, allowing complex transformations:

```bash
nc2parquet convert weather.nc result.parquet \
  --variable temperature \
  --rename "temperature:temp_k,lat:latitude" \
  --kelvin-to-celsius temp_k \
  --formula "temp_f:temp_k*1.8+32" \
  --formula "heat_index:temp_k+humidity*0.05"
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
