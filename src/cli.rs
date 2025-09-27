//! # CLI Module
//! 
//! This module provides the command-line interface for nc2parquet, including:
//! - Argument parsing with clap
//! - Configuration file loading (JSON/YAML)
//! - Environment variable support
//! - Subcommands for different operations
//! - Progress reporting and logging

use clap::{Parser, Subcommand, ValueEnum};
use clap_complete::Shell;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use crate::input::{JobConfig, FilterConfig};

/// High-performance NetCDF to Parquet converter with cloud storage support
#[derive(Parser, Debug)]
#[command(name = "nc2parquet")]
#[command(about = "Convert NetCDF files to Parquet format with advanced filtering")]
#[command(version)]
#[command(author = "Rogerio Malves <rjmalves@users.noreply.github.com>")]
#[command(long_about = "
nc2parquet is a high-performance command-line tool for converting NetCDF files to Parquet format.
It supports advanced filtering capabilities, cloud storage (S3), and provides comprehensive 
configuration management.

FEATURES:
  • Multiple filter types: Range, list, 2D point, and 3D point filters
  • Cloud storage support: Direct S3 input/output with authentication
  • Configuration files: JSON and YAML format support with templates
  • Progress indicators: Real-time progress bars and performance metrics
  • Validation: Comprehensive configuration and data validation
  • Shell completions: Auto-completion for bash, zsh, fish, and PowerShell

EXAMPLES:
  # Basic conversion
  nc2parquet convert input.nc output.parquet -n temperature

  # With filters  
  nc2parquet convert data.nc filtered.parquet -n temp \\
    --range 'latitude:30:60' --list 'level:1000,850,500'

  # S3 support
  nc2parquet convert s3://bucket/input.nc s3://bucket/output.parquet -n sst

  # Using config file
  nc2parquet convert --config weather.json

  # Generate templates
  nc2parquet template multi-filter --format yaml > config.yaml

  # File inspection
  nc2parquet info data.nc --detailed

  # Generate completions
  nc2parquet completions bash > ~/.bash_completion.d/nc2parquet

For more information and examples, see: https://github.com/rjmalves/nc2parquet
")]
pub struct Cli {
    /// Enable verbose logging
    #[arg(short, long, global = true)]
    pub verbose: bool,
    
    /// Quiet mode - suppress all output except errors
    #[arg(short, long, global = true, conflicts_with = "verbose")]
    pub quiet: bool,
    
    /// Output format for structured data
    #[arg(long, global = true, value_enum, default_value_t = OutputFormat::Human)]
    pub output_format: OutputFormat,
    
    /// Configuration file path (JSON or YAML)
    #[arg(short, long, global = true, env = "NC2PARQUET_CONFIG")]
    pub config: Option<PathBuf>,
    
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Convert NetCDF files to Parquet format
    #[command(long_about = "
Convert NetCDF files to Parquet format with optional filtering.

This command supports both local files and S3 objects as input/output.
Filters can be specified via command-line arguments or configuration files.

EXAMPLES:
  # Basic conversion
  nc2parquet convert input.nc output.parquet -n temperature

  # With multiple filters
  nc2parquet convert weather.nc filtered.parquet -n temp \\
    --range 'time:0:365' --range 'latitude:30:60' \\
    --list 'pressure:1000,850,500'

  # S3 to S3 conversion
  nc2parquet convert s3://data/input.nc s3://results/output.parquet -n sst

  # Dry run for validation
  nc2parquet convert input.nc output.parquet -n temp --dry-run

  # Using config file with overrides
  nc2parquet convert --config base.json \\
    --input-override new_input.nc --output-override new_output.parquet
")]
    Convert {
        /// Input NetCDF file path (local or S3)
        #[arg(value_name = "INPUT")]
        input: Option<String>,
        
        /// Output Parquet file path (local or S3)
        #[arg(value_name = "OUTPUT")]
        output: Option<String>,
        
        /// NetCDF variable name to extract
        #[arg(short = 'n', long, env = "NC2PARQUET_VARIABLE")]
        variable: Option<String>,
        
        /// Override input path from config
        #[arg(long)]
        input_override: Option<String>,
        
        /// Override output path from config
        #[arg(long)]
        output_override: Option<String>,
        
        /// Apply range filter: dimension:min:max
        #[arg(long = "range", value_parser = parse_range_filter)]
        range_filters: Vec<RangeFilterArg>,
        
        /// Apply list filter: dimension:val1,val2,val3
        #[arg(long = "list", value_parser = parse_list_filter)]
        list_filters: Vec<ListFilterArg>,
        
        /// Force overwrite existing output files
        #[arg(long)]
        force: bool,
        
        /// Dry run - validate configuration without processing
        #[arg(long)]
        dry_run: bool,
    },
    
    /// Validate configuration file or arguments
    #[command(long_about = "
Validate configuration files and command-line arguments without processing.

This command performs comprehensive validation including:
• Configuration file syntax and structure
• Filter parameter validation  
• File existence checks (for local files)
• S3 path format validation
• Environment variable validation

EXAMPLES:
  # Validate a configuration file
  nc2parquet validate config.json

  # Validate with detailed output
  nc2parquet validate config.yaml --detailed

  # Validate using global config
  nc2parquet validate --config ~/.nc2parquet.json
")]
    Validate {
        /// Configuration file to validate
        config_file: Option<PathBuf>,
        
        /// Show detailed validation report
        #[arg(long)]
        detailed: bool,
    },
    
    /// Show information about NetCDF file
    #[command(long_about = "
Inspect NetCDF files and display structure information.

This command analyzes NetCDF files (local or S3) and displays:
• File dimensions and their sizes
• Available variables and their attributes
• Variable-specific information (when specified)
• Coordinate information and metadata

EXAMPLES:
  # Basic file info
  nc2parquet info data.nc

  # Detailed information
  nc2parquet info weather.nc --detailed

  # Info about specific variable
  nc2parquet info ocean.nc -n sea_surface_temperature

  # JSON output for scripting
  nc2parquet info data.nc --format json

  # S3 file inspection
  nc2parquet info s3://bucket/data.nc --detailed
")]
    Info {
        /// NetCDF file path (local or S3)
        file: String,
        
        /// Show detailed variable information
        #[arg(long)]
        detailed: bool,
        
        /// Show only specific variable info
        #[arg(short = 'n', long)]
        variable: Option<String>,
        
        /// Output format for file information
        #[arg(long, value_enum)]
        format: Option<OutputFormat>,
    },
    
    /// Generate configuration templates
    #[command(long_about = "
Generate configuration file templates for common use cases.

Available templates:
• basic: Simple conversion template
• s3: S3 storage template with authentication
• multi-filter: Complex filtering examples
• weather: Weather data processing template  
• ocean: Ocean/marine data template

Templates can be generated in JSON or YAML format and saved to files
or printed to stdout for piping to other commands.

EXAMPLES:
  # Generate basic JSON template
  nc2parquet template basic

  # Generate YAML template to file
  nc2parquet template s3 --format yaml -o s3_config.yaml

  # Generate multi-filter example
  nc2parquet template multi-filter --format yaml

  # Generate and edit template
  nc2parquet template weather > weather.json
  # ... edit weather.json ...
  nc2parquet convert --config weather.json
")]
    Template {
        /// Template type to generate
        #[arg(value_enum)]
        template_type: TemplateType,
        
        /// Output file path (default: stdout)
        #[arg(short, long)]
        output: Option<PathBuf>,
        
        /// Configuration format
        #[arg(long, value_enum, default_value_t = ConfigFormat::Json)]
        format: ConfigFormat,
    },
    
    /// Generate shell completions
    #[command(long_about = "
Generate shell completion scripts for various shells.

Supports bash, zsh, fish, and PowerShell completion generation.
Completions provide auto-completion for all commands, options, and values.

INSTALLATION:
  # Bash (add to ~/.bashrc or /etc/bash_completion.d/)
  nc2parquet completions bash > ~/.bash_completion.d/nc2parquet
  source ~/.bashrc

  # Zsh (add to ~/.zshrc or fpath)
  nc2parquet completions zsh > ~/.zsh/completions/_nc2parquet
  # Add to ~/.zshrc: fpath=(~/.zsh/completions $fpath)

  # Fish (save to completions directory)
  nc2parquet completions fish > ~/.config/fish/completions/nc2parquet.fish

  # PowerShell (add to profile)
  nc2parquet completions powershell > nc2parquet.ps1

EXAMPLES:
  # Generate bash completions
  nc2parquet completions bash

  # Save zsh completions to file
  nc2parquet completions zsh -o _nc2parquet

  # Test completions work
  nc2parquet <TAB><TAB>
")]
    Completions {
        /// Shell to generate completions for
        #[arg(value_enum)]
        shell: Shell,
        
        /// Output file path (default: stdout)
        #[arg(short, long)]
        output: Option<PathBuf>,
    },
}

#[derive(ValueEnum, Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum OutputFormat {
    /// Human-readable output
    Human,
    /// JSON structured output
    Json,
    /// YAML structured output  
    Yaml,
    /// CSV output (where applicable)
    Csv,
}

#[derive(ValueEnum, Clone, Debug, PartialEq, Eq)]
pub enum TemplateType {
    /// Basic conversion template
    Basic,
    /// S3 storage template
    S3,
    /// Multi-filter template
    MultiFilter,
    /// Weather data template
    Weather,
    /// Ocean data template
    Ocean,
}

#[derive(ValueEnum, Clone, Debug, PartialEq, Eq)]
pub enum ConfigFormat {
    /// JSON configuration format
    Json,
    /// YAML configuration format
    Yaml,
}

/// Range filter argument from command line
#[derive(Clone, Debug, PartialEq)]
pub struct RangeFilterArg {
    pub dimension: String,
    pub min_value: f64,
    pub max_value: f64,
}

/// List filter argument from command line
#[derive(Clone, Debug, PartialEq)]
pub struct ListFilterArg {
    pub dimension: String,
    pub values: Vec<f64>,
}

/// Extended configuration that includes CLI-specific options
#[derive(Deserialize, Serialize, Clone)]
pub struct CliConfig {
    #[serde(flatten)]
    pub job: JobConfig,
    
    /// CLI-specific options
    #[serde(default)]
    pub cli_options: CliOptions,
}

#[derive(Deserialize, Serialize, Clone, Default)]
pub struct CliOptions {
    /// Default log level
    pub log_level: Option<String>,
    
    /// Progress reporting settings
    pub progress: Option<ProgressConfig>,
    
    /// Output formatting preferences
    pub output_format: Option<OutputFormat>,
    
    /// Validation settings
    pub validation: Option<ValidationConfig>,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct ProgressConfig {
    /// Enable progress bars
    pub enabled: bool,
    
    /// Progress update interval in seconds
    pub interval: Option<u64>,
    
    /// Progress bar style
    pub style: Option<String>,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct ValidationConfig {
    /// Strict validation mode
    pub strict: bool,
    
    /// Validate S3 paths
    pub check_s3_paths: bool,
    
    /// Validate NetCDF file accessibility  
    pub check_file_access: bool,
}

/// Parse range filter from command line argument
/// Format: dimension:min:max
fn parse_range_filter(s: &str) -> Result<RangeFilterArg, String> {
    let parts: Vec<&str> = s.split(':').collect();
    if parts.len() != 3 {
        return Err("Range filter must be in format 'dimension:min:max'".to_string());
    }
    
    let dimension = parts[0].to_string();
    let min_value = parts[1].parse::<f64>()
        .map_err(|_| "Invalid minimum value in range filter")?;
    let max_value = parts[2].parse::<f64>()
        .map_err(|_| "Invalid maximum value in range filter")?;
        
    if min_value >= max_value {
        return Err("Minimum value must be less than maximum value".to_string());
    }
    
    Ok(RangeFilterArg {
        dimension,
        min_value,
        max_value,
    })
}

/// Parse list filter from command line argument
/// Format: dimension:val1,val2,val3
fn parse_list_filter(s: &str) -> Result<ListFilterArg, String> {
    let parts: Vec<&str> = s.split(':').collect();
    if parts.len() != 2 {
        return Err("List filter must be in format 'dimension:val1,val2,val3'".to_string());
    }
    
    let dimension = parts[0].to_string();
    let values: Result<Vec<f64>, _> = parts[1]
        .split(',')
        .map(|v| v.trim().parse::<f64>())
        .collect();
        
    let values = values.map_err(|_| "Invalid numeric values in list filter")?;
    
    if values.is_empty() {
        return Err("List filter must contain at least one value".to_string());
    }
    
    Ok(ListFilterArg {
        dimension,
        values,
    })
}

impl From<RangeFilterArg> for FilterConfig {
    fn from(arg: RangeFilterArg) -> Self {
        FilterConfig::Range {
            params: crate::input::RangeParams {
                dimension_name: arg.dimension,
                min_value: arg.min_value,
                max_value: arg.max_value,
            }
        }
    }
}

impl From<ListFilterArg> for FilterConfig {
    fn from(arg: ListFilterArg) -> Self {
        FilterConfig::List {
            params: crate::input::ListParams {
                dimension_name: arg.dimension,
                values: arg.values,
            }
        }
    }
}

impl Default for ProgressConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            interval: Some(1),
            style: Some("█▉▊▋▌▍▎▏  ".to_string()),
        }
    }
}

impl Default for ValidationConfig {
    fn default() -> Self {
        Self {
            strict: false,
            check_s3_paths: true,
            check_file_access: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_parse_range_filter() {
        let result = parse_range_filter("latitude:30.0:60.0").unwrap();
        assert_eq!(result.dimension, "latitude");
        assert_eq!(result.min_value, 30.0);
        assert_eq!(result.max_value, 60.0);
        
        // Test invalid formats
        assert!(parse_range_filter("latitude:30.0").is_err());
        assert!(parse_range_filter("latitude:30.0:60.0:extra").is_err());
        assert!(parse_range_filter("latitude:invalid:60.0").is_err());
        assert!(parse_range_filter("latitude:60.0:30.0").is_err()); // min > max
    }
    
    #[test]
    fn test_parse_list_filter() {
        let result = parse_list_filter("pressure:850.0,500.0,200.0").unwrap();
        assert_eq!(result.dimension, "pressure");
        assert_eq!(result.values, vec![850.0, 500.0, 200.0]);
        
        // Test single value
        let result = parse_list_filter("time:0.0").unwrap();
        assert_eq!(result.values, vec![0.0]);
        
        // Test invalid formats
        assert!(parse_list_filter("pressure:850.0,invalid,200.0").is_err());
        assert!(parse_list_filter("pressure:").is_err());
        assert!(parse_list_filter("pressure").is_err());
    }
    
    #[test]
    fn test_filter_conversion() {
        let range_arg = RangeFilterArg {
            dimension: "lat".to_string(),
            min_value: 10.0,
            max_value: 50.0,
        };
        
        let filter_config: FilterConfig = range_arg.into();
        if let FilterConfig::Range { params } = filter_config {
            assert_eq!(params.dimension_name, "lat");
            assert_eq!(params.min_value, 10.0);
            assert_eq!(params.max_value, 50.0);
        } else {
            panic!("Expected Range filter config");
        }
    }
}