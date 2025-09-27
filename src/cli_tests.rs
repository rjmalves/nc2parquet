//! # CLI Integration Tests
//! 
//! This module provides comprehensive tests for the command-line interface,
//! including argument parsing, command execution, and error handling scenarios.

#[cfg(test)]
mod tests {
    use clap::Parser;
    use std::path::PathBuf;
    
    use crate::cli::{Cli, Commands, OutputFormat, TemplateType, ConfigFormat};
    
    /// Test basic CLI argument parsing
    #[test]
    fn test_cli_help() {
        let result = Cli::try_parse_from(&["nc2parquet", "--help"]);
        assert!(result.is_err()); // --help causes early exit with "error"
        
        let error = result.unwrap_err();
        assert!(error.to_string().contains("Convert NetCDF files to Parquet format"));
    }
    
    /// Test version argument
    #[test]
    fn test_cli_version() {
        let result = Cli::try_parse_from(&["nc2parquet", "--version"]);
        assert!(result.is_err()); // --version causes early exit
    }
    
    /// Test global flags
    #[test]
    fn test_cli_global_flags() {
        let cli = Cli::parse_from(&[
            "nc2parquet", 
            "--verbose", 
            "--output-format", "json",
            "--config", "/path/to/config.json",
            "template", "basic"
        ]);
        
        assert!(cli.verbose);
        assert_eq!(cli.output_format, OutputFormat::Json);
        assert_eq!(cli.config, Some(PathBuf::from("/path/to/config.json")));
    }
    
    /// Test convert command argument parsing
    #[test]
    fn test_convert_command_basic() {
        let cli = Cli::parse_from(&[
            "nc2parquet", "convert", 
            "input.nc", "output.parquet",
            "-n", "temperature"
        ]);
        
        if let Commands::Convert { 
            input, output, variable, .. 
        } = &cli.command {
            assert_eq!(input, &Some("input.nc".to_string()));
            assert_eq!(output, &Some("output.parquet".to_string()));
            assert_eq!(variable, &Some("temperature".to_string()));
        } else {
            panic!("Expected Convert command");
        }
    }
    
    /// Test convert command with filters
    #[test]
    fn test_convert_command_with_filters() {
        let cli = Cli::parse_from(&[
            "nc2parquet", "convert", 
            "input.nc", "output.parquet",
            "-n", "temperature",
            "--range", "latitude:30:60",
            "--range", "longitude:-10:10",
            "--list", "level:1000,850,500",
            "--force",
            "--dry-run"
        ]);
        
        if let Commands::Convert { 
            input, output, variable, 
            range_filters, list_filters,
            force, dry_run, ..
        } = &cli.command {
            assert_eq!(input, &Some("input.nc".to_string()));
            assert_eq!(output, &Some("output.parquet".to_string()));
            assert_eq!(variable, &Some("temperature".to_string()));
            assert_eq!(range_filters.len(), 2);
            assert_eq!(list_filters.len(), 1);
            assert!(force);
            assert!(dry_run);
            
            // Test range filter parsing
            let lat_filter = &range_filters[0];
            assert_eq!(lat_filter.dimension, "latitude");
            assert_eq!(lat_filter.min_value, 30.0);
            assert_eq!(lat_filter.max_value, 60.0);
            
            // Test list filter parsing
            let level_filter = &list_filters[0];
            assert_eq!(level_filter.dimension, "level");
            assert_eq!(level_filter.values, vec![1000.0, 850.0, 500.0]);
        } else {
            panic!("Expected Convert command");
        }
    }
    
    /// Test info command parsing
    #[test]
    fn test_info_command() {
        let cli = Cli::parse_from(&[
            "nc2parquet", "info", 
            "test.nc",
            "--detailed",
            "-n", "temperature",
            "--format", "json"
        ]);
        
        if let Commands::Info { 
            file, detailed, variable, format 
        } = &cli.command {
            assert_eq!(file, "test.nc");
            assert!(detailed);
            assert_eq!(variable, &Some("temperature".to_string()));
            assert_eq!(format, &Some(OutputFormat::Json));
        } else {
            panic!("Expected Info command");
        }
    }
    
    /// Test validate command parsing
    #[test]
    fn test_validate_command() {
        let cli = Cli::parse_from(&[
            "nc2parquet", "validate", 
            "config.json",
            "--detailed"
        ]);
        
        if let Commands::Validate { 
            config_file, detailed 
        } = &cli.command {
            assert_eq!(config_file, &Some(PathBuf::from("config.json")));
            assert!(detailed);
        } else {
            panic!("Expected Validate command");
        }
    }
    
    /// Test template command parsing
    #[test]
    fn test_template_command() {
        let cli = Cli::parse_from(&[
            "nc2parquet", "template", 
            "multi-filter",
            "--output", "template.yaml",
            "--format", "yaml"
        ]);
        
        if let Commands::Template { 
            template_type, output, format 
        } = &cli.command {
            assert_eq!(template_type, &TemplateType::MultiFilter);
            assert_eq!(output, &Some(PathBuf::from("template.yaml")));
            assert_eq!(format, &ConfigFormat::Yaml);
        } else {
            panic!("Expected Template command");
        }
    }
    
    /// Test filter parsing edge cases
    #[test]
    fn test_range_filter_parsing() {
        // Valid range filter
        let cli = Cli::parse_from(&[
            "nc2parquet", "convert", 
            "input.nc", "output.parquet",
            "--range", "time:0.5:10.75"
        ]);
        
        if let Commands::Convert { range_filters, .. } = &cli.command {
            assert_eq!(range_filters.len(), 1);
            let filter = &range_filters[0];
            assert_eq!(filter.dimension, "time");
            assert_eq!(filter.min_value, 0.5);
            assert_eq!(filter.max_value, 10.75);
        }
    }
    
    #[test]
    fn test_list_filter_parsing() {
        // Valid list filter with various numbers
        let cli = Cli::parse_from(&[
            "nc2parquet", "convert", 
            "input.nc", "output.parquet",
            "--list", "pressure:1013.25,850.0,500,300.5"
        ]);
        
        if let Commands::Convert { list_filters, .. } = &cli.command {
            assert_eq!(list_filters.len(), 1);
            let filter = &list_filters[0];
            assert_eq!(filter.dimension, "pressure");
            assert_eq!(filter.values, vec![1013.25, 850.0, 500.0, 300.5]);
        }
    }
    
    /// Test invalid filter formats
    #[test]
    fn test_invalid_range_filter() {
        // Missing colon
        let result = Cli::try_parse_from(&[
            "nc2parquet", "convert", 
            "input.nc", "output.parquet",
            "--range", "invalid_range"
        ]);
        assert!(result.is_err());
        
        // Invalid number format
        let result = Cli::try_parse_from(&[
            "nc2parquet", "convert", 
            "input.nc", "output.parquet",
            "--range", "dim:not_a_number:10"
        ]);
        assert!(result.is_err());
        
        // Min > Max
        let result = Cli::try_parse_from(&[
            "nc2parquet", "convert", 
            "input.nc", "output.parquet",
            "--range", "dim:10:5"
        ]);
        assert!(result.is_err());
    }
    
    #[test]
    fn test_invalid_list_filter() {
        // Missing colon
        let result = Cli::try_parse_from(&[
            "nc2parquet", "convert", 
            "input.nc", "output.parquet",
            "--list", "invalid_list"
        ]);
        assert!(result.is_err());
        
        // Invalid number in list
        let result = Cli::try_parse_from(&[
            "nc2parquet", "convert", 
            "input.nc", "output.parquet",
            "--list", "dim:1,not_a_number,3"
        ]);
        assert!(result.is_err());
    }
    
    /// Test environment variable handling
    #[test]
    fn test_environment_variables() {
        unsafe {
            std::env::set_var("NC2PARQUET_CONFIG", "/path/to/env/config.json");
            std::env::set_var("NC2PARQUET_VARIABLE", "env_temperature");
        }
        
        let _cli = Cli::parse_from(&[
            "nc2parquet", "convert", 
            "input.nc", "output.parquet"
        ]);
        
        // Note: Environment variables are processed by clap, 
        // but we need to test that they're properly configured in the CLI structure
        
        // Clean up
        unsafe {
            std::env::remove_var("NC2PARQUET_CONFIG");
            std::env::remove_var("NC2PARQUET_VARIABLE");
        }
    }
    
    /// Test output format enum
    #[test] 
    fn test_output_format_values() {
        let formats = ["human", "json", "yaml", "csv"];
        
        for format in &formats {
            let cli = Cli::parse_from(&[
                "nc2parquet", "--output-format", format,
                "template", "basic"
            ]);
            
            match format {
                &"human" => assert_eq!(cli.output_format, OutputFormat::Human),
                &"json" => assert_eq!(cli.output_format, OutputFormat::Json),
                &"yaml" => assert_eq!(cli.output_format, OutputFormat::Yaml),
                &"csv" => assert_eq!(cli.output_format, OutputFormat::Csv),
                _ => unreachable!(),
            }
        }
    }
    
    /// Test template type enum
    #[test]
    fn test_template_types() {
        let templates = ["basic", "s3", "multi-filter", "weather", "ocean"];
        
        for template in &templates {
            let cli = Cli::parse_from(&[
                "nc2parquet", "template", template
            ]);
            
            if let Commands::Template { template_type, .. } = &cli.command {
                match template {
                    &"basic" => assert_eq!(template_type, &TemplateType::Basic),
                    &"s3" => assert_eq!(template_type, &TemplateType::S3),
                    &"multi-filter" => assert_eq!(template_type, &TemplateType::MultiFilter),
                    &"weather" => assert_eq!(template_type, &TemplateType::Weather),
                    &"ocean" => assert_eq!(template_type, &TemplateType::Ocean),
                    _ => unreachable!(),
                }
            } else {
                panic!("Expected Template command");
            }
        }
    }
    
    /// Test quiet mode flag
    #[test]
    fn test_quiet_mode() {
        let cli = Cli::parse_from(&[
            "nc2parquet", "--quiet", 
            "info", "test.nc"
        ]);
        
        assert!(cli.quiet);
    }
    
    /// Test conflicting verbose and quiet flags
    #[test]
    fn test_verbose_quiet_conflict() {
        // Both verbose and quiet should conflict - this should fail
        let result = Cli::try_parse_from(&[
            "nc2parquet", "--verbose", "--quiet",
            "info", "test.nc"
        ]);
        
        // This should fail due to conflicting arguments
        assert!(result.is_err());
        
        // Test individual flags work
        let cli_verbose = Cli::parse_from(&[
            "nc2parquet", "--verbose",
            "info", "test.nc"
        ]);
        assert!(cli_verbose.verbose);
        assert!(!cli_verbose.quiet);
        
        let cli_quiet = Cli::parse_from(&[
            "nc2parquet", "--quiet",
            "info", "test.nc"
        ]);
        assert!(!cli_quiet.verbose);
        assert!(cli_quiet.quiet);
    }
    
    /// Test command-specific overrides
    #[test]
    fn test_command_overrides() {
        let cli = Cli::parse_from(&[
            "nc2parquet", "convert", 
            "input.nc", "output.parquet",
            "-n", "temperature",
            "--input-override", "new_input.nc",
            "--output-override", "new_output.parquet"
        ]);
        
        if let Commands::Convert { 
            input, output, 
            input_override, output_override,
            ..
        } = &cli.command {
            assert_eq!(input, &Some("input.nc".to_string()));
            assert_eq!(output, &Some("output.parquet".to_string()));
            assert_eq!(input_override, &Some("new_input.nc".to_string()));
            assert_eq!(output_override, &Some("new_output.parquet".to_string()));
        } else {
            panic!("Expected Convert command");
        }
    }
}