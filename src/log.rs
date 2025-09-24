use crate::input::JobConfig;
use std::time::{Duration};

pub fn show_greeting(config_path: &str) {
    println!("=== nc2parquet - NetCDF to Parquet Converter ===");
    println!("Loading configuration from: {}", config_path);
}

pub fn config_echo(config: &JobConfig) {
    println!("\nConfiguration:");
    println!("  Input NetCDF: {}", config.nc_key);
    println!("  Variable: {}", config.variable_name);
    println!("  Output Parquet: {}", config.parquet_key);
    println!("  Number of filters: {}", config.filters.len());
    
    for (i, filter) in config.filters.iter().enumerate() {
        println!("    Filter {}: {}", i + 1, filter.kind());
    }   
}

pub fn show_netcdf_file_info(file: &netcdf::File) -> Result<(), Box<dyn std::error::Error>> {
    println!("\nNetCDF File Info:");
    println!("Dimensions:");
    for dim in file.dimensions() {
        println!("  {}: {}", dim.name(), dim.len());
    }
    println!("Variables:");
    for var in file.variables() {
        let dims: Vec<String> = var.dimensions().iter().map(|d| d.name().to_string()).collect();
        println!("  {}: {:?}", var.name(), dims);
    }
    Ok(())
}


pub fn show_farewell_with_timing(elapsed: Duration) {
    println!("\n=== Conversion completed successfully! ===");
    println!("Total elapsed time: {:.2?}", elapsed);
}

