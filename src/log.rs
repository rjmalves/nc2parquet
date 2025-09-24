use crate::input::JobConfig;
use crate::filters::FilterResult;

pub fn show_greeting(config_path: &str) {
    println!("=== NetCDF to Parquet Converter ===");
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

pub fn show_filter_results(filter_results: &Vec<FilterResult>) {
    println!("\nFilter Results:");
    for (i, result) in filter_results.iter().enumerate() {
        match result {
            FilterResult::Single { dimension, indices } => {
                println!("  Filter {}: {} indices for dimension '{}'", i + 1, indices.len(), dimension);
            },
            FilterResult::Pairs { lat_dimension, lon_dimension, pairs } => {
                println!("  Filter {}: {} coordinate pairs for dimensions '{}', '{}'", 
                    i + 1, pairs.len(), lat_dimension, lon_dimension);
            },
            FilterResult::Triplets { time_dimension, lat_dimension, lon_dimension, triplets } => {
                println!("  Filter {}: {} coordinate triplets for dimensions '{}', '{}', '{}'", 
                    i + 1, triplets.len(), time_dimension, lat_dimension, lon_dimension);
            },
        }
    }
}

pub fn show_farewell() {
    println!("\n=== Conversion completed successfully! ===");
}
