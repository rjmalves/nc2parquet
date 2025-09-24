//! # nc2parquet
//! 
//! A Rust library for converting NetCDF files to Parquet format with flexible filtering capabilities.
//! 
//! ## Features
//! 
//! - **Multiple filter types**: Range filters, list filters, 2D point filters, and 3D point filters
//! - **Filter intersection**: Apply multiple filters that intersect properly across dimensions
//! - **Efficient processing**: Only extracts data for coordinates that match all filter criteria
//! - **Type safety**: Strong typing with comprehensive error handling
//! 
//! ## Quick Start
//! 
//! ```rust,no_run
//! use nc2parquet::{process_netcdf_job, input::JobConfig};
//! 
//! // Load configuration from JSON file
//! let config = JobConfig::from_file("config.json").expect("Failed to load config");
//! 
//! // Process the NetCDF file and convert to Parquet
//! process_netcdf_job(&config).expect("Failed to process NetCDF file");
//! ```
//! 
//! ## Configuration Example
//! 
//! ```json
//! {
//!   "nc_key": "input.nc",
//!   "variable_name": "temperature",
//!   "parquet_key": "output.parquet",
//!   "filters": [
//!     {
//!       "kind": "range",
//!       "params": {
//!         "dimension_name": "time",
//!         "min_value": 10.0,
//!         "max_value": 20.0
//!       }
//!     }
//!   ]
//! }
//! ```

pub mod log;
pub mod input;
pub mod filters;
pub mod extract;
pub mod output;

#[cfg(test)]
mod tests;

use crate::input::{JobConfig};
use crate::extract::{extract_data_to_dataframe};
use crate::output::{write_dataframe_to_parquet};
use crate::log::{show_netcdf_file_info};

/// Processes a NetCDF file according to the provided job configuration.
/// 
/// This function orchestrates the entire conversion pipeline:
/// 1. Opens the NetCDF file
/// 2. Validates the specified variable exists
/// 3. Applies all configured filters with intersection logic
/// 4. Extracts the filtered data into a DataFrame
/// 5. Writes the DataFrame to a Parquet file
/// 
/// # Arguments
/// 
/// * `config` - The job configuration specifying input file, filters, and output
/// 
/// # Returns
/// 
/// Returns `Ok(())` on successful conversion, or an error if any step fails.
/// 
/// # Examples
/// 
/// ```rust,no_run
/// use nc2parquet::{process_netcdf_job, input::JobConfig};
/// 
/// let config = JobConfig::from_file("weather_config.json")?;
/// process_netcdf_job(&config)?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
/// 
/// # Errors
/// 
/// This function will return an error if:
/// - The NetCDF file cannot be opened
/// - The specified variable is not found in the NetCDF file
/// - Any filter fails to apply
/// - The output Parquet file cannot be written
pub fn process_netcdf_job(config: &JobConfig) -> Result<(), Box<dyn std::error::Error>> {
    let file = netcdf::open(&config.nc_key)?;
    show_netcdf_file_info(&file)?;
    let var = file.variable(&config.variable_name)
        .ok_or(format!("Variable '{}' not found in NetCDF file", config.variable_name))?;
    
    let mut filters = Vec::new();
    for filter_config in &config.filters {
        let filter = filter_config.to_filter()?;
        filters.push(filter);
    }

    let df = extract_data_to_dataframe(&file, &var, &config.variable_name, &filters)?;
    write_dataframe_to_parquet(&df, &config.parquet_key)?;
    file.close()?;

    Ok(())
}
