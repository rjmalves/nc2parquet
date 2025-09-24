pub mod log;
pub mod input;
pub mod filters;
pub mod extract;
pub mod output;

use crate::input::{JobConfig};
use crate::extract::{extract_data_to_dataframe};
use crate::output::{write_dataframe_to_parquet};
use crate::log::{show_netcdf_file_info};

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
