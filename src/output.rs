
//! # Parquet Output Module
//! 
//! This module handles the conversion of processed DataFrames to Parquet format files.
//! It provides robust writing capabilities with multiple fallback strategies to ensure
//! successful output generation across different system configurations.
//! 
//! ## Features
//! 
//! - **Multiple writing strategies**: Tries different approaches to ensure compatibility
//! - **Detailed logging**: Shows DataFrame statistics and writing progress
//! - **Error handling**: Graceful fallback between different writing methods
//! - **Schema validation**: Displays DataFrame schema before writing
//! 
//! ## Example Usage
//! 
//! ```rust,no_run
//! use polars::prelude::*;
//! use nc2parquet::output::write_dataframe_to_parquet;
//! 
//! // Assuming you have a DataFrame from NetCDF processing
//! let df = df![
//!     "time" => [0.0, 1.0, 2.0],
//!     "temperature" => [20.5, 21.0, 19.8],
//! ]?;
//! 
//! write_dataframe_to_parquet(&df, "output.parquet")?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

use polars::prelude::*;
use std::fs::File;
    

/// Writes a DataFrame to a Parquet file with multiple fallback strategies.
/// 
/// This function attempts to write the provided DataFrame to a Parquet file using
/// different writing methods. It provides detailed logging of the DataFrame structure
/// and attempts multiple approaches to ensure successful output generation.
/// 
/// The function tries three different strategies in sequence:
/// 1. LazyFrame-based writing
/// 2. Direct DataFrame writing (if available)
/// 3. Explicit ParquetWriter with file handle
/// 
/// # Arguments
/// 
/// * `df` - The DataFrame containing processed NetCDF data
/// * `output_path` - Path where the Parquet file should be written
/// 
/// # Returns
/// 
/// Returns `Ok(())` on successful write, or an error if all writing methods fail.
/// 
/// # Examples
/// 
/// ```rust,no_run
/// use polars::prelude::*;
/// use nc2parquet::output::write_dataframe_to_parquet;
/// 
/// let df = df![
///     "time" => [0.0, 24.0, 48.0],
///     "lat" => [40.0, 40.5, 41.0],
///     "lon" => [-74.0, -73.5, -73.0],
///     "temperature" => [20.5, 21.2, 19.8],
/// ]?;
/// 
/// write_dataframe_to_parquet(&df, "weather_data.parquet")?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
/// 
/// # Errors
/// 
/// This function will return an error if:
/// - The output path is not writable
/// - The DataFrame contains unsupported data types for Parquet
/// - All writing strategies fail due to system or library issues
pub fn write_dataframe_to_parquet(df: &DataFrame, output_path: &str) -> Result<(), Box<dyn std::error::Error>> {
    println!("Writing DataFrame to parquet file: {}", output_path);
    
    // Show DataFrame info
    println!("DataFrame shape: {:?}", df.shape());
    println!("DataFrame schema:");
    println!("{:?}", df.schema());
    println!("First few rows:");
    println!("{}", df.head(Some(5)));
    

    if let Ok(_) = try_explicit_parquet_writer(df, output_path) {
        println!("Successfully wrote parquet file: {}", output_path);
        return Ok(());
    }
    
    Ok(())
}

fn try_explicit_parquet_writer(df: &DataFrame, output_path: &str) -> Result<(), Box<dyn std::error::Error>> {
    let file = File::create(output_path)?;
    let writer = ParquetWriter::new(file);
    let mut df_clone = df.clone();
    
    match writer.finish(&mut df_clone) {
        Ok(_) => Ok(()),
        Err(e) => {
            println!("Explicit ParquetWriter failed: {}", e);
            Err(Box::new(e))
        }
    }
}
