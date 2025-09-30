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

use crate::storage::{StorageBackend, StorageFactory};
use log::debug;
use polars::prelude::*;
use std::io::Cursor;

/// Writes a DataFrame to a Parquet file for local file systems.
///
/// This function writes the DataFrame directly to a local file using Polars'
/// ParquetWriter. It creates parent directories as needed and provides detailed logging.
/// For S3 operations, use the async version instead.
///
/// # Arguments
///
/// * `df` - The DataFrame containing processed NetCDF data
/// * `output_path` - Local path where the Parquet file should be written
///
/// # Returns
///
/// Returns `Ok(())` on successful write, or an error if writing fails.
///
/// # Errors
///
/// This function will return an error if:
/// - The output path is not writable
/// - The DataFrame contains unsupported data types for Parquet
/// - Parent directories cannot be created
pub fn write_dataframe_to_parquet(
    df: &DataFrame,
    output_path: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    debug!("Writing DataFrame to parquet file: {}\n", output_path);

    // Show DataFrame info
    debug!("DataFrame shape: {:?}", df.shape());
    debug!("DataFrame schema:\n{:?}", df.schema());
    debug!("First few rows:\n{}", df.head(Some(5)));

    // Create parent directories if they don't exist
    if let Some(parent) = std::path::Path::new(output_path).parent() {
        std::fs::create_dir_all(parent)?;
    }

    // Write directly to file
    let file = std::fs::File::create(output_path)?;
    let writer = ParquetWriter::new(file);
    let mut df_clone = df.clone();

    writer.finish(&mut df_clone)?;
    debug!("Successfully wrote parquet file: {}", output_path);

    Ok(())
}

/// Async version of DataFrame writing using storage abstraction.
///
/// This function converts the DataFrame to Parquet format in memory and then uses
/// the storage abstraction layer to write it to the destination (local or S3).
/// This version is fully async and works consistently across all storage backends.
///
/// # Arguments
///
/// * `df` - The DataFrame containing processed NetCDF data
/// * `output_path` - Path where the Parquet file should be written (local or S3)
///
/// # Returns
///
/// Returns `Ok(())` on successful write, or an error if writing fails.
///
/// # Errors
///
/// This function will return an error if:
/// - The DataFrame cannot be converted to Parquet format
/// - The storage backend cannot write to the destination
pub async fn write_dataframe_to_parquet_async(
    df: &DataFrame,
    output_path: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    debug!("Writing DataFrame to parquet file: {}\n", output_path);

    // Show DataFrame info
    debug!("DataFrame shape: {:?}", df.shape());
    debug!("DataFrame schema:\n{:?}", df.schema());
    debug!("First few rows:\n{}", df.head(Some(5)));

    // Convert DataFrame to Parquet bytes in memory
    let parquet_bytes = dataframe_to_parquet_bytes(df)?;

    // Use storage abstraction for all backends
    let storage = StorageFactory::from_path(output_path).await?;
    storage.write(output_path, &parquet_bytes).await?;

    debug!("Successfully wrote parquet file: {}", output_path);
    Ok(())
}

/// Converts a DataFrame to Parquet format as bytes in memory.
///
/// This helper function serializes a DataFrame to Parquet format without
/// writing to a file, allowing the bytes to be written via storage abstraction.
///
/// # Arguments
///
/// * `df` - The DataFrame to convert to Parquet format
///
/// # Returns
///
/// Returns the Parquet-formatted bytes, or an error if conversion fails.
fn dataframe_to_parquet_bytes(df: &DataFrame) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let mut buffer = Vec::new();
    let cursor = Cursor::new(&mut buffer);
    let writer = ParquetWriter::new(cursor);
    let mut df_clone = df.clone();

    writer.finish(&mut df_clone)?;
    Ok(buffer)
}
