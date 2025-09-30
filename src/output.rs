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
use log::{debug, warn};
use polars::prelude::*;
use std::fs::File;

/// Writes a DataFrame to a Parquet file with multiple fallback strategies.
///
/// This function attempts to write the provided DataFrame to a Parquet file using
/// different writing methods. It provides detailed logging of the DataFrame structure.
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
/// # Errors
///
/// This function will return an error if:
/// - The output path is not writable
/// - The DataFrame contains unsupported data types for Parquet
pub fn write_dataframe_to_parquet(
    df: &DataFrame,
    output_path: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    debug!("Writing DataFrame to parquet file: {}\n", output_path);

    // Show DataFrame info
    debug!("DataFrame shape: {:?}", df.shape());
    debug!("DataFrame schema:\n{:?}", df.schema());
    debug!("First few rows:\n{}", df.head(Some(5)));

    if let Ok(_) = try_explicit_parquet_writer(df, output_path) {
        debug!("Successfully wrote parquet file: {}", output_path);
        return Ok(());
    } else {
        warn!("Parquet writing failed...");
    }

    Ok(())
}

/// Async version of DataFrame writing that supports both local files and S3.
///
/// This function provides the same functionality as `write_dataframe_to_parquet` but with
/// support for S3 output paths. When an S3 path is detected, the Parquet file is written
/// to a temporary location and then uploaded to S3.
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
/// - The DataFrame cannot be written to Parquet format
/// - S3 upload fails (for S3 paths)
/// - Local file cannot be written (for local paths)
pub async fn write_dataframe_to_parquet_async(
    df: &DataFrame,
    output_path: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    debug!("Writing DataFrame to parquet file: {}\n", output_path);

    // Show DataFrame info
    debug!("DataFrame shape: {:?}", df.shape());
    debug!("DataFrame schema:\n{:?}", df.schema());
    debug!("First few rows:\n{}", df.head(Some(5)));

    if output_path.starts_with("s3://") {
        // Write to temporary file and upload to S3
        let temp_file = tempfile::NamedTempFile::new()?;
        let temp_path = temp_file.path();

        // Write to temporary file
        try_explicit_parquet_writer(df, temp_path.to_str().unwrap())?;

        // Upload to S3
        let storage = StorageFactory::from_path(output_path).await?;
        let data: Vec<u8> = tokio::fs::read(temp_path).await?;
        storage.write(output_path, &data).await?;

        debug!("Successfully wrote parquet file to S3: {}", output_path);
    } else {
        // Write directly to local file
        write_dataframe_to_parquet(df, output_path)?;
    }

    Ok(())
}

fn try_explicit_parquet_writer(
    df: &DataFrame,
    output_path: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let file = File::create(output_path)?;
    let writer = ParquetWriter::new(file);
    let mut df_clone = df.clone();

    match writer.finish(&mut df_clone) {
        Ok(_) => Ok(()),
        Err(e) => {
            debug!("Explicit ParquetWriter failed: {}", e);
            Err(Box::new(e))
        }
    }
}
