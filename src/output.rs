
use polars::prelude::*;

/// Write DataFrame to Parquet file  
pub fn write_dataframe_to_parquet(df: &DataFrame, output_path: &str) -> Result<(), Box<dyn std::error::Error>> {
    println!("Writing DataFrame to parquet file: {}", output_path);
    
    // Show DataFrame info
    println!("DataFrame shape: {:?}", df.shape());
    println!("DataFrame schema:");
    println!("{:?}", df.schema());
    println!("First few rows:");
    println!("{}", df.head(Some(5)));
    
    // Try different approaches for parquet writing
    println!("Attempting to write parquet file...");
    
    // Method 1: Try using LazyFrame write_parquet
    if let Ok(_) = try_lazy_frame_parquet(df, output_path) {
        println!("Successfully wrote parquet file: {}", output_path);
        return Ok(());
    }
    
    // Method 2: Try using DataFrame write_parquet method
    if let Ok(_) = try_dataframe_write_parquet(df, output_path) {
        println!("Successfully wrote parquet file: {}", output_path);
        return Ok(());
    }
    
    // Method 3: Try with explicit ParquetWriter
    if let Ok(_) = try_explicit_parquet_writer(df, output_path) {
        println!("Successfully wrote parquet file: {}", output_path);
        return Ok(());
    }

    
    Ok(())
}

fn try_lazy_frame_parquet(df: &DataFrame, output_path: &str) -> Result<(), Box<dyn std::error::Error>> {
    // Try using LazyFrame for parquet writing - simplified approach
    let lazy_df = df.clone().lazy();
    
    // Try with a simple file output approach
    match lazy_df.collect() {
        Ok(result_df) => {
            // Try to write with explicit ParquetWriter instead
            try_explicit_parquet_writer(&result_df, output_path)
        },
        Err(e) => {
            println!("LazyFrame collect failed: {}", e);
            Err(Box::new(e))
        }
    }
}

fn try_dataframe_write_parquet(_df: &DataFrame, _output_path: &str) -> Result<(), Box<dyn std::error::Error>> {
    // This method doesn't exist in this version, skip it
    Err("DataFrame write_parquet not available in this Polars version".into())
}

fn try_explicit_parquet_writer(df: &DataFrame, output_path: &str) -> Result<(), Box<dyn std::error::Error>> {
    // Try using ParquetWriter directly
    use std::fs::File;
    
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
