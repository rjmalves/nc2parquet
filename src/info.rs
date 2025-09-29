//! # NetCDF File Information Module
//!
//! This module provides functionality to extract and display information about NetCDF files,
//! including dimensions, variables, attributes, and metadata.

use crate::storage::{StorageBackend, StorageFactory};
use anyhow::{Context, Result};
use log::debug;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Information about a NetCDF dimension
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetCdfDimensionInfo {
    pub name: String,
    pub length: usize,
    pub is_unlimited: bool,
}

/// Information about a NetCDF variable
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetCdfVariableInfo {
    pub name: String,
    pub data_type: String,
    pub dimensions: Vec<String>,
    pub attributes: HashMap<String, String>,
    pub shape: Vec<usize>,
}

/// Complete information about a NetCDF file
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetCdfInfo {
    pub path: String,
    pub dimensions: Vec<NetCdfDimensionInfo>,
    pub variables: Vec<NetCdfVariableInfo>,
    pub global_attributes: HashMap<String, String>,
    pub file_size: Option<u64>,
    pub total_variables: usize,
    pub total_dimensions: usize,
}

/// Extract comprehensive information from a NetCDF file
pub async fn get_netcdf_info(
    file_path: &str,
    variable: Option<&str>,
    detailed: bool,
) -> Result<NetCdfInfo> {
    // Handle S3 paths - download to temporary file first
    let (temp_file, local_path) = if file_path.starts_with("s3://") {
        let storage = StorageFactory::from_path(file_path).await?;
        let data = storage
            .read(file_path)
            .await
            .context("Failed to read S3 file for analysis")?;

        // Create temporary file
        let temp_file =
            tempfile::NamedTempFile::new().context("Failed to create temporary file")?;
        let temp_path = temp_file.path().to_path_buf();

        debug!("Writing S3 data to temporary path: {:?}", temp_path);
        tokio::fs::write(&temp_path, data)
            .await
            .context("Failed to write temporary file")?;

        (Some(temp_file), temp_path.to_string_lossy().to_string())
    } else {
        (None, file_path.to_string())
    };

    // Open and analyze NetCDF file
    debug!("Opening NetCDF file: {}", local_path);
    let file = netcdf::open(&local_path)
        .with_context(|| format!("Failed to open NetCDF file: {}", file_path))?;

    // Get file size
    let file_size = if file_path.starts_with("s3://") {
        None // Could implement S3 size check, but it's optional
    } else {
        tokio::fs::metadata(&local_path)
            .await
            .ok()
            .map(|metadata| metadata.len())
    };

    // Extract dimensions
    let mut dimensions = Vec::new();
    for dim in file.dimensions() {
        dimensions.push(NetCdfDimensionInfo {
            name: dim.name().to_string(),
            length: dim.len(),
            is_unlimited: dim.is_unlimited(),
        });
    }

    // Extract variables
    let mut variables = Vec::new();
    for var in file.variables() {
        // Skip if specific variable requested and this isn't it
        if let Some(var_name) = variable {
            if var.name() != var_name {
                continue;
            }
        }

        let mut attributes = HashMap::new();

        // Extract variable attributes
        for attr in var.attributes() {
            if let Ok(value) = attr.value() {
                let value_str = format_attribute_value(&value);
                attributes.insert(attr.name().to_string(), value_str);
            }
        }

        // Get variable shape
        let shape: Vec<usize> = var.dimensions().iter().map(|d| d.len()).collect();

        variables.push(NetCdfVariableInfo {
            name: var.name().to_string(),
            data_type: format_variable_type(&var.vartype()),
            dimensions: var
                .dimensions()
                .iter()
                .map(|d| d.name().to_string())
                .collect(),
            attributes,
            shape,
        });
    }

    // Extract global attributes
    let mut global_attributes = HashMap::new();
    if detailed {
        for attr in file.attributes() {
            if let Ok(value) = attr.value() {
                let value_str = format_attribute_value(&value);
                global_attributes.insert(attr.name().to_string(), value_str);
            }
        }
    }

    file.close().context("Failed to close NetCDF file")?;

    // Keep temp file alive until after we close the netcdf file
    drop(temp_file);

    Ok(NetCdfInfo {
        path: file_path.to_string(),
        total_dimensions: dimensions.len(),
        total_variables: variables.len(),
        dimensions,
        variables,
        global_attributes,
        file_size,
    })
}

/// Format netcdf attribute value for display
fn format_attribute_value(value: &netcdf::AttributeValue) -> String {
    format!("{:?}", value)
}

/// Format netcdf variable type for display
fn format_variable_type(var_type: &netcdf::types::NcVariableType) -> String {
    format!("{:?}", var_type)
}

/// Print NetCDF info in human-readable format
pub fn print_file_info_human(info: &NetCdfInfo) {
    println!("NetCDF File Information:");
    println!("  Path: {}", info.path);
    if let Some(size) = info.file_size {
        println!("  File Size: {:.2} MB", size as f64 / 1_048_576.0);
    }
    println!("  Dimensions: {} total", info.total_dimensions);
    for dim in &info.dimensions {
        println!(
            "    {} ({}{})",
            dim.name,
            dim.length,
            if dim.is_unlimited { ", unlimited" } else { "" }
        );
    }
    println!("  Variables: {} total", info.total_variables);
    for var in &info.variables {
        println!(
            "    {} ({}) - dimensions: [{}]",
            var.name,
            var.data_type,
            var.dimensions.join(", ")
        );
        if !var.attributes.is_empty() {
            for (name, value) in &var.attributes {
                println!("      @{}: {}", name, value);
            }
        }
    }
    if !info.global_attributes.is_empty() {
        println!("  Global Attributes:");
        for (name, value) in &info.global_attributes {
            println!("    @{}: {}", name, value);
        }
    }
}

/// Print NetCDF info in JSON format
pub fn print_file_info_json(info: &NetCdfInfo) -> Result<()> {
    let json = serde_json::json!({
        "path": info.path,
        "dimensions": info.dimensions,
        "variables": info.variables,
        "global_attributes": info.global_attributes,
        "file_size": info.file_size,
        "total_variables": info.total_variables,
        "total_dimensions": info.total_dimensions
    });
    println!("{}", serde_json::to_string_pretty(&json)?);
    Ok(())
}

/// Print NetCDF info in YAML format
pub fn print_file_info_yaml(info: &NetCdfInfo) -> Result<()> {
    let yaml = serde_yaml::to_string(info).context("Failed to serialize NetCDF info to YAML")?;
    println!("{}", yaml);
    Ok(())
}

/// Print NetCDF info in CSV format (variables only)
pub fn print_file_info_csv(info: &NetCdfInfo) -> Result<()> {
    // Print variables as CSV - this is the most useful tabular data
    println!("variable_name,data_type,dimensions,shape,attributes_count");
    for var in &info.variables {
        println!(
            "{},{},{},{},{}",
            var.name,
            var.data_type,
            format!("\"{}\"", var.dimensions.join(";")),
            format!(
                "\"{}\"",
                var.shape
                    .iter()
                    .map(|s| s.to_string())
                    .collect::<Vec<_>>()
                    .join(";")
            ),
            var.attributes.len()
        );
    }
    Ok(())
}
