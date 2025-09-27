//! # Input Configuration Module
//! 
//! This module provides configuration parsing and validation for nc2parquet jobs.
//! It handles JSON configuration files that specify NetCDF input files, variables,
//! filters, and Parquet output destinations.
//! 
//! ## Configuration Structure
//! 
//! A configuration file specifies:
//! - **nc_key**: Path to the input NetCDF file
//! - **variable_name**: Name of the variable to extract from the NetCDF file
//! - **parquet_key**: Path for the output Parquet file
//! - **filters**: Array of filters to apply during extraction
//! 
//! ## Filter Types
//! 
//! The module supports four types of filters:
//! - **Range filters**: Select values within a numeric range
//! - **List filters**: Select specific discrete values
//! - **2D Point filters**: Select spatial coordinates with tolerance
//! - **3D Point filters**: Select spatiotemporal coordinates with tolerance
//! 
use serde::{Deserialize};
use std::fs;
use std::path::Path;
use crate::filters::{NCFilter, NCRangeFilter, NCListFilter, NC2DPointFilter, NC3DPointFilter};

/// Main configuration structure for nc2parquet jobs.
/// 
/// This struct represents the complete configuration needed to process a NetCDF file,
/// including input specifications, variable selection, filtering criteria, and output destination.
#[derive(Deserialize)]
pub struct JobConfig {
    pub nc_key: String,
    pub variable_name: String,
    pub filters: Vec<FilterConfig>,
    pub parquet_key: String,
}

/// Enumeration of all supported filter configurations.
/// 
/// This enum provides a type-safe way to represent different filter types
/// that can be applied to NetCDF data during extraction. Each variant contains
/// the parameters specific to that filter type.
#[derive(Deserialize)]
#[serde(tag = "kind")]
pub enum FilterConfig {
    #[serde(rename = "range")]
    Range {
        params: RangeParams,
    },
    #[serde(rename = "list")]
    List {
        params: ListParams,
    },
    #[serde(rename = "2d_point")]
    Point2D {
        params: Point2DParams,
    },
    #[serde(rename = "3d_point")]
    Point3D {
        params: Point3DParams,
    },
}

/// Parameters for range-based filtering.
/// 
/// Defines a numeric range filter that selects values within specified bounds.
#[derive(Deserialize)]
pub struct RangeParams {
    pub dimension_name: String,
    pub min_value: f64,
    pub max_value: f64,
}

/// Parameters for list-based filtering.
/// 
/// Defines a discrete value filter that selects only specified values.
#[derive(Deserialize)]
pub struct ListParams {
    pub dimension_name: String,
    pub values: Vec<f64>,
}

/// Parameters for 2D spatial point filtering.
/// 
/// Defines spatial coordinate filtering with tolerance for approximate matching.
#[derive(Deserialize)]
pub struct Point2DParams {
    pub lat_dimension_name: String,
    pub lon_dimension_name: String,
    pub points: Vec<(f64, f64)>,
    pub tolerance: f64,
}

/// Parameters for 3D spatiotemporal point filtering.
/// 
/// Defines filtering for specific time steps at specific spatial coordinates.
#[derive(Deserialize)]
pub struct Point3DParams {
    pub time_dimension_name: String,
    pub lat_dimension_name: String,
    pub lon_dimension_name: String,
    pub steps: Vec<f64>,
    pub points: Vec<(f64, f64)>,
    pub tolerance: f64,
}

impl JobConfig {
    /// Loads a job configuration from a JSON file.
    /// 
    /// This function reads and parses a JSON configuration file, validating
    /// the structure and returning a `JobConfig` instance.
    /// 
    /// # Arguments
    /// 
    /// * `path` - Path to the JSON configuration file
    /// 
    /// # Returns
    /// 
    /// Returns `Ok(JobConfig)` on success, or an error if the file cannot be
    /// read or the JSON is invalid.
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self, Box<dyn std::error::Error>> {
        let content = fs::read_to_string(path)?;
        let config: JobConfig = serde_json::from_str(&content)?;
        Ok(config)
    }
    
    /// Loads a job configuration from a JSON string.
    /// 
    /// This function parses a JSON string directly, which is useful for
    /// programmatic configuration or testing.
    /// 
    /// # Arguments
    /// 
    /// * `json_str` - JSON string containing the configuration
    /// 
    /// # Returns
    /// 
    /// Returns `Ok(JobConfig)` on success, or an error if the JSON is invalid.
    pub fn from_json(json_str: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let config: JobConfig = serde_json::from_str(json_str)?;
        Ok(config)
    }
}

impl FilterConfig {
    /// Converts this filter configuration into a concrete filter implementation.
    /// 
    /// This method takes the configuration parameters and creates an instance
    /// of the appropriate filter type that can be applied to NetCDF data.
    /// 
    /// # Returns
    /// 
    /// Returns a boxed `NCFilter` trait object on success, or an error if
    /// the filter parameters are invalid.
    pub fn to_filter(&self) -> Result<Box<dyn NCFilter>, Box<dyn std::error::Error>> {
        match self {
            FilterConfig::Range { params } => {
                let filter = NCRangeFilter::new(
                    &params.dimension_name,
                    params.min_value,
                    params.max_value
                );
                Ok(Box::new(filter))
            },
            FilterConfig::List { params } => {
                let filter = NCListFilter::new(
                    &params.dimension_name,
                    params.values.clone()
                );
                Ok(Box::new(filter))
            },
            FilterConfig::Point2D { params } => {
                let filter = NC2DPointFilter::new(
                    &params.lat_dimension_name,
                    &params.lon_dimension_name,
                    params.points.clone(),
                    params.tolerance
                );
                Ok(Box::new(filter))
            },
            FilterConfig::Point3D { params } => {
                let filter = NC3DPointFilter::new(
                    &params.time_dimension_name,
                    &params.lat_dimension_name,
                    &params.lon_dimension_name,
                    params.steps.clone(),
                    params.points.clone(),
                    params.tolerance
                );
                Ok(Box::new(filter))
            },
        }
    }
    
    /// Returns the string identifier for this filter type.
    /// 
    /// This method provides a way to programmatically determine the filter
    /// type without pattern matching.
    /// 
    /// # Returns
    /// 
    /// Returns a static string identifying the filter kind.
    pub fn kind(&self) -> &'static str {
        match self {
            FilterConfig::Range { .. } => "range",
            FilterConfig::List { .. } => "list",
            FilterConfig::Point2D { .. } => "2d_point",
            FilterConfig::Point3D { .. } => "3d_point",
        }
    }
}
