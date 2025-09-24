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
//! ## Example Usage
//! 
//! ```rust,no_run
//! use nc2parquet::input::JobConfig;
//! 
//! // Load from file
//! let config = JobConfig::from_file("config.json")?;
//! 
//! // Load from JSON string
//! let json = r#"
//! {
//!   "nc_key": "data.nc",
//!   "variable_name": "temperature",
//!   "parquet_key": "output.parquet",
//!   "filters": []
//! }"#;
//! let config = JobConfig::from_json(json)?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

use serde::{Deserialize};
use std::fs;
use std::path::Path;
use crate::filters::{NCFilter, NCRangeFilter, NCListFilter, NC2DPointFilter, NC3DPointFilter};

/// Main configuration structure for nc2parquet jobs.
/// 
/// This struct represents the complete configuration needed to process a NetCDF file,
/// including input specifications, variable selection, filtering criteria, and output destination.
/// 
/// # Examples
/// 
/// ```rust
/// use nc2parquet::input::{JobConfig, FilterConfig, RangeParams};
/// 
/// let config = JobConfig {
///     nc_key: "temperature_data.nc".to_string(),
///     variable_name: "temperature".to_string(),
///     parquet_key: "filtered_temp.parquet".to_string(),
///     filters: vec![
///         FilterConfig::Range {
///             params: RangeParams {
///                 dimension_name: "time".to_string(),
///                 min_value: 0.0,
///                 max_value: 100.0,
///             },
///         },
///     ],
/// };
/// ```
#[derive(Deserialize)]
pub struct JobConfig {
    /// Path to the input NetCDF file
    pub nc_key: String,
    /// Name of the variable to extract from the NetCDF file
    pub variable_name: String,
    /// Array of filters to apply during data extraction
    pub filters: Vec<FilterConfig>,
    /// Path for the output Parquet file
    pub parquet_key: String,
}

/// Enumeration of all supported filter configurations.
/// 
/// This enum provides a type-safe way to represent different filter types
/// that can be applied to NetCDF data during extraction. Each variant contains
/// the parameters specific to that filter type.
/// 
/// # Examples
/// 
/// ```rust
/// use nc2parquet::input::{FilterConfig, RangeParams};
/// 
/// let filter = FilterConfig::Range {
///     params: RangeParams {
///         dimension_name: "time".to_string(),
///         min_value: 10.0,
///         max_value: 20.0,
///     },
/// };
/// ```
#[derive(Deserialize)]
#[serde(tag = "kind")]
pub enum FilterConfig {
    /// Range filter for selecting values within a numeric range
    #[serde(rename = "range")]
    Range {
        params: RangeParams,
    },
    /// List filter for selecting specific discrete values
    #[serde(rename = "list")]
    List {
        params: ListParams,
    },
    /// 2D point filter for spatial coordinate matching
    #[serde(rename = "2d_point")]
    Point2D {
        params: Point2DParams,
    },
    /// 3D point filter for spatiotemporal coordinate matching
    #[serde(rename = "3d_point")]
    Point3D {
        params: Point3DParams,
    },
}

/// Parameters for range-based filtering.
/// 
/// Defines a numeric range filter that selects values within specified bounds.
/// 
/// # Examples
/// 
/// ```rust
/// use nc2parquet::input::RangeParams;
/// 
/// let params = RangeParams {
///     dimension_name: "time".to_string(),
///     min_value: 0.0,
///     max_value: 86400.0, // One day in seconds
/// };
/// ```
#[derive(Deserialize)]
pub struct RangeParams {
    /// Name of the dimension to filter
    pub dimension_name: String,
    /// Minimum value (inclusive)
    pub min_value: f64,
    /// Maximum value (inclusive)  
    pub max_value: f64,
}

/// Parameters for list-based filtering.
/// 
/// Defines a discrete value filter that selects only specified values.
/// 
/// # Examples
/// 
/// ```rust
/// use nc2parquet::input::ListParams;
/// 
/// let params = ListParams {
///     dimension_name: "depth".to_string(),
///     values: vec![0.0, 10.0, 50.0, 100.0], // Specific depth levels
/// };
/// ```
#[derive(Deserialize)]
pub struct ListParams {
    /// Name of the dimension to filter
    pub dimension_name: String,
    /// List of exact values to match
    pub values: Vec<f64>,
}

/// Parameters for 2D spatial point filtering.
/// 
/// Defines spatial coordinate filtering with tolerance for approximate matching.
/// 
/// # Examples
/// 
/// ```rust
/// use nc2parquet::input::Point2DParams;
/// 
/// let params = Point2DParams {
///     lat_dimension_name: "latitude".to_string(),
///     lon_dimension_name: "longitude".to_string(),
///     points: vec![(40.7128, -74.0060), (34.0522, -118.2437)], // NYC, LA
///     tolerance: 0.1, // ~11km tolerance
/// };
/// ```
#[derive(Deserialize)]
pub struct Point2DParams {
    /// Name of the latitude dimension
    pub lat_dimension_name: String,
    /// Name of the longitude dimension
    pub lon_dimension_name: String,
    /// List of (latitude, longitude) coordinate pairs
    pub points: Vec<(f64, f64)>,
    /// Tolerance for coordinate matching (in degrees)
    pub tolerance: f64,
}

/// Parameters for 3D spatiotemporal point filtering.
/// 
/// Defines filtering for specific time steps at specific spatial coordinates.
/// 
/// # Examples
/// 
/// ```rust
/// use nc2parquet::input::Point3DParams;
/// 
/// let params = Point3DParams {
///     time_dimension_name: "time".to_string(),
///     lat_dimension_name: "latitude".to_string(),
///     lon_dimension_name: "longitude".to_string(),
///     steps: vec![0.0, 24.0, 48.0], // First 3 days
///     points: vec![(40.7128, -74.0060)], // NYC
///     tolerance: 0.1,
/// };
/// ```
#[derive(Deserialize)]
pub struct Point3DParams {
    /// Name of the time dimension
    pub time_dimension_name: String,
    /// Name of the latitude dimension
    pub lat_dimension_name: String,
    /// Name of the longitude dimension
    pub lon_dimension_name: String,
    /// List of time steps to include
    pub steps: Vec<f64>,
    /// List of (latitude, longitude) coordinate pairs
    pub points: Vec<(f64, f64)>,
    /// Tolerance for coordinate matching (in degrees)
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
    /// 
    /// # Examples
    /// 
    /// ```rust,no_run
    /// use nc2parquet::input::JobConfig;
    /// 
    /// let config = JobConfig::from_file("weather_config.json")?;
    /// println!("Processing variable: {}", config.variable_name);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
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
    /// 
    /// # Examples
    /// 
    /// ```rust
    /// use nc2parquet::input::JobConfig;
    /// 
    /// let json = r#"
    /// {
    ///   "nc_key": "data.nc",
    ///   "variable_name": "temperature",
    ///   "parquet_key": "output.parquet",
    ///   "filters": []
    /// }"#;
    /// let config = JobConfig::from_json(json)?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
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
    /// 
    /// # Examples
    /// 
    /// ```rust
    /// use nc2parquet::input::{FilterConfig, RangeParams};
    /// 
    /// let config = FilterConfig::Range {
    ///     params: RangeParams {
    ///         dimension_name: "time".to_string(),
    ///         min_value: 0.0,
    ///         max_value: 100.0,
    ///     },
    /// };
    /// let filter = config.to_filter()?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
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
    /// 
    /// # Examples
    /// 
    /// ```rust
    /// use nc2parquet::input::{FilterConfig, RangeParams};
    /// 
    /// let config = FilterConfig::Range {
    ///     params: RangeParams {
    ///         dimension_name: "time".to_string(),
    ///         min_value: 0.0,
    ///         max_value: 100.0,
    ///     },
    /// };
    /// assert_eq!(config.kind(), "range");
    /// ```
    pub fn kind(&self) -> &'static str {
        match self {
            FilterConfig::Range { .. } => "range",
            FilterConfig::List { .. } => "list",
            FilterConfig::Point2D { .. } => "2d_point",
            FilterConfig::Point3D { .. } => "3d_point",
        }
    }
}
