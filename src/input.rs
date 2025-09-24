use serde::{Deserialize};
use std::fs;
use std::path::Path;
use crate::filters::{NCFilter, NCRangeFilter, NCListFilter, NC2DPointFilter, NC3DPointFilter};

#[derive(Deserialize)]
pub struct JobConfig {
    pub nc_key: String,
    pub variable_name: String,
    pub filters: Vec<FilterConfig>,
    pub parquet_key: String,
}

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

#[derive(Deserialize)]
pub struct RangeParams {
    pub dimension_name: String,
    pub min_value: f64,
    pub max_value: f64,
}

#[derive(Deserialize)]
pub struct ListParams {
    pub dimension_name: String,
    pub values: Vec<f64>,
}

#[derive(Deserialize)]
pub struct Point2DParams {
    pub lat_dimension_name: String,
    pub lon_dimension_name: String,
    pub points: Vec<(f64, f64)>,
    pub tolerance: f64,
}

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
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self, Box<dyn std::error::Error>> {
        let content = fs::read_to_string(path)?;
        let config: JobConfig = serde_json::from_str(&content)?;
        Ok(config)
    }
    
    pub fn from_json(json_str: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let config: JobConfig = serde_json::from_str(json_str)?;
        Ok(config)
    }
}

impl FilterConfig {
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
    
    pub fn kind(&self) -> &'static str {
        match self {
            FilterConfig::Range { .. } => "range",
            FilterConfig::List { .. } => "list",
            FilterConfig::Point2D { .. } => "2d_point",
            FilterConfig::Point3D { .. } => "3d_point",
        }
    }
}
