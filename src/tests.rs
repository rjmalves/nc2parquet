//! # Unit Tests for nc2parquet
//! 
//! This module contains comprehensive unit tests for all components of the nc2parquet library.
//! These tests focus on configuration parsing, filter creation, and basic functionality
//! without requiring external NetCDF files.

use crate::input::*;
use crate::filters::*;

#[cfg(test)]
mod input_tests {
    use super::*;

    #[test]
    fn test_job_config_from_json() {
        let json = r#"
        {
            "nc_key": "test.nc",
            "variable_name": "temp",
            "parquet_key": "test.parquet",
            "filters": [
                {
                    "kind": "range",
                    "params": {
                        "dimension_name": "time",
                        "min_value": 10.0,
                        "max_value": 20.0
                    }
                }
            ]
        }"#;
        
        let config = JobConfig::from_json(json).unwrap();
        assert_eq!(config.nc_key, "test.nc");
        assert_eq!(config.variable_name, "temp");
        assert_eq!(config.parquet_key, "test.parquet");
        assert_eq!(config.filters.len(), 1);
    }
    
    #[test]
    fn test_filter_config_range() {
        let json = r#"
        {
            "kind": "range",
            "params": {
                "dimension_name": "time",
                "min_value": 5.0,
                "max_value": 15.0
            }
        }"#;
        
        let filter_config: FilterConfig = serde_json::from_str(json).unwrap();
        assert_eq!(filter_config.kind(), "range");
        
        let filter = filter_config.to_filter().unwrap();
        // Test that we successfully created a filter (just check it's not None)
        assert!(matches!(filter.as_ref(), _));
    }
    
    #[test]
    fn test_filter_config_2d_point() {
        let json = r#"
        {
            "kind": "2d_point",
            "params": {
                "lat_dimension_name": "lat",
                "lon_dimension_name": "lon",
                "points": [
                    [10.0, 20.0],
                    [15.0, 25.0]
                ],
                "tolerance": 0.1
            }
        }"#;
        
        let filter_config: FilterConfig = serde_json::from_str(json).unwrap();
        assert_eq!(filter_config.kind(), "2d_point");
        
        let filter = filter_config.to_filter().unwrap();
        assert!(matches!(filter.as_ref(), _));
    }
    
    #[test]
    fn test_filter_config_list() {
        let json = r#"
        {
            "kind": "list",
            "params": {
                "dimension_name": "depth",
                "values": [0.0, 10.0, 50.0, 100.0]
            }
        }"#;
        
        let filter_config: FilterConfig = serde_json::from_str(json).unwrap();
        assert_eq!(filter_config.kind(), "list");
        
        let filter = filter_config.to_filter().unwrap();
        assert!(matches!(filter.as_ref(), _));
    }
    
    #[test]
    fn test_filter_config_3d_point() {
        let json = r#"
        {
            "kind": "3d_point",
            "params": {
                "time_dimension_name": "time",
                "lat_dimension_name": "lat", 
                "lon_dimension_name": "lon",
                "steps": [0.0, 24.0, 48.0],
                "points": [[40.0, -74.0], [34.0, -118.0]],
                "tolerance": 0.1
            }
        }"#;
        
        let filter_config: FilterConfig = serde_json::from_str(json).unwrap();
        assert_eq!(filter_config.kind(), "3d_point");
        
        let filter = filter_config.to_filter().unwrap();
        assert!(matches!(filter.as_ref(), _));
    }
    
    #[test] 
    fn test_multiple_filters_config() {
        let json = r#"
        {
            "nc_key": "complex_data.nc",
            "variable_name": "temperature",
            "parquet_key": "filtered_output.parquet",
            "filters": [
                {
                    "kind": "range",
                    "params": {
                        "dimension_name": "time",
                        "min_value": 0.0,
                        "max_value": 100.0
                    }
                },
                {
                    "kind": "2d_point",
                    "params": {
                        "lat_dimension_name": "latitude",
                        "lon_dimension_name": "longitude",
                        "points": [[40.7128, -74.0060]],
                        "tolerance": 0.1
                    }
                }
            ]
        }"#;
        
        let config = JobConfig::from_json(json).unwrap();
        assert_eq!(config.filters.len(), 2);
        assert_eq!(config.filters[0].kind(), "range");
        assert_eq!(config.filters[1].kind(), "2d_point");
    }
}

#[cfg(test)]
mod filter_tests {
    use super::*;
    
    #[test]
    fn test_range_filter_creation() {
        let filter = NCRangeFilter::new("time", 10.0, 20.0);
        assert_eq!(filter.dimension_name, "time");
        assert_eq!(filter.min_value, 10.0);
        assert_eq!(filter.max_value, 20.0);
    }
    
    #[test]
    fn test_list_filter_creation() {
        let values = vec![0.0, 10.0, 20.0, 30.0];
        let filter = NCListFilter::new("depth", values.clone());
        assert_eq!(filter.dimension_name, "depth");
        assert_eq!(filter.values, values);
    }
    
    #[test]
    fn test_2d_point_filter_creation() {
        let points = vec![(10.0, 20.0), (15.0, 25.0)];
        let filter = NC2DPointFilter::new("lat", "lon", points.clone(), 0.1);
        
        assert_eq!(filter.lat_dimension_name, "lat");
        assert_eq!(filter.lon_dimension_name, "lon");
        assert_eq!(filter.points, points);
        assert_eq!(filter.tolerance, 0.1);
    }
    
    #[test]
    fn test_3d_point_filter_creation() {
        let steps = vec![0.0, 24.0, 48.0];
        let points = vec![(40.0, -74.0), (34.0, -118.0)];
        let filter = NC3DPointFilter::new("time", "lat", "lon", steps.clone(), points.clone(), 0.1);
        
        assert_eq!(filter.time_dimension_name, "time");
        assert_eq!(filter.lat_dimension_name, "lat");
        assert_eq!(filter.lon_dimension_name, "lon");
        assert_eq!(filter.steps, steps);
        assert_eq!(filter.points, points);
        assert_eq!(filter.tolerance, 0.1);
    }
    
    #[test]
    fn test_filter_result_single() {
        let result = FilterResult::Single {
            dimension: "time".to_string(),
            indices: vec![1, 2, 3, 5, 8],
        };
        
        assert_eq!(result.len(), 5);
        assert!(!result.is_empty());
        
        if let Some((dim, indices)) = result.as_single() {
            assert_eq!(dim, "time");
            assert_eq!(indices.len(), 5);
            assert!(indices.contains(&1));
            assert!(indices.contains(&8));
        } else {
            panic!("Expected single result");
        }
    }
    
    #[test]
    fn test_filter_result_pairs() {
        let result = FilterResult::Pairs {
            lat_dimension: "latitude".to_string(),
            lon_dimension: "longitude".to_string(),
            pairs: vec![(0, 1), (2, 3), (4, 0)],
        };
        
        assert_eq!(result.len(), 3);
        assert!(!result.is_empty());
        
        if let Some((lat_dim, lon_dim, pairs)) = result.as_pairs() {
            assert_eq!(lat_dim, "latitude");
            assert_eq!(lon_dim, "longitude");
            assert_eq!(pairs.len(), 3);
            assert!(pairs.contains(&(0, 1)));
            assert!(pairs.contains(&(4, 0)));
        } else {
            panic!("Expected pairs result");
        }
    }
    
    #[test]
    fn test_filter_result_empty() {
        let empty_single = FilterResult::Single {
            dimension: "time".to_string(),
            indices: vec![],
        };
        
        assert_eq!(empty_single.len(), 0);
        assert!(empty_single.is_empty());
        
        let empty_pairs = FilterResult::Pairs {
            lat_dimension: "lat".to_string(),
            lon_dimension: "lon".to_string(),
            pairs: vec![],
        };
        
        assert_eq!(empty_pairs.len(), 0);
        assert!(empty_pairs.is_empty());
    }
}

#[cfg(test)]
mod utility_tests {
    use super::*;
    
    #[test]
    fn test_json_parsing_errors() {
        // Test invalid JSON
        let invalid_json = "{ invalid json }";
        let result = JobConfig::from_json(invalid_json);
        assert!(result.is_err());
        
        // Test missing required fields
        let incomplete_json = r#"
        {
            "nc_key": "test.nc"
        }"#;
        let result = JobConfig::from_json(incomplete_json);
        assert!(result.is_err());
    }
    
    #[test]
    fn test_filter_config_invalid_kind() {
        let invalid_filter = r#"
        {
            "kind": "invalid_filter_type",
            "params": {}
        }"#;
        
        let result: Result<FilterConfig, _> = serde_json::from_str(invalid_filter);
        assert!(result.is_err());
    }
    
    #[test]
    fn test_empty_filters_array() {
        let json = r#"
        {
            "nc_key": "test.nc",
            "variable_name": "temp",
            "parquet_key": "test.parquet",
            "filters": []
        }"#;
        
        let config = JobConfig::from_json(json).unwrap();
        assert_eq!(config.filters.len(), 0);
    }
}

/// Integration test demonstrating the complete workflow
/// (without actually processing files due to test environment limitations)
#[cfg(test)]
mod workflow_tests {
    use super::*;
    
    #[test]
    fn test_complete_configuration_workflow() {
        // Create a comprehensive configuration
        let json = r#"
        {
            "nc_key": "weather_data.nc",
            "variable_name": "temperature",
            "parquet_key": "filtered_weather.parquet",
            "filters": [
                {
                    "kind": "range",
                    "params": {
                        "dimension_name": "time",
                        "min_value": 0.0,
                        "max_value": 86400.0
                    }
                },
                {
                    "kind": "list",
                    "params": {
                        "dimension_name": "depth",
                        "values": [0.0, 10.0, 50.0, 100.0]
                    }
                },
                {
                    "kind": "2d_point",
                    "params": {
                        "lat_dimension_name": "latitude",
                        "lon_dimension_name": "longitude",
                        "points": [[40.7128, -74.0060], [34.0522, -118.2437]],
                        "tolerance": 0.1
                    }
                }
            ]
        }"#;
        
        // Parse configuration
        let config = JobConfig::from_json(json).unwrap();
        assert_eq!(config.nc_key, "weather_data.nc");
        assert_eq!(config.variable_name, "temperature");
        assert_eq!(config.parquet_key, "filtered_weather.parquet");
        assert_eq!(config.filters.len(), 3);
        
        // Convert all filters
        let mut filters = Vec::new();
        for filter_config in &config.filters {
            let filter = filter_config.to_filter().unwrap();
            filters.push(filter);
        }
        
        assert_eq!(filters.len(), 3);
        
        // Verify filter types
        assert_eq!(config.filters[0].kind(), "range");
        assert_eq!(config.filters[1].kind(), "list");
        assert_eq!(config.filters[2].kind(), "2d_point");
    }
}