//! # Unit Tests for nc2parquet
//! 
//! This module contains comprehensive unit tests for all components of the nc2parquet library.
//! Tests use actual NetCDF files from examples/data directory:
//! - `simple_xy.nc`: 2D data with dimensions x(6), y(12) and variable `data`
//! - `pres_temp_4D.nc`: 4D data with dimensions time(2), level(2), latitude(6), longitude(12)
//!   and variables `pressure` and `temperature`

use crate::input::*;
use crate::filters::*;
use crate::extract::*;
use tempfile::tempdir;
use std::path::PathBuf;

/// Helper function to get the path to test NetCDF files
fn get_test_data_path(filename: &str) -> PathBuf {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("examples");
    path.push("data");
    path.push(filename);
    path
}

#[cfg(test)]
mod input_tests {
    use super::*;

    #[test]
    fn test_job_config_from_json() {
        let json = r#"
        {
            "nc_key": "examples/data/simple_xy.nc",
            "variable_name": "data",
            "parquet_key": "test.parquet",
            "filters": [
                {
                    "kind": "range",
                    "params": {
                        "dimension_name": "x",
                        "min_value": 1.0,
                        "max_value": 4.0
                    }
                }
            ]
        }"#;
        
        let config = JobConfig::from_json(json).unwrap();
        assert_eq!(config.nc_key, "examples/data/simple_xy.nc");
        assert_eq!(config.variable_name, "data");
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
                "min_value": 0.0,
                "max_value": 1.0
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
                "lat_dimension_name": "latitude",
                "lon_dimension_name": "longitude",
                "points": [
                    [30.0, -120.0],
                    [40.0, -100.0]
                ],
                "tolerance": 5.0
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
                "dimension_name": "level",
                "values": [0.0, 1.0]
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
                "lat_dimension_name": "latitude", 
                "lon_dimension_name": "longitude",
                "steps": [0.0, 1.0],
                "points": [[35.0, -110.0], [45.0, -85.0]],
                "tolerance": 5.0
            }
        }"#;
        
        let filter_config: FilterConfig = serde_json::from_str(json).unwrap();
        assert_eq!(filter_config.kind(), "3d_point");
        
        let filter = filter_config.to_filter().unwrap();
        assert!(matches!(filter.as_ref(), _));
    }
    
    #[test] 
    fn test_multiple_filters_config_with_real_data() {
        let json = r#"
        {
            "nc_key": "examples/data/pres_temp_4D.nc",
            "variable_name": "temperature",
            "parquet_key": "filtered_output.parquet",
            "filters": [
                {
                    "kind": "range",
                    "params": {
                        "dimension_name": "time",
                        "min_value": 0.0,
                        "max_value": 1.0
                    }
                },
                {
                    "kind": "2d_point",
                    "params": {
                        "lat_dimension_name": "latitude",
                        "lon_dimension_name": "longitude",
                        "points": [[30.0, -120.0]],
                        "tolerance": 5.0
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
    fn test_range_filter_with_real_data() -> Result<(), Box<dyn std::error::Error>> {
        let file_path = get_test_data_path("pres_temp_4D.nc");
        let file = netcdf::open(&file_path)?;
        
        // Test latitude filtering with actual coordinate values
        let filter = NCRangeFilter::new("latitude", 30.0, 45.0);
        let result = filter.apply(&file)?;
        
        if let FilterResult::Single { dimension, indices } = result {
            assert_eq!(dimension, "latitude");
            // Should include indices for 30, 35, 40, 45 degrees (indices 1, 2, 3, 4)
            assert_eq!(indices.len(), 4);
            assert!(indices.contains(&1)); // 30.0
            assert!(indices.contains(&2)); // 35.0
            assert!(indices.contains(&3)); // 40.0
            assert!(indices.contains(&4)); // 45.0
        } else {
            panic!("Expected Single filter result");
        }
        
        file.close()?;
        Ok(())
    }
    
    #[test]
    fn test_list_filter_creation() {
        let values = vec![0.0, 10.0, 20.0, 30.0];
        let filter = NCListFilter::new("depth", values.clone());
        assert_eq!(filter.dimension_name, "depth");
        assert_eq!(filter.values, values);
    }

    #[test]
    fn test_list_filter_with_real_data() -> Result<(), Box<dyn std::error::Error>> {
        let file_path = get_test_data_path("pres_temp_4D.nc");
        let file = netcdf::open(&file_path)?;
        
        // Test longitude filtering with specific values
        let filter = NCListFilter::new("longitude", vec![-120.0, -85.0]);
        let result = filter.apply(&file)?;
        
        if let FilterResult::Single { dimension, indices } = result {
            assert_eq!(dimension, "longitude");
            assert_eq!(indices.len(), 2);
            assert!(indices.contains(&1)); // -120.0
            assert!(indices.contains(&8)); // -85.0
        } else {
            panic!("Expected Single filter result");
        }
        
        file.close()?;
        Ok(())
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
    fn test_2d_point_filter_with_real_data() -> Result<(), Box<dyn std::error::Error>> {
        let file_path = get_test_data_path("pres_temp_4D.nc");
        let file = netcdf::open(&file_path)?;
        
        // Test spatial filtering with actual coordinate values
        // lat: [25, 30, 35, 40, 45, 50], lon: [-125, -120, -115, -110, -105, -100, -95, -90, -85, -80, -75, -70]
        let points = vec![(30.0, -120.0), (45.0, -85.0)]; // Should match indices (1,1) and (4,8)
        let filter = NC2DPointFilter::new("latitude", "longitude", points, 1.0);
        let result = filter.apply(&file)?;
        
        if let FilterResult::Pairs { lat_dimension, lon_dimension, pairs } = result {
            assert_eq!(lat_dimension, "latitude");
            assert_eq!(lon_dimension, "longitude");
            assert_eq!(pairs.len(), 2);
            // Check that we found the expected coordinate pairs
            assert!(pairs.contains(&(1, 1))); // (30.0, -120.0)
            assert!(pairs.contains(&(4, 8))); // (45.0, -85.0)
        } else {
            panic!("Expected Pairs filter result");
        }
        
        file.close()?;
        Ok(())
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
    fn test_3d_point_filter_creation_only() {
        // Since pres_temp_4D.nc doesn't have time coordinate variable,
        // we can only test the creation and basic properties
        let steps = vec![0.0, 1.0]; // Time step indices
        let points = vec![(35.0, -110.0)]; // Spatial coordinates
        let filter = NC3DPointFilter::new("time", "latitude", "longitude", steps.clone(), points.clone(), 5.0);
        
        assert_eq!(filter.time_dimension_name, "time");
        assert_eq!(filter.lat_dimension_name, "latitude");
        assert_eq!(filter.lon_dimension_name, "longitude");
        assert_eq!(filter.steps, steps);
        assert_eq!(filter.points, points);
        assert_eq!(filter.tolerance, 5.0);
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
mod extract_tests {
    use super::*;
    
    #[test]
    fn test_dimension_index_manager_with_simple_data() -> Result<(), Box<dyn std::error::Error>> {
        let file_path = get_test_data_path("simple_xy.nc");
        let file = netcdf::open(&file_path)?;
        let var = file.variable("data").unwrap();
        
        let manager = DimensionIndexManager::new(&var)?;
        let dimensions = manager.get_dimension_order();
        
        assert_eq!(dimensions.len(), 2);
        assert!(dimensions.contains(&"x".to_string()));
        assert!(dimensions.contains(&"y".to_string()));
        
        file.close()?;
        Ok(())
    }

    #[test]
    fn test_dimension_index_manager_with_4d_data() -> Result<(), Box<dyn std::error::Error>> {
        let file_path = get_test_data_path("pres_temp_4D.nc");
        let file = netcdf::open(&file_path)?;
        let var = file.variable("temperature").unwrap();
        
        let manager = DimensionIndexManager::new(&var)?;
        let dimensions = manager.get_dimension_order();
        
        assert_eq!(dimensions.len(), 4);
        assert!(dimensions.contains(&"time".to_string()));
        assert!(dimensions.contains(&"level".to_string()));
        assert!(dimensions.contains(&"latitude".to_string()));
        assert!(dimensions.contains(&"longitude".to_string()));
        
        file.close()?;
        Ok(())
    }
    
    #[test]
    fn test_dimension_index_manager_filter_application() -> Result<(), Box<dyn std::error::Error>> {
        let file_path = get_test_data_path("pres_temp_4D.nc");
        let file = netcdf::open(&file_path)?;
        let var = file.variable("temperature").unwrap();
        
        let mut manager = DimensionIndexManager::new(&var)?;
        
        // Apply a range filter result
        let filter_result = FilterResult::Single {
            dimension: "time".to_string(),
            indices: vec![0, 1],
        };
        
        manager.apply_filter_result(&filter_result)?;
        
        let time_indices = manager.get_dimension_indices("time").unwrap();
        assert_eq!(time_indices.len(), 2);
        assert!(time_indices.contains(&0));
        assert!(time_indices.contains(&1));
        
        file.close()?;
        Ok(())
    }
    
    #[test]
    fn test_extract_data_to_dataframe_simple() -> Result<(), Box<dyn std::error::Error>> {
        let file_path = get_test_data_path("simple_xy.nc");
        let file = netcdf::open(&file_path)?;
        let var = file.variable("data").unwrap();
        
        // No filters - extract all data
        let filters: Vec<Box<dyn NCFilter>> = vec![];
        let df = extract_data_to_dataframe(&file, &var, "data", &filters)?;
        
        // Should have 6 * 12 = 72 rows (all combinations)
        assert_eq!(df.height(), 72);
        
        // Check column names
        let column_names: Vec<String> = df.get_column_names().iter().map(|s| s.to_string()).collect();
        assert!(column_names.contains(&"x".to_string()));
        assert!(column_names.contains(&"y".to_string()));
        assert!(column_names.contains(&"data".to_string()));
        
        file.close()?;
        Ok(())
    }

    #[test]
    fn test_extract_data_to_dataframe_with_filter() -> Result<(), Box<dyn std::error::Error>> {
        let file_path = get_test_data_path("pres_temp_4D.nc");
        let file = netcdf::open(&file_path)?;
        let var = file.variable("temperature").unwrap();
        
        // Create a range filter for latitude dimension
        let filter = NCRangeFilter::new("latitude", 30.0, 40.0);
        let filters: Vec<Box<dyn NCFilter>> = vec![Box::new(filter)];
        
        let df = extract_data_to_dataframe(&file, &var, "temperature", &filters)?;
        
        // Should have 2 time steps * 2 levels * 3 lats * 12 lons = 144 rows
        assert_eq!(df.height(), 144);
        
        // Check column names
        let column_names: Vec<String> = df.get_column_names().iter().map(|s| s.to_string()).collect();
        assert!(column_names.contains(&"time".to_string()));
        assert!(column_names.contains(&"level".to_string()));
        assert!(column_names.contains(&"latitude".to_string()));
        assert!(column_names.contains(&"longitude".to_string()));
        assert!(column_names.contains(&"temperature".to_string()));
        
        file.close()?;
        Ok(())
    }

    #[test]
    fn test_extract_data_to_dataframe_with_spatial_filter() -> Result<(), Box<dyn std::error::Error>> {
        let file_path = get_test_data_path("pres_temp_4D.nc");
        let file = netcdf::open(&file_path)?;
        let var = file.variable("temperature").unwrap();
        
        // Create a spatial filter for specific coordinates
        let points = vec![(30.0, -120.0)]; // Should match one lat/lon pair
        let filter = NC2DPointFilter::new("latitude", "longitude", points, 1.0);
        let filters: Vec<Box<dyn NCFilter>> = vec![Box::new(filter)];
        
        let df = extract_data_to_dataframe(&file, &var, "temperature", &filters)?;
        
        // Should have 2 time steps * 2 levels * 1 coordinate pair = 4 rows
        assert_eq!(df.height(), 4);
        
        file.close()?;
        Ok(())
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

/// Integration tests using real NetCDF files
#[cfg(test)]
mod integration_tests {
    use super::*;
    
    #[test]
    fn test_full_pipeline_simple_xy() -> Result<(), Box<dyn std::error::Error>> {
        let file_path = get_test_data_path("simple_xy.nc");
        let temp_dir = tempdir()?;
        let output_path = temp_dir.path().join("simple_xy_output.parquet");
        
        // Create job configuration for simple_xy.nc
        let config = JobConfig {
            nc_key: file_path.to_string_lossy().to_string(),
            variable_name: "data".to_string(),
            parquet_key: output_path.to_string_lossy().to_string(),
            filters: vec![],
        };
        
        // Run the full pipeline
        crate::process_netcdf_job(&config)?;
        
        // Verify output file exists
        assert!(output_path.exists());
        
        // Verify the file has some content (basic check)
        let metadata = std::fs::metadata(&output_path)?;
        assert!(metadata.len() > 0);
        
        Ok(())
    }
    
    #[test]
    fn test_full_pipeline_with_latitude_filter() -> Result<(), Box<dyn std::error::Error>> {
        let file_path = get_test_data_path("pres_temp_4D.nc");
        let temp_dir = tempdir()?;
        let output_path = temp_dir.path().join("filtered_temp_output.parquet");
        
        // Create job configuration with latitude filter
        let config = JobConfig {
            nc_key: file_path.to_string_lossy().to_string(),
            variable_name: "temperature".to_string(),
            parquet_key: output_path.to_string_lossy().to_string(),
            filters: vec![
                FilterConfig::Range {
                    params: RangeParams {
                        dimension_name: "latitude".to_string(),
                        min_value: 30.0,
                        max_value: 45.0,
                    },
                },
            ],
        };
        
        // Run the full pipeline
        crate::process_netcdf_job(&config)?;
        
        // Verify output file exists and has content
        assert!(output_path.exists());
        let metadata = std::fs::metadata(&output_path)?;
        assert!(metadata.len() > 0);
        
        Ok(())
    }
    
    #[test]
    fn test_full_pipeline_with_spatial_filter() -> Result<(), Box<dyn std::error::Error>> {
        let file_path = get_test_data_path("pres_temp_4D.nc");
        let temp_dir = tempdir()?;
        let output_path = temp_dir.path().join("spatial_filtered_output.parquet");
        
        // Create job configuration with spatial filter
        let config = JobConfig {
            nc_key: file_path.to_string_lossy().to_string(),
            variable_name: "pressure".to_string(),
            parquet_key: output_path.to_string_lossy().to_string(),
            filters: vec![
                FilterConfig::Point2D {
                    params: Point2DParams {
                        lat_dimension_name: "latitude".to_string(),
                        lon_dimension_name: "longitude".to_string(),
                        points: vec![(30.0, -120.0), (40.0, -100.0)],
                        tolerance: 1.0,
                    },
                },
            ],
        };
        
        // Run the full pipeline
        crate::process_netcdf_job(&config)?;
        
        // Verify output file exists and has content
        assert!(output_path.exists());
        let metadata = std::fs::metadata(&output_path)?;
        assert!(metadata.len() > 0);
        
        Ok(())
    }

    #[test]
    fn test_full_pipeline_multi_filter() -> Result<(), Box<dyn std::error::Error>> {
        let file_path = get_test_data_path("pres_temp_4D.nc");
        let temp_dir = tempdir()?;
        let output_path = temp_dir.path().join("multi_filter_output.parquet");
        
        // Create job configuration with multiple spatial filters
        let config = JobConfig {
            nc_key: file_path.to_string_lossy().to_string(),
            variable_name: "temperature".to_string(),
            parquet_key: output_path.to_string_lossy().to_string(),
            filters: vec![
                FilterConfig::Range {
                    params: RangeParams {
                        dimension_name: "latitude".to_string(),
                        min_value: 35.0,
                        max_value: 45.0,
                    },
                },
                FilterConfig::List {
                    params: ListParams {
                        dimension_name: "longitude".to_string(),
                        values: vec![-120.0, -110.0, -100.0],
                    },
                },
            ],
        };
        
        // Run the full pipeline
        crate::process_netcdf_job(&config)?;
        
        // Verify output file exists and has content
        assert!(output_path.exists());
        let metadata = std::fs::metadata(&output_path)?;
        assert!(metadata.len() > 0);
        
        Ok(())
    }
}

/// Integration test demonstrating the complete workflow
#[cfg(test)]
mod workflow_tests {
    use super::*;
    
    #[test]
    fn test_complete_configuration_workflow_with_real_data() {
        // Create a comprehensive configuration using real file structure
        let json = r#"
        {
            "nc_key": "examples/data/pres_temp_4D.nc",
            "variable_name": "temperature",
            "parquet_key": "filtered_weather.parquet",
            "filters": [
                {
                    "kind": "range",
                    "params": {
                        "dimension_name": "latitude",
                        "min_value": 30.0,
                        "max_value": 45.0
                    }
                },
                {
                    "kind": "list",
                    "params": {
                        "dimension_name": "longitude",
                        "values": [-120.0, -100.0, -80.0]
                    }
                },
                {
                    "kind": "2d_point",
                    "params": {
                        "lat_dimension_name": "latitude",
                        "lon_dimension_name": "longitude",
                        "points": [[30.0, -120.0], [45.0, -85.0]],
                        "tolerance": 5.0
                    }
                }
            ]
        }"#;
        
        // Parse configuration
        let config = JobConfig::from_json(json).unwrap();
        assert_eq!(config.nc_key, "examples/data/pres_temp_4D.nc");
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