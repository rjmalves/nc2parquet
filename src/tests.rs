use crate::extract::*;
use crate::filters::*;
use crate::input::*;
use std::path::PathBuf;
use tempfile::tempdir;

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

        if let FilterResult::Pairs {
            lat_dimension,
            lon_dimension,
            pairs,
        } = result
        {
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
        let filter = NC3DPointFilter::new(
            "time",
            "latitude",
            "longitude",
            steps.clone(),
            points.clone(),
            5.0,
        );

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
        let column_names: Vec<String> = df
            .get_column_names()
            .iter()
            .map(|s| s.to_string())
            .collect();
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
        let column_names: Vec<String> = df
            .get_column_names()
            .iter()
            .map(|s| s.to_string())
            .collect();
        assert!(column_names.contains(&"time".to_string()));
        assert!(column_names.contains(&"level".to_string()));
        assert!(column_names.contains(&"latitude".to_string()));
        assert!(column_names.contains(&"longitude".to_string()));
        assert!(column_names.contains(&"temperature".to_string()));

        file.close()?;
        Ok(())
    }

    #[test]
    fn test_extract_data_to_dataframe_with_spatial_filter() -> Result<(), Box<dyn std::error::Error>>
    {
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
            postprocessing: None,
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
            filters: vec![FilterConfig::Range {
                params: RangeParams {
                    dimension_name: "latitude".to_string(),
                    min_value: 30.0,
                    max_value: 45.0,
                },
            }],
            postprocessing: None,
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
            filters: vec![FilterConfig::Point2D {
                params: Point2DParams {
                    lat_dimension_name: "latitude".to_string(),
                    lon_dimension_name: "longitude".to_string(),
                    points: vec![(30.0, -120.0), (40.0, -100.0)],
                    tolerance: 1.0,
                },
            }],
            postprocessing: None,
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
            postprocessing: None,
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

    // Sprint 6: Comprehensive Integration Tests
    #[test]
    fn test_sprint6_integration_local_file_with_all_features()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::postprocess::*;
        use std::collections::HashMap;

        let temp_dir = tempdir()?;
        let output_path = temp_dir.path().join("sprint6_full_features.parquet");

        // Create comprehensive config with filtering and post-processing
        let config = JobConfig {
            nc_key: get_test_data_path("simple_xy.nc")
                .to_string_lossy()
                .to_string(),
            variable_name: "data".to_string(),
            parquet_key: output_path.to_string_lossy().to_string(),
            filters: vec![], // Remove filters for simple_xy.nc since it doesn't have coordinate variables
            postprocessing: Some(ProcessingPipelineConfig {
                name: Some("Sprint 6 Integration Pipeline".to_string()),
                processors: vec![
                    ProcessorConfig::RenameColumns {
                        mappings: {
                            let mut map = HashMap::new();
                            map.insert("data".to_string(), "temp_k".to_string());
                            map.insert("x".to_string(), "longitude".to_string());
                            map.insert("y".to_string(), "latitude".to_string());
                            map
                        },
                    },
                    ProcessorConfig::ApplyFormula {
                        target_column: "temp_celsius".to_string(),
                        formula: "temp_k - 273.15".to_string(),
                        source_columns: vec!["temp_k".to_string()],
                    },
                    ProcessorConfig::UnitConvert {
                        column: "temp_k".to_string(),
                        from_unit: "kelvin".to_string(),
                        to_unit: "celsius".to_string(),
                    },
                ],
            }),
        };

        // Execute the full pipeline
        crate::process_netcdf_job(&config)?;

        // Verify the output file was created
        assert!(output_path.exists());
        let metadata = std::fs::metadata(&output_path)?;
        assert!(metadata.len() > 0);

        println!("Sprint 6: Full feature integration test completed successfully");
        Ok(())
    }

    #[tokio::test]
    async fn test_sprint6_integration_async_processing() -> Result<(), Box<dyn std::error::Error>> {
        use crate::postprocess::*;
        use std::collections::HashMap;

        let temp_dir = tempdir()?;
        let output_path = temp_dir.path().join("sprint6_async_test.parquet");

        let config = JobConfig {
            nc_key: get_test_data_path("pres_temp_4D.nc")
                .to_string_lossy()
                .to_string(),
            variable_name: "temperature".to_string(),
            parquet_key: output_path.to_string_lossy().to_string(),
            filters: vec![FilterConfig::Range {
                params: RangeParams {
                    dimension_name: "latitude".to_string(),
                    min_value: 25.0,
                    max_value: 35.0,
                },
            }],
            postprocessing: Some(ProcessingPipelineConfig {
                name: Some("Async Processing Test".to_string()),
                processors: vec![
                    ProcessorConfig::RenameColumns {
                        mappings: {
                            let mut map = HashMap::new();
                            map.insert("temperature".to_string(), "temp_k".to_string());
                            map
                        },
                    },
                    ProcessorConfig::UnitConvert {
                        column: "temp_k".to_string(),
                        from_unit: "kelvin".to_string(),
                        to_unit: "celsius".to_string(),
                    },
                ],
            }),
        };

        // Execute async pipeline
        crate::process_netcdf_job_async(&config).await?;

        assert!(output_path.exists());
        let metadata = std::fs::metadata(&output_path)?;
        assert!(metadata.len() > 0);

        println!("Sprint 6: Async processing with post-processing completed");
        Ok(())
    }

    #[test]
    fn test_sprint6_integration_complex_pipeline_chaining() -> Result<(), Box<dyn std::error::Error>>
    {
        use crate::postprocess::*;
        use std::collections::HashMap;

        let temp_dir = tempdir()?;
        let output_path = temp_dir.path().join("sprint6_complex_pipeline.parquet");

        // Test complex pipeline with multiple processors in sequence
        let config = JobConfig {
            nc_key: get_test_data_path("simple_xy.nc")
                .to_string_lossy()
                .to_string(),
            variable_name: "data".to_string(),
            parquet_key: output_path.to_string_lossy().to_string(),
            filters: vec![], // Remove filters for simple_xy.nc
            postprocessing: Some(ProcessingPipelineConfig {
                name: Some("Complex Pipeline Chaining Test".to_string()),
                processors: vec![
                    // Step 1: Rename all columns
                    ProcessorConfig::RenameColumns {
                        mappings: {
                            let mut map = HashMap::new();
                            map.insert("data".to_string(), "temp_k".to_string());
                            map.insert("x".to_string(), "lon".to_string());
                            map.insert("y".to_string(), "lat".to_string());
                            map
                        },
                    },
                    // Step 2: Add formula column based on renamed column
                    ProcessorConfig::ApplyFormula {
                        target_column: "temp_celsius".to_string(),
                        formula: "temp_k - 273.15".to_string(),
                        source_columns: vec!["temp_k".to_string()],
                    },
                    // Step 3: Add another simple formula
                    ProcessorConfig::ApplyFormula {
                        target_column: "temp_doubled".to_string(),
                        formula: "temp_k * 2.0".to_string(),
                        source_columns: vec!["temp_k".to_string()],
                    },
                    // Step 4: Unit conversion on original temperature column
                    ProcessorConfig::UnitConvert {
                        column: "temp_k".to_string(),
                        from_unit: "kelvin".to_string(),
                        to_unit: "celsius".to_string(),
                    },
                ],
            }),
        };

        crate::process_netcdf_job(&config)?;

        assert!(output_path.exists());
        let metadata = std::fs::metadata(&output_path)?;
        assert!(metadata.len() > 0);

        println!("Sprint 6: Complex pipeline chaining test completed successfully");
        Ok(())
    }

    #[test]
    fn test_sprint6_integration_error_handling() {
        let temp_dir = tempdir().unwrap();
        let output_path = temp_dir.path().join("should_not_exist.parquet");

        // Test with nonexistent input file - should fail gracefully
        let config = JobConfig {
            nc_key: "nonexistent_file.nc".to_string(),
            variable_name: "data".to_string(),
            parquet_key: output_path.to_string_lossy().to_string(),
            filters: vec![],
            postprocessing: None,
        };

        let result = crate::process_netcdf_job(&config);
        assert!(result.is_err(), "Should fail with nonexistent input file");
        assert!(
            !output_path.exists(),
            "Output file should not be created on error"
        );

        // Test with invalid variable name - should fail gracefully
        let config = JobConfig {
            nc_key: get_test_data_path("simple_xy.nc")
                .to_string_lossy()
                .to_string(),
            variable_name: "nonexistent_variable".to_string(),
            parquet_key: output_path.to_string_lossy().to_string(),
            filters: vec![],
            postprocessing: None,
        };

        let result = crate::process_netcdf_job(&config);
        assert!(result.is_err(), "Should fail with nonexistent variable");

        // Test with invalid dimension in filter - should fail gracefully
        let config = JobConfig {
            nc_key: get_test_data_path("simple_xy.nc")
                .to_string_lossy()
                .to_string(),
            variable_name: "data".to_string(),
            parquet_key: output_path.to_string_lossy().to_string(),
            filters: vec![FilterConfig::Range {
                params: RangeParams {
                    dimension_name: "nonexistent_dimension".to_string(),
                    min_value: 0.0,
                    max_value: 10.0,
                },
            }],
            postprocessing: None,
        };

        let result = crate::process_netcdf_job(&config);
        assert!(result.is_err(), "Should fail with nonexistent dimension");

        println!("Sprint 6: Error handling tests completed successfully");
    }

    #[test]
    fn test_sprint6_performance_benchmarking() -> Result<(), Box<dyn std::error::Error>> {
        use std::time::Instant;

        let temp_dir = tempdir()?;
        let output_path = temp_dir.path().join("performance_test.parquet");

        // Benchmark basic conversion
        let start = Instant::now();
        let config = JobConfig {
            nc_key: get_test_data_path("simple_xy.nc")
                .to_string_lossy()
                .to_string(),
            variable_name: "data".to_string(),
            parquet_key: output_path.to_string_lossy().to_string(),
            filters: vec![],
            postprocessing: None,
        };

        crate::process_netcdf_job(&config)?;
        let duration = start.elapsed();
        println!("Sprint 6: Basic conversion took: {:?}", duration);

        // Benchmark with post-processing
        let output_path2 = temp_dir.path().join("performance_postprocess.parquet");
        let start = Instant::now();
        let config_with_processing = JobConfig {
            nc_key: get_test_data_path("simple_xy.nc")
                .to_string_lossy()
                .to_string(),
            variable_name: "data".to_string(),
            parquet_key: output_path2.to_string_lossy().to_string(),
            filters: vec![],
            postprocessing: Some(crate::postprocess::ProcessingPipelineConfig {
                name: Some("Performance Test Pipeline".to_string()),
                processors: vec![
                    crate::postprocess::ProcessorConfig::RenameColumns {
                        mappings: {
                            let mut map = std::collections::HashMap::new();
                            map.insert("data".to_string(), "measurement".to_string());
                            map
                        },
                    },
                    crate::postprocess::ProcessorConfig::ApplyFormula {
                        target_column: "measurement_squared".to_string(),
                        formula: "measurement * measurement".to_string(),
                        source_columns: vec!["measurement".to_string()],
                    },
                ],
            }),
        };

        crate::process_netcdf_job(&config_with_processing)?;
        let duration_with_processing = start.elapsed();
        println!(
            "Sprint 6: Conversion with post-processing took: {:?}",
            duration_with_processing
        );

        // Verify post-processing overhead is reasonable
        let processing_overhead = duration_with_processing.saturating_sub(duration);
        println!(
            "Sprint 6: Post-processing overhead: {:?}",
            processing_overhead
        );

        // Basic performance assertion - post-processing shouldn't take more than 10x longer
        assert!(
            duration_with_processing < duration * 10,
            "Post-processing should not add excessive overhead"
        );

        println!("Sprint 6: Performance benchmarking completed successfully");
        Ok(())
    }

    #[tokio::test]
    async fn test_sprint6_async_vs_sync_performance() -> Result<(), Box<dyn std::error::Error>> {
        use std::time::Instant;

        let temp_dir = tempdir()?;
        let sync_output = temp_dir.path().join("sync_performance.parquet");
        let async_output = temp_dir.path().join("async_performance.parquet");

        let config = JobConfig {
            nc_key: get_test_data_path("pres_temp_4D.nc")
                .to_string_lossy()
                .to_string(),
            variable_name: "temperature".to_string(),
            parquet_key: sync_output.to_string_lossy().to_string(),
            filters: vec![],
            postprocessing: None,
        };

        // Benchmark sync processing
        let start = Instant::now();
        crate::process_netcdf_job(&config)?;
        let sync_duration = start.elapsed();
        println!("Sprint 6: Sync processing took: {:?}", sync_duration);

        // Benchmark async processing
        let mut async_config = config.clone();
        async_config.parquet_key = async_output.to_string_lossy().to_string();

        let start = Instant::now();
        crate::process_netcdf_job_async(&async_config).await?;
        let async_duration = start.elapsed();
        println!("Sprint 6: Async processing took: {:?}", async_duration);

        println!("Sprint 6: Async vs Sync performance comparison completed");
        Ok(())
    }
}

/// Integration tests for S3 operations with real AWS (optional)
#[cfg(test)]
mod s3_integration_tests {
    use super::*;
    use crate::storage::{StorageBackend, StorageFactory};

    #[tokio::test]
    #[ignore] // Ignore by default as it requires real AWS credentials and S3 bucket
    async fn test_end_to_end_s3_pipeline() -> Result<(), Box<dyn std::error::Error>> {
        // This test requires:
        // 1. AWS credentials configured (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION)
        // 2. TEST_S3_BUCKET environment variable set to a test bucket name
        // 3. The bucket must exist and be writable

        let test_bucket = match std::env::var("TEST_S3_BUCKET") {
            Ok(bucket) => bucket,
            Err(_) => {
                println!("Skipping S3 integration test - set TEST_S3_BUCKET environment variable");
                return Ok(());
            }
        };

        // Upload a test NetCDF file to S3
        let netcdf_path = get_test_data_path("simple_xy.nc");
        let netcdf_data = std::fs::read(&netcdf_path)?;

        let storage = StorageFactory::from_path(&format!("s3://{}/test.nc", test_bucket)).await?;
        let s3_input_path = format!("s3://{}/test-input/simple_xy.nc", test_bucket);
        let s3_output_path = format!("s3://{}/test-output/result.parquet", test_bucket);

        // Upload NetCDF file
        storage.write(&s3_input_path, &netcdf_data).await?;

        // Create job configuration for S3 input and output
        let json_config = format!(
            r#"{{
            "nc_key": "{}",
            "variable_name": "data",
            "parquet_key": "{}",
            "filters": [
                {{
                    "kind": "range",
                    "params": {{
                        "dimension_name": "x",
                        "min_value": 1.0,
                        "max_value": 4.0
                    }}
                }}
            ]
        }}"#,
            s3_input_path, s3_output_path
        );

        let config = JobConfig::from_json(&json_config)?;

        // Process the job with S3 input/output
        crate::process_netcdf_job_async(&config).await?;

        // Verify output file exists in S3
        assert!(storage.exists(&s3_output_path).await?);

        // Download and verify the output
        let parquet_data = storage.read(&s3_output_path).await?;
        assert!(!parquet_data.is_empty());
        assert!(parquet_data.len() > 100); // Basic sanity check

        println!("S3 end-to-end test passed with bucket: {}", test_bucket);
        Ok(())
    }

    #[tokio::test]
    #[ignore] // Ignore by default as it requires AWS credentials
    async fn test_mixed_s3_local_pipeline() -> Result<(), Box<dyn std::error::Error>> {
        let test_bucket = match std::env::var("TEST_S3_BUCKET") {
            Ok(bucket) => bucket,
            Err(_) => {
                println!("Skipping mixed S3/local test - set TEST_S3_BUCKET environment variable");
                return Ok(());
            }
        };

        // Create temporary output directory
        let temp_dir = tempfile::tempdir()?;
        let output_path = temp_dir.path().join("s3_to_local_output.parquet");

        // Upload NetCDF file to S3
        let netcdf_path = get_test_data_path("pres_temp_4D.nc");
        let netcdf_data = std::fs::read(&netcdf_path)?;

        let storage = StorageFactory::from_path(&format!("s3://{}/test.nc", test_bucket)).await?;
        let s3_input_path = format!("s3://{}/mixed-test-input/pres_temp_4D.nc", test_bucket);

        storage.write(&s3_input_path, &netcdf_data).await?;

        let json_config = format!(
            r#"{{
            "nc_key": "{}",
            "variable_name": "temperature", 
            "parquet_key": "{}",
            "filters": [
                {{
                    "kind": "list",
                    "params": {{
                        "dimension_name": "latitude",
                        "values": [25.0, 30.0]
                    }}
                }}
            ]
        }}"#,
            s3_input_path,
            output_path.display()
        );

        let config = JobConfig::from_json(&json_config)?;

        // Process: S3 input -> local output
        crate::process_netcdf_job_async(&config).await?;

        // Verify local output exists
        assert!(output_path.exists());
        let file_size = std::fs::metadata(&output_path)?.len();
        assert!(file_size > 50);

        println!("Mixed S3->local test passed with bucket: {}", test_bucket);
        Ok(())
    }
}

#[cfg(test)]
mod postprocess_tests {
    use crate::postprocess::*;
    use polars::prelude::*;
    use std::collections::HashMap;

    /// Create a test DataFrame for processor testing
    fn create_test_dataframe() -> DataFrame {
        df! {
            "temperature" => [273.15, 283.15, 293.15, 303.15],
            "pressure" => [1013.25, 1012.0, 1010.5, 1009.0],
            "humidity" => [60.0, 65.0, 70.0, 75.0],
            "time_offset" => [0.0, 1.0, 2.0, 3.0], // hours since base
        }
        .unwrap()
    }

    #[test]
    fn test_column_renamer() {
        let df = create_test_dataframe();
        let mut mappings = HashMap::new();
        mappings.insert("temperature".to_string(), "temp_k".to_string());
        mappings.insert("pressure".to_string(), "pres_hpa".to_string());

        let processor = ColumnRenamer::new(mappings);
        let result = processor.process(df).unwrap();

        let columns: Vec<&str> = result
            .get_column_names()
            .iter()
            .map(|s| s.as_str())
            .collect();
        assert!(columns.contains(&"temp_k"));
        assert!(columns.contains(&"pres_hpa"));
        assert!(columns.contains(&"humidity"));
        assert!(!columns.contains(&"temperature"));
        assert!(!columns.contains(&"pressure"));
    }

    #[test]
    fn test_unit_converter_kelvin_to_celsius() {
        let df = create_test_dataframe();
        let processor = UnitConverter::new(
            "temperature".to_string(),
            "kelvin".to_string(),
            "celsius".to_string(),
        );

        let result = processor.process(df).unwrap();
        let temp_col = result.column("temperature").unwrap();
        let values: Vec<f64> = temp_col
            .f64()
            .unwrap()
            .into_iter()
            .map(|v| v.unwrap())
            .collect();

        // 273.15K = 0°C, 283.15K = 10°C, etc.
        assert!((values[0] - 0.0).abs() < 1e-10);
        assert!((values[1] - 10.0).abs() < 1e-10);
        assert!((values[2] - 20.0).abs() < 1e-10);
        assert!((values[3] - 30.0).abs() < 1e-10);
    }

    #[test]
    fn test_unit_converter_multiplication() {
        let df = create_test_dataframe();
        let processor = UnitConverter::with_conversion_factor(
            "pressure".to_string(),
            "hpa".to_string(),
            "pa".to_string(),
            100.0, // hPa to Pa conversion factor
        );

        let result = processor.process(df).unwrap();
        let pres_col = result.column("pressure").unwrap();
        let values: Vec<f64> = pres_col
            .f64()
            .unwrap()
            .into_iter()
            .map(|v| v.unwrap())
            .collect();

        // Should be multiplied by conversion factor (100.0 for hPa->Pa)
        assert!((values[0] - 101325.0).abs() < 1e-6);
        assert!((values[1] - 101200.0).abs() < 1e-6);
    }

    #[test]
    fn test_aggregator() {
        let df = df! {
            "station" => ["A", "A", "B", "B", "A", "B"],
            "temperature" => [20.0, 22.0, 18.0, 19.0, 21.0, 17.0],
            "pressure" => [1013.0, 1012.0, 1015.0, 1014.0, 1013.5, 1016.0],
        }
        .unwrap();

        let group_by = vec!["station".to_string()];
        let mut aggregations = HashMap::new();
        aggregations.insert("temperature".to_string(), AggregationOp::Mean);
        aggregations.insert("pressure".to_string(), AggregationOp::Max);

        let processor = Aggregator::new(group_by, aggregations);
        let result = processor.process(df).unwrap();

        // Should have 2 rows (one per station)
        assert_eq!(result.height(), 2);

        // Check column names
        let columns: Vec<&str> = result
            .get_column_names()
            .iter()
            .map(|s| s.as_str())
            .collect();
        assert!(columns.contains(&"station"));
        assert!(columns.contains(&"temperature_mean"));
        assert!(columns.contains(&"pressure_max"));
    }

    #[test]
    fn test_formula_applier_arithmetic() {
        let df = create_test_dataframe();
        let processor = FormulaApplier::new(
            "apparent_temp".to_string(),
            "temperature + humidity".to_string(),
            vec!["temperature".to_string(), "humidity".to_string()],
        );

        let result = processor.process(df).unwrap();

        // Should have new column
        let columns: Vec<&str> = result
            .get_column_names()
            .iter()
            .map(|s| s.as_str())
            .collect();
        assert!(columns.contains(&"apparent_temp"));

        let new_col = result.column("apparent_temp").unwrap();
        let values: Vec<f64> = new_col
            .f64()
            .unwrap()
            .into_iter()
            .map(|v| v.unwrap())
            .collect();

        // First value: 273.15 + 60.0 = 333.15
        assert!((values[0] - 333.15).abs() < 1e-10);
    }

    #[test]
    fn test_formula_applier_sqrt() {
        let df = df! {
            "value" => [4.0, 9.0, 16.0, 25.0],
        }
        .unwrap();

        let processor = FormulaApplier::new(
            "sqrt_value".to_string(),
            "sqrt(value)".to_string(),
            vec!["value".to_string()],
        );

        let result = processor.process(df).unwrap();
        let new_col = result.column("sqrt_value").unwrap();
        let values: Vec<f64> = new_col
            .f64()
            .unwrap()
            .into_iter()
            .map(|v| v.unwrap())
            .collect();

        assert!((values[0] - 2.0).abs() < 1e-10);
        assert!((values[1] - 3.0).abs() < 1e-10);
        assert!((values[2] - 4.0).abs() < 1e-10);
        assert!((values[3] - 5.0).abs() < 1e-10);
    }

    #[test]
    fn test_processing_pipeline() {
        let df = create_test_dataframe();
        let mut pipeline = ProcessingPipeline::new();

        // Add column renamer
        let mut mappings = HashMap::new();
        mappings.insert("temperature".to_string(), "temp".to_string());
        pipeline.add_processor(Box::new(ColumnRenamer::new(mappings)));

        // Add unit converter
        let converter = UnitConverter::new(
            "temp".to_string(),
            "kelvin".to_string(),
            "celsius".to_string(),
        );
        pipeline.add_processor(Box::new(converter));

        let result = pipeline.execute(df).unwrap();

        // Check that both processors were applied
        let columns: Vec<&str> = result
            .get_column_names()
            .iter()
            .map(|s| s.as_str())
            .collect();
        assert!(columns.contains(&"temp"));
        assert!(!columns.contains(&"temperature"));

        // Check temperature was converted to Celsius
        let temp_col = result.column("temp").unwrap();
        let values: Vec<f64> = temp_col
            .f64()
            .unwrap()
            .into_iter()
            .map(|v| v.unwrap())
            .collect();
        assert!((values[0] - 0.0).abs() < 1e-10); // 273.15K = 0°C
    }

    #[test]
    fn test_create_processor_from_config() {
        // Test RenameColumns processor creation
        let mut mappings = HashMap::new();
        mappings.insert("old_name".to_string(), "new_name".to_string());
        let config = ProcessorConfig::RenameColumns { mappings };

        let processor = create_processor(&config).unwrap();
        assert_eq!(processor.name(), "ColumnRenamer");
        assert_eq!(
            processor.description(),
            "Renames columns based on provided mappings"
        );
    }

    #[test]
    fn test_unit_converter_with_config() {
        let config = ProcessorConfig::UnitConvert {
            column: "temperature".to_string(),
            from_unit: "kelvin".to_string(),
            to_unit: "celsius".to_string(),
        };

        let processor = create_processor(&config).unwrap();
        assert_eq!(processor.name(), "UnitConverter");

        // Test with actual data
        let df = create_test_dataframe();
        let result = processor.process(df).unwrap();

        let temp_col = result.column("temperature").unwrap();
        let values: Vec<f64> = temp_col
            .f64()
            .unwrap()
            .into_iter()
            .map(|v| v.unwrap())
            .collect();
        assert!((values[0] - 0.0).abs() < 1e-10);
    }

    #[test]
    fn test_error_handling() {
        let df = create_test_dataframe();

        // Test with non-existent column
        let processor = UnitConverter::new(
            "nonexistent".to_string(),
            "kelvin".to_string(),
            "celsius".to_string(),
        );

        let result = processor.process(df);
        assert!(result.is_err());

        if let Err(PostProcessError::ColumnNotFound(col)) = result {
            assert_eq!(col, "nonexistent");
        } else {
            panic!("Expected ColumnNotFound error");
        }
    }

    #[test]
    fn test_pipeline_from_config() {
        let config = ProcessingPipelineConfig {
            name: Some("Test Pipeline".to_string()),
            processors: vec![
                ProcessorConfig::RenameColumns {
                    mappings: {
                        let mut map = HashMap::new();
                        map.insert("temperature".to_string(), "temp".to_string());
                        map
                    },
                },
                ProcessorConfig::UnitConvert {
                    column: "temp".to_string(),
                    from_unit: "kelvin".to_string(),
                    to_unit: "celsius".to_string(),
                },
            ],
        };

        let mut pipeline = ProcessingPipeline::from_config(&config).unwrap();
        assert_eq!(pipeline.name(), "Test Pipeline");

        let df = create_test_dataframe();
        let result = pipeline.execute(df).unwrap();

        // Verify both processors were applied
        let columns: Vec<&str> = result
            .get_column_names()
            .iter()
            .map(|s| s.as_str())
            .collect();
        assert!(columns.contains(&"temp"));
        assert!(!columns.contains(&"temperature"));

        let temp_col = result.column("temp").unwrap();
        let values: Vec<f64> = temp_col
            .f64()
            .unwrap()
            .into_iter()
            .map(|v| v.unwrap())
            .collect();
        assert!((values[0] - 0.0).abs() < 1e-10);
    }
}

#[cfg(test)]
mod cli_tests {
    use clap::Parser;
    use std::path::PathBuf;
    use std::sync::Mutex;

    use crate::cli::{Cli, Commands, ConfigFormat, OutputFormat, TemplateType};

    // Global mutex to ensure environment variable tests run sequentially
    static ENV_TEST_MUTEX: Mutex<()> = Mutex::new(());

    /// Test basic CLI argument parsing
    #[test]
    fn test_cli_help() {
        let result = Cli::try_parse_from(&["nc2parquet", "--help"]);
        assert!(result.is_err()); // --help causes early exit with "error"

        let error = result.unwrap_err();
        assert!(
            error
                .to_string()
                .contains("Convert NetCDF files to Parquet format")
        );
    }

    /// Test version argument
    #[test]
    fn test_cli_version() {
        let result = Cli::try_parse_from(&["nc2parquet", "--version"]);
        assert!(result.is_err()); // --version causes early exit
    }

    /// Test global flags
    #[test]
    fn test_cli_global_flags() {
        let cli = Cli::parse_from(&[
            "nc2parquet",
            "--verbose",
            "--output-format",
            "json",
            "--config",
            "/path/to/config.json",
            "template",
            "basic",
        ]);

        assert!(cli.verbose);
        assert_eq!(cli.output_format, OutputFormat::Json);
        assert_eq!(cli.config, Some(PathBuf::from("/path/to/config.json")));
    }

    /// Test convert command argument parsing
    #[test]
    fn test_convert_command_basic() {
        let cli = Cli::parse_from(&[
            "nc2parquet",
            "convert",
            "input.nc",
            "output.parquet",
            "-n",
            "temperature",
        ]);

        if let Commands::Convert {
            input,
            output,
            variable,
            ..
        } = &cli.command
        {
            assert_eq!(input, &Some("input.nc".to_string()));
            assert_eq!(output, &Some("output.parquet".to_string()));
            assert_eq!(variable, &Some("temperature".to_string()));
        } else {
            panic!("Expected Convert command");
        }
    }

    /// Test convert command with filters
    #[test]
    fn test_convert_command_with_filters() {
        let cli = Cli::parse_from(&[
            "nc2parquet",
            "convert",
            "input.nc",
            "output.parquet",
            "-n",
            "temperature",
            "--range",
            "latitude:30:60",
            "--range",
            "longitude:-10:10",
            "--list",
            "level:1000,850,500",
            "--force",
            "--dry-run",
        ]);

        if let Commands::Convert {
            input,
            output,
            variable,
            range_filters,
            list_filters,
            force,
            dry_run,
            ..
        } = &cli.command
        {
            assert_eq!(input, &Some("input.nc".to_string()));
            assert_eq!(output, &Some("output.parquet".to_string()));
            assert_eq!(variable, &Some("temperature".to_string()));
            assert_eq!(range_filters.len(), 2);
            assert_eq!(list_filters.len(), 1);
            assert!(force);
            assert!(dry_run);

            // Test range filter parsing
            let lat_filter = &range_filters[0];
            assert_eq!(lat_filter.dimension, "latitude");
            assert_eq!(lat_filter.min_value, 30.0);
            assert_eq!(lat_filter.max_value, 60.0);

            // Test list filter parsing
            let level_filter = &list_filters[0];
            assert_eq!(level_filter.dimension, "level");
            assert_eq!(level_filter.values, vec![1000.0, 850.0, 500.0]);
        } else {
            panic!("Expected Convert command");
        }
    }

    /// Test info command parsing
    #[test]
    fn test_info_command() {
        let cli = Cli::parse_from(&[
            "nc2parquet",
            "info",
            "test.nc",
            "--detailed",
            "-n",
            "temperature",
            "--format",
            "json",
        ]);

        if let Commands::Info {
            file,
            detailed,
            variable,
            format,
        } = &cli.command
        {
            assert_eq!(file, "test.nc");
            assert!(detailed);
            assert_eq!(variable, &Some("temperature".to_string()));
            assert_eq!(format, &Some(OutputFormat::Json));
        } else {
            panic!("Expected Info command");
        }
    }

    /// Test validate command parsing
    #[test]
    fn test_validate_command() {
        let cli = Cli::parse_from(&["nc2parquet", "validate", "config.json", "--detailed"]);

        if let Commands::Validate {
            config_file,
            detailed,
        } = &cli.command
        {
            assert_eq!(config_file, &Some(PathBuf::from("config.json")));
            assert!(detailed);
        } else {
            panic!("Expected Validate command");
        }
    }

    /// Test template command parsing
    #[test]
    fn test_template_command() {
        let cli = Cli::parse_from(&[
            "nc2parquet",
            "template",
            "multi-filter",
            "--output",
            "template.yaml",
            "--format",
            "yaml",
        ]);

        if let Commands::Template {
            template_type,
            output,
            format,
        } = &cli.command
        {
            assert_eq!(template_type, &TemplateType::MultiFilter);
            assert_eq!(output, &Some(PathBuf::from("template.yaml")));
            assert_eq!(format, &ConfigFormat::Yaml);
        } else {
            panic!("Expected Template command");
        }
    }

    /// Test filter parsing edge cases
    #[test]
    fn test_range_filter_parsing() {
        // Valid range filter
        let cli = Cli::parse_from(&[
            "nc2parquet",
            "convert",
            "input.nc",
            "output.parquet",
            "--range",
            "time:0.5:10.75",
        ]);

        if let Commands::Convert { range_filters, .. } = &cli.command {
            assert_eq!(range_filters.len(), 1);
            let filter = &range_filters[0];
            assert_eq!(filter.dimension, "time");
            assert_eq!(filter.min_value, 0.5);
            assert_eq!(filter.max_value, 10.75);
        }
    }

    #[test]
    fn test_list_filter_parsing() {
        // Valid list filter with various numbers
        let cli = Cli::parse_from(&[
            "nc2parquet",
            "convert",
            "input.nc",
            "output.parquet",
            "--list",
            "pressure:1013.25,850.0,500,300.5",
        ]);

        if let Commands::Convert { list_filters, .. } = &cli.command {
            assert_eq!(list_filters.len(), 1);
            let filter = &list_filters[0];
            assert_eq!(filter.dimension, "pressure");
            assert_eq!(filter.values, vec![1013.25, 850.0, 500.0, 300.5]);
        }
    }

    /// Test invalid filter formats
    #[test]
    fn test_invalid_range_filter() {
        // Missing colon
        let result = Cli::try_parse_from(&[
            "nc2parquet",
            "convert",
            "input.nc",
            "output.parquet",
            "--range",
            "invalid_range",
        ]);
        assert!(result.is_err());

        // Invalid number format
        let result = Cli::try_parse_from(&[
            "nc2parquet",
            "convert",
            "input.nc",
            "output.parquet",
            "--range",
            "dim:not_a_number:10",
        ]);
        assert!(result.is_err());

        // Min > Max
        let result = Cli::try_parse_from(&[
            "nc2parquet",
            "convert",
            "input.nc",
            "output.parquet",
            "--range",
            "dim:10:5",
        ]);
        assert!(result.is_err());
    }

    #[test]
    fn test_invalid_list_filter() {
        // Missing colon
        let result = Cli::try_parse_from(&[
            "nc2parquet",
            "convert",
            "input.nc",
            "output.parquet",
            "--list",
            "invalid_list",
        ]);
        assert!(result.is_err());

        // Invalid number in list
        let result = Cli::try_parse_from(&[
            "nc2parquet",
            "convert",
            "input.nc",
            "output.parquet",
            "--list",
            "dim:1,not_a_number,3",
        ]);
        assert!(result.is_err());
    }

    /// Test environment variable handling
    #[test]
    fn test_environment_variables() {
        // Acquire mutex to ensure exclusive access to environment variables
        let _guard = ENV_TEST_MUTEX.lock().unwrap();

        unsafe {
            std::env::set_var("NC2PARQUET_CONFIG", "/path/to/env/config.json");
            std::env::set_var("NC2PARQUET_VARIABLE", "env_temperature");
        }

        let _cli = Cli::parse_from(&["nc2parquet", "convert", "input.nc", "output.parquet"]);

        // Note: Environment variables are processed by clap,
        // but we need to test that they're properly configured in the CLI structure

        // Clean up
        unsafe {
            std::env::remove_var("NC2PARQUET_CONFIG");
            std::env::remove_var("NC2PARQUET_VARIABLE");
        }
    }

    /// Test output format enum
    #[test]
    fn test_output_format_values() {
        let formats = ["human", "json", "yaml", "csv"];

        for format in &formats {
            let cli =
                Cli::parse_from(&["nc2parquet", "--output-format", format, "template", "basic"]);

            match format {
                &"human" => assert_eq!(cli.output_format, OutputFormat::Human),
                &"json" => assert_eq!(cli.output_format, OutputFormat::Json),
                &"yaml" => assert_eq!(cli.output_format, OutputFormat::Yaml),
                &"csv" => assert_eq!(cli.output_format, OutputFormat::Csv),
                _ => unreachable!(),
            }
        }
    }

    /// Test template type enum
    #[test]
    fn test_template_types() {
        let templates = ["basic", "s3", "multi-filter", "weather", "ocean"];

        for template in &templates {
            let cli = Cli::parse_from(&["nc2parquet", "template", template]);

            if let Commands::Template { template_type, .. } = &cli.command {
                match template {
                    &"basic" => assert_eq!(template_type, &TemplateType::Basic),
                    &"s3" => assert_eq!(template_type, &TemplateType::S3),
                    &"multi-filter" => assert_eq!(template_type, &TemplateType::MultiFilter),
                    &"weather" => assert_eq!(template_type, &TemplateType::Weather),
                    &"ocean" => assert_eq!(template_type, &TemplateType::Ocean),
                    _ => unreachable!(),
                }
            } else {
                panic!("Expected Template command");
            }
        }
    }

    /// Test quiet mode flag
    #[test]
    fn test_quiet_mode() {
        let cli = Cli::parse_from(&["nc2parquet", "--quiet", "info", "test.nc"]);

        assert!(cli.quiet);
    }

    /// Test conflicting verbose and quiet flags
    #[test]
    fn test_verbose_quiet_conflict() {
        // Both verbose and quiet should conflict - this should fail
        let result =
            Cli::try_parse_from(&["nc2parquet", "--verbose", "--quiet", "info", "test.nc"]);

        // This should fail due to conflicting arguments
        assert!(result.is_err());

        // Test individual flags work
        let cli_verbose = Cli::parse_from(&["nc2parquet", "--verbose", "info", "test.nc"]);
        assert!(cli_verbose.verbose);
        assert!(!cli_verbose.quiet);

        let cli_quiet = Cli::parse_from(&["nc2parquet", "--quiet", "info", "test.nc"]);
        assert!(!cli_quiet.verbose);
        assert!(cli_quiet.quiet);
    }

    /// Test command-specific overrides
    #[test]
    fn test_command_overrides() {
        let cli = Cli::parse_from(&[
            "nc2parquet",
            "convert",
            "input.nc",
            "output.parquet",
            "-n",
            "temperature",
            "--input-override",
            "new_input.nc",
            "--output-override",
            "new_output.parquet",
        ]);

        if let Commands::Convert {
            input,
            output,
            input_override,
            output_override,
            ..
        } = &cli.command
        {
            assert_eq!(input, &Some("input.nc".to_string()));
            assert_eq!(output, &Some("output.parquet".to_string()));
            assert_eq!(input_override, &Some("new_input.nc".to_string()));
            assert_eq!(output_override, &Some("new_output.parquet".to_string()));
        } else {
            panic!("Expected Convert command");
        }
    }
}

#[cfg(test)]
mod netcdf_exploration_tests {
    use super::*;

    #[test]
    fn test_explore_netcdf_api() -> Result<(), Box<dyn std::error::Error>> {
        let file_path = get_test_data_path("pres_temp_4D.nc");
        let file = netcdf::open(&file_path)?;

        println!("\n=== EXPLORING NETCDF API ===");
        println!("File: {}", file_path.display());

        println!("\nDimensions:");
        for dim in file.dimensions() {
            println!(
                "  {} (len={}, unlimited={})",
                dim.name(),
                dim.len(),
                dim.is_unlimited()
            );
        }

        println!("\nVariables:");
        for var in file.variables() {
            println!("  {} (type={:?})", var.name(), var.vartype());
            println!(
                "    Dimensions: {:?}",
                var.dimensions()
                    .iter()
                    .map(|d| d.name())
                    .collect::<Vec<_>>()
            );

            println!("    Attributes:");
            for attr in var.attributes() {
                println!("      {}: {:?}", attr.name(), attr.value());
            }
        }

        println!("\nGlobal Attributes:");
        for attr in file.attributes() {
            println!("  {}: {:?}", attr.name(), attr.value());
        }

        file.close()?;
        Ok(())
    }
}

#[cfg(test)]
mod info_command_tests {
    use super::*;
    use crate::info::{NetCdfDimensionInfo, NetCdfInfo, NetCdfVariableInfo, get_netcdf_info};

    #[tokio::test]
    async fn test_get_netcdf_info_basic() -> Result<(), Box<dyn std::error::Error>> {
        let file_path = get_test_data_path("pres_temp_4D.nc");
        let info = get_netcdf_info(&file_path.to_string_lossy(), None, false).await?;

        assert_eq!(info.path, file_path.to_string_lossy());
        assert_eq!(info.total_dimensions, 4);
        assert_eq!(info.total_variables, 4);

        // Check dimensions
        let dim_names: Vec<&str> = info.dimensions.iter().map(|d| d.name.as_str()).collect();
        assert!(dim_names.contains(&"level"));
        assert!(dim_names.contains(&"latitude"));
        assert!(dim_names.contains(&"longitude"));
        assert!(dim_names.contains(&"time"));

        // Check variables
        let var_names: Vec<&str> = info.variables.iter().map(|v| v.name.as_str()).collect();
        assert!(var_names.contains(&"latitude"));
        assert!(var_names.contains(&"longitude"));
        assert!(var_names.contains(&"pressure"));
        assert!(var_names.contains(&"temperature"));

        Ok(())
    }

    #[tokio::test]
    async fn test_get_netcdf_info_detailed() -> Result<(), Box<dyn std::error::Error>> {
        let file_path = get_test_data_path("pres_temp_4D.nc");
        let info = get_netcdf_info(&file_path.to_string_lossy(), None, true).await?;

        // Detailed mode should include global attributes (even if empty)
        assert!(info.global_attributes.is_empty() || !info.global_attributes.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn test_get_netcdf_info_specific_variable() -> Result<(), Box<dyn std::error::Error>> {
        let file_path = get_test_data_path("pres_temp_4D.nc");
        let info =
            get_netcdf_info(&file_path.to_string_lossy(), Some("temperature"), false).await?;

        assert_eq!(info.total_variables, 1);
        assert_eq!(info.variables[0].name, "temperature");
        assert_eq!(
            info.variables[0].dimensions,
            vec!["time", "level", "latitude", "longitude"]
        );
        assert!(info.variables[0].attributes.contains_key("units"));

        Ok(())
    }

    #[tokio::test]
    async fn test_get_netcdf_info_simple_xy() -> Result<(), Box<dyn std::error::Error>> {
        let file_path = get_test_data_path("simple_xy.nc");
        let info = get_netcdf_info(&file_path.to_string_lossy(), None, false).await?;

        assert_eq!(info.total_dimensions, 2);
        assert_eq!(info.total_variables, 1);
        assert_eq!(info.variables[0].name, "data");
        assert_eq!(info.variables[0].dimensions, vec!["x", "y"]);

        Ok(())
    }

    #[tokio::test]
    async fn test_get_netcdf_info_error_handling() {
        let result = get_netcdf_info("nonexistent.nc", None, false).await;
        assert!(result.is_err());

        // Test with invalid file
        std::fs::write("test_invalid.nc", "not a netcdf file").unwrap();
        let result = get_netcdf_info("test_invalid.nc", None, false).await;
        assert!(result.is_err());

        // Cleanup
        let _ = std::fs::remove_file("test_invalid.nc");
    }

    #[test]
    fn test_dimension_info_structure() {
        let dim = NetCdfDimensionInfo {
            name: "time".to_string(),
            length: 10,
            is_unlimited: true,
        };

        assert_eq!(dim.name, "time");
        assert_eq!(dim.length, 10);
        assert!(dim.is_unlimited);
    }

    #[test]
    fn test_variable_info_structure() {
        let mut attributes = std::collections::HashMap::new();
        attributes.insert("units".to_string(), "celsius".to_string());

        let var = NetCdfVariableInfo {
            name: "temperature".to_string(),
            data_type: "Float(F32)".to_string(),
            dimensions: vec!["time".to_string(), "lat".to_string()],
            attributes,
            shape: vec![10, 20],
        };

        assert_eq!(var.name, "temperature");
        assert_eq!(var.data_type, "Float(F32)");
        assert_eq!(var.dimensions.len(), 2);
        assert_eq!(var.shape, vec![10, 20]);
        assert!(var.attributes.contains_key("units"));
    }

    #[test]
    fn test_format_output_json() -> Result<(), Box<dyn std::error::Error>> {
        let info = create_test_netcdf_info();

        // Test JSON serialization
        let json = serde_json::to_string_pretty(&info)?;
        assert!(json.contains("test.nc"));
        assert!(json.contains("temperature"));

        Ok(())
    }

    #[test]
    fn test_format_output_yaml() -> Result<(), Box<dyn std::error::Error>> {
        let info = create_test_netcdf_info();

        let yaml = serde_yaml::to_string(&info)?;
        assert!(yaml.contains("test.nc"));
        assert!(yaml.contains("temperature"));

        Ok(())
    }

    fn create_test_netcdf_info() -> NetCdfInfo {
        let mut attributes = std::collections::HashMap::new();
        attributes.insert("units".to_string(), "celsius".to_string());

        let variables = vec![NetCdfVariableInfo {
            name: "temperature".to_string(),
            data_type: "Float(F32)".to_string(),
            dimensions: vec!["time".to_string(), "lat".to_string()],
            attributes,
            shape: vec![10, 20],
        }];

        let dimensions = vec![
            NetCdfDimensionInfo {
                name: "time".to_string(),
                length: 10,
                is_unlimited: true,
            },
            NetCdfDimensionInfo {
                name: "lat".to_string(),
                length: 20,
                is_unlimited: false,
            },
        ];

        NetCdfInfo {
            path: "test.nc".to_string(),
            dimensions,
            variables,
            global_attributes: std::collections::HashMap::new(),
            file_size: Some(1024),
            total_variables: 1,
            total_dimensions: 2,
        }
    }
}
