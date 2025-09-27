//! # Data Extraction
//! 
//! This module handles the extraction of NetCDF data into Polars DataFrames
//! with sophisticated filtering and dimension management.
//! 
//! ## Key Components
//! 
//! - [`DimensionIndexManager`]: Manages dimension indices and filter intersections
//! - [`extract_data_to_dataframe`]: Main extraction function with filter application

use polars::prelude::*;
use crate::filters::{FilterResult, NCFilter};
use std::collections::{HashMap, HashSet};

/// Manages dimension indices and coordinate combinations during filtering operations.
/// 
/// This struct maintains the state of valid indices for each dimension and handles
/// the intersection of multiple filters while preserving coordinate relationships.
#[derive(Debug, Clone)]
pub struct DimensionIndexManager {
    dimension_indices: HashMap<String, HashSet<usize>>,
    dimension_order: Vec<String>,
    explicit_combinations: Option<Vec<Vec<usize>>>,
}

impl DimensionIndexManager {
    pub fn new(var: &netcdf::Variable) -> Result<Self, Box<dyn std::error::Error>> {
        let mut dimension_indices = HashMap::new();
        let mut dimension_order = Vec::new();
        
        for dim in var.dimensions() {
            let dim_name = dim.name().to_string();
            let dim_size = dim.len();
            
            let indices: HashSet<usize> = (0..dim_size).collect();
            dimension_indices.insert(dim_name.clone(), indices);
            dimension_order.push(dim_name);
        }
        
        Ok(DimensionIndexManager {
            dimension_indices,
            dimension_order,
            explicit_combinations: None,
        })
    }
    
    pub fn apply_filter_result(&mut self, result: &FilterResult) -> Result<(), Box<dyn std::error::Error>> {
        match result {
            FilterResult::Single { dimension, indices } => {
                if let Some(current_indices) = self.dimension_indices.get_mut(dimension) {
                    let new_indices: HashSet<usize> = indices.iter().cloned().collect();
                    *current_indices = current_indices.intersection(&new_indices).cloned().collect();
                } else {
                    return Err(format!("Unknown dimension: {}", dimension).into());
                }
            },
            
            FilterResult::Pairs { lat_dimension, lon_dimension, pairs } => {
                self.apply_explicit_pairs(lat_dimension, lon_dimension, pairs)?;
            },
            
            FilterResult::Triplets { time_dimension, lat_dimension, lon_dimension, triplets } => {
                self.apply_explicit_triplets(time_dimension, lat_dimension, lon_dimension, triplets)?;
            },
        }
        Ok(())
    }
    
    fn apply_explicit_pairs(&mut self, lat_dim: &str, lon_dim: &str, pairs: &[(usize, usize)]) -> Result<(), Box<dyn std::error::Error>> {
        let lat_pos = self.dimension_order.iter().position(|d| d == lat_dim)
            .ok_or(format!("Dimension {} not found", lat_dim))?;
        let lon_pos = self.dimension_order.iter().position(|d| d == lon_dim)
            .ok_or(format!("Dimension {} not found", lon_dim))?;
        
        let mut combinations = Vec::new();
        
        let other_dimensions: Vec<(usize, Vec<usize>)> = self.dimension_order.iter()
            .enumerate()
            .filter(|(pos, _)| *pos != lat_pos && *pos != lon_pos)
            .map(|(pos, dim_name)| {
                let indices: Vec<usize> = self.dimension_indices[dim_name].iter().cloned().collect();
                (pos, indices)
            })
            .collect();
        
        self.generate_combinations_with_pairs(&other_dimensions, pairs, lat_pos, lon_pos, &mut Vec::new(), 0, &mut combinations);
        
        self.explicit_combinations = Some(combinations);
        Ok(())
    }
    
    fn apply_explicit_triplets(&mut self, time_dim: &str, lat_dim: &str, lon_dim: &str, triplets: &[(usize, usize, usize)]) -> Result<(), Box<dyn std::error::Error>> {
        let time_pos = self.dimension_order.iter().position(|d| d == time_dim)
            .ok_or(format!("Dimension {} not found", time_dim))?;
        let lat_pos = self.dimension_order.iter().position(|d| d == lat_dim)
            .ok_or(format!("Dimension {} not found", lat_dim))?;
        let lon_pos = self.dimension_order.iter().position(|d| d == lon_dim)
            .ok_or(format!("Dimension {} not found", lon_dim))?;
        
        let mut combinations = Vec::new();
        for &(time_idx, lat_idx, lon_idx) in triplets {
            let mut coord = vec![0; self.dimension_order.len()];
            coord[time_pos] = time_idx;
            coord[lat_pos] = lat_idx;
            coord[lon_pos] = lon_idx;
            combinations.push(coord);
        }
        
        self.explicit_combinations = Some(combinations);
        Ok(())
    }
    
    fn generate_combinations_with_pairs(
        &self,
        other_dims: &[(usize, Vec<usize>)],
        pairs: &[(usize, usize)],
        lat_pos: usize,
        lon_pos: usize,
        current: &mut Vec<usize>,
        other_dim_idx: usize,
        results: &mut Vec<Vec<usize>>,
    ) {
        if other_dim_idx >= other_dims.len() {
            for &(lat_idx, lon_idx) in pairs {
                let mut coord = vec![0; self.dimension_order.len()];
                for (i, &val) in current.iter().enumerate() {
                    coord[other_dims[i].0] = val;
                }
                coord[lat_pos] = lat_idx;
                coord[lon_pos] = lon_idx;
                results.push(coord);
            }
            return;
        }
        
        let (_, ref indices) = other_dims[other_dim_idx];
        for &idx in indices {
            current.push(idx);
            self.generate_combinations_with_pairs(other_dims, pairs, lat_pos, lon_pos, current, other_dim_idx + 1, results);
            current.pop();
        }
    }
    
    pub fn get_dimension_indices(&self, dim_name: &str) -> Option<&HashSet<usize>> {
        self.dimension_indices.get(dim_name)
    }
    
    pub fn get_dimension_order(&self) -> &Vec<String> {
        &self.dimension_order
    }
    
    pub fn get_all_coordinate_combinations(&self) -> Vec<Vec<usize>> {
        if let Some(ref explicit) = self.explicit_combinations {
            explicit.clone()
        } else {
            let mut result = Vec::new();
            self.generate_combinations(&mut Vec::new(), 0, &mut result);
            result
        }
    }
    
    fn generate_combinations(&self, current: &mut Vec<usize>, dim_index: usize, result: &mut Vec<Vec<usize>>) {
        if dim_index >= self.dimension_order.len() {
            result.push(current.clone());
            return;
        }
        
        let dim_name = &self.dimension_order[dim_index];
        if let Some(indices) = self.dimension_indices.get(dim_name) {
            let mut sorted_indices: Vec<usize> = indices.iter().cloned().collect();
            sorted_indices.sort();
            
            for &idx in &sorted_indices {
                current.push(idx);
                self.generate_combinations(current, dim_index + 1, result);
                current.pop();
            }
        }
    }
}

/// Extracts NetCDF data to a Polars DataFrame with filter application.
/// 
/// This is the main extraction function that:
/// 1. Creates a dimension index manager for the variable
/// 2. Applies all filters with intersection logic
/// 3. Extracts data only for valid coordinate combinations
/// 4. Returns a DataFrame with coordinate and variable data
/// 
/// # Arguments
/// 
/// * `file` - The opened NetCDF file
/// * `var` - The NetCDF variable to extract data from
/// * `var_name` - Name of the variable for DataFrame column naming
/// * `filters` - Vector of filters to apply
/// 
/// # Returns
/// 
/// Returns a DataFrame containing coordinate columns and the variable data,
/// or an error if extraction fails.
pub fn extract_data_to_dataframe(
    file: &netcdf::File,
    var: &netcdf::Variable,
    var_name: &str,
    filters: &Vec<Box<dyn NCFilter>>,
) -> Result<DataFrame, Box<dyn std::error::Error>> {
    
    let mut dim_manager = DimensionIndexManager::new(var)?;
    for (_, filter) in filters.iter().enumerate() {
        let result = filter.apply(&file)?;
        dim_manager.apply_filter_result(&result)?;
    }
    extract_data_with_dimension_manager(file, var, var_name, &dim_manager)
}

fn extract_data_with_dimension_manager(
    file: &netcdf::File,
    var: &netcdf::Variable,
    var_name: &str,
    dim_manager: &DimensionIndexManager,
) -> Result<DataFrame, Box<dyn std::error::Error>> {
    
    let dimension_order = dim_manager.get_dimension_order();
    let coordinate_vars: HashMap<String, Vec<f64>> = get_coordinate_variables(file, dimension_order)?;
    let combinations = dim_manager.get_all_coordinate_combinations();

    let mut data_columns: HashMap<String, Vec<f64>> = HashMap::new();
    let mut variable_values = Vec::new();
    
    for dim_name in dimension_order {
        data_columns.insert(dim_name.clone(), Vec::new());
    }
    
    for combination in &combinations {
        for (i, dim_name) in dimension_order.iter().enumerate() {
            let idx = combination[i];
            
            let coord_value = coordinate_vars
                .get(dim_name)
                .map(|coords| coords[idx])
                .unwrap_or(idx as f64);
            data_columns.get_mut(dim_name).unwrap().push(coord_value);
        }
        
        let indices: Vec<usize> = combination.clone();
        let value = extract_variable_value(var, &indices)?;
        variable_values.push(value);
    }

    let mut columns = Vec::new();
    
    for dim_name in dimension_order {
        let values = data_columns.remove(dim_name).unwrap();
        columns.push(Series::new(dim_name.as_str().into(), values).into());
    }
    
    columns.push(Series::new(var_name.into(), variable_values).into());
    
    let df = DataFrame::new(columns)?;
    Ok(df)
}

fn get_coordinate_variables(
    file: &netcdf::File,
    dimension_order: &[String],
) -> Result<HashMap<String, Vec<f64>>, Box<dyn std::error::Error>> {
    let mut coordinate_vars = HashMap::new();
    
    for dim_name in dimension_order {
        if let Some(coord_var) = file.variable(dim_name) {
            if let Ok(coords_array) = coord_var.get::<f64, _>(..) {
                let coords_vec: Vec<f64> = coords_array.iter().cloned().collect();
                coordinate_vars.insert(dim_name.clone(), coords_vec);
            }
        }
    }
    
    Ok(coordinate_vars)
}

fn extract_variable_value(
    var: &netcdf::Variable,
    indices: &[usize],
) -> Result<f32, Box<dyn std::error::Error>> {
    match indices.len() {
        1 => {
            let value_array = var.get::<f32, _>(indices[0])?;
            Ok(value_array[[]])
        },
        2 => {
            let value_array = var.get::<f32, _>((indices[0], indices[1]))?;
            Ok(value_array[[]])
        },
        3 => {
            let value_array = var.get::<f32, _>((indices[0], indices[1], indices[2]))?;
            Ok(value_array[[]])
        },
        4 => {
            let value_array = var.get::<f32, _>((indices[0], indices[1], indices[2], indices[3]))?;
            Ok(value_array[[]])
        },
        _ => Err(format!("Unsupported number of dimensions: {}", indices.len()).into())
    }
}
