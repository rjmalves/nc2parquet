use polars::prelude::*;
use crate::filters::{FilterResult, NCFilter};
use crate::log::show_filter_results;

pub fn extract_data_to_dataframe(
    file: &netcdf::File,
    var: &netcdf::Variable,
    var_name: &str,
    filters: &Vec<Box<dyn NCFilter>>,
) -> Result<DataFrame, Box<dyn std::error::Error>> {
    
    let mut filter_results = Vec::new();

    for (_, filter) in filters.iter().enumerate() {
        let result = filter.apply(&file)?;
        filter_results.push(result);
    }

    show_filter_results(&filter_results);
    
    if filter_results.is_empty() {
        return Err("No filters provided".into());
    }
    
    // Handle different filter result types
    match &filter_results[0] {
        FilterResult::Single(_indices) => {
            // For single dimension filters, we need to determine which dimension they apply to
            // This is a simplified approach - in practice, you might want to track which dimension each filter applies to
            Err("Single dimension filters not yet implemented in data extraction".into())
        },
        
        FilterResult::Pairs(pairs) => {
            extract_2d_point_data(file, var, var_name, pairs)
        },
        
        FilterResult::Triplets(triplets) => {
            extract_3d_point_data(file, var, var_name, triplets)
        },
    }
}

fn extract_2d_point_data(
    file: &netcdf::File,
    var: &netcdf::Variable,
    var_name: &str,
    pairs: &[(usize, usize)],
) -> Result<DataFrame, Box<dyn std::error::Error>> {
    
    let var_shape: Vec<usize> = var.dimensions().iter().map(|d| d.len()).collect();
    
    if var_shape.len() != 3 {
        return Err(format!("Expected 3D variable for 2D point extraction, got {}D", var_shape.len()).into());
    }
    
    let time_dim = var_shape[0];
    
    let lat_coords = if let Some(lat_var) = file.variable("lat") {
        lat_var.get::<f64, _>(..).ok()
    } else { None };
    
    let lon_coords = if let Some(lon_var) = file.variable("lon") {
        lon_var.get::<f64, _>(..).ok()
    } else { None };
    
    let time_coords = if let Some(time_var) = file.variable("time") {
        time_var.get::<f64, _>(..).ok()
    } else { None };
    
    let mut time_values = Vec::new();
    let mut lat_values = Vec::new();
    let mut lon_values = Vec::new();
    let mut variable_values = Vec::new();
    
    println!("Extracting data for {} coordinate pairs across {} time steps", pairs.len(), time_dim);
    
    for (lat_idx, lon_idx) in pairs {
        let lat_coord = lat_coords.as_ref().map(|coords| coords[*lat_idx]).unwrap_or(*lat_idx as f64);
        let lon_coord = lon_coords.as_ref().map(|coords| coords[*lon_idx]).unwrap_or(*lon_idx as f64);
        
        for time_idx in 0..time_dim {
            let time_coord = time_coords.as_ref().map(|coords| coords[time_idx]).unwrap_or(time_idx as f64);
            let value_array = var.get::<f32, _>((time_idx, *lat_idx, *lon_idx))?;
            let value = value_array[[]];
            
            time_values.push(time_coord);
            lat_values.push(lat_coord);
            lon_values.push(lon_coord);
            variable_values.push(value);
        }
    }
    
    // Create DataFrame
    let df = df! {
        "time" => time_values,
        "lat" => lat_values,
        "lon" => lon_values,
        var_name => variable_values,
    }?;
    
    Ok(df)
}

fn extract_3d_point_data(
    file: &netcdf::File,
    var: &netcdf::Variable,
    var_name: &str,
    triplets: &[(usize, usize, usize)],
) -> Result<DataFrame, Box<dyn std::error::Error>> {
    
    // Get coordinate variables for metadata
    let lat_coords = if let Some(lat_var) = file.variable("lat") {
        lat_var.get::<f64, _>(..).ok()
    } else { None };
    
    let lon_coords = if let Some(lon_var) = file.variable("lon") {
        lon_var.get::<f64, _>(..).ok()
    } else { None };
    
    let time_coords = if let Some(time_var) = file.variable("time") {
        time_var.get::<f64, _>(..).ok()
    } else { None };
    
    // Prepare data vectors
    let mut time_values = Vec::new();
    let mut lat_values = Vec::new();
    let mut lon_values = Vec::new();
    let mut time_indices = Vec::new();
    let mut lat_indices = Vec::new();
    let mut lon_indices = Vec::new();
    let mut variable_values = Vec::new();
    
    println!("Extracting data for {} coordinate triplets", triplets.len());
    
    // Extract data for each coordinate triplet
    for (time_idx, lat_idx, lon_idx) in triplets {
        let time_coord = time_coords.as_ref().map(|coords| coords[*time_idx]).unwrap_or(*time_idx as f64);
        let lat_coord = lat_coords.as_ref().map(|coords| coords[*lat_idx]).unwrap_or(*lat_idx as f64);
        let lon_coord = lon_coords.as_ref().map(|coords| coords[*lon_idx]).unwrap_or(*lon_idx as f64);
        
        // Read single point value
        let value_array = var.get::<f32, _>((*time_idx, *lat_idx, *lon_idx))?;
        let value = value_array[[]];
        
        time_values.push(time_coord);
        lat_values.push(lat_coord);
        lon_values.push(lon_coord);
        time_indices.push(*time_idx as i32);
        lat_indices.push(*lat_idx as i32);
        lon_indices.push(*lon_idx as i32);
        variable_values.push(value);
    }
    
    // Create DataFrame
    let df = df! {
        "time" => time_values,
        "lat" => lat_values,
        "lon" => lon_values,
        "time_idx" => time_indices,
        "lat_idx" => lat_indices,
        "lon_idx" => lon_indices,
        var_name => variable_values,
    }?;
    
    Ok(df)
}
