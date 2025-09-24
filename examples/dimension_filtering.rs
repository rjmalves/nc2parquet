use netcdf;

/// Example demonstrating how to filter NetCDF data by dimension intervals
fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Open the NetCDF file
    let file = netcdf::open("hres_fullrange_det_2t.nc")?;
    
    // Get the variable we want to work with
    let var = file.variable("t2").ok_or("Variable 't2' not found")?;
    
    // Get the shape of the variable (time, lat, lon)
    let shape: Vec<usize> = var.dimensions().iter().map(|d| d.len()).collect();
    println!("Variable shape: [time: {}, lat: {}, lon: {}]", shape[0], shape[1], shape[2]);
    
    // Method 1: Filter by index ranges
    // ================================
    println!("\n=== Method 1: Filter by Index Ranges ===");
    
    // Define ranges for each dimension
    let time_start = 10;
    let time_end = 30;
    let lat_start = 200;
    let lat_end = 400;
    let lon_start = 100;
    let lon_end = 300;
    
    // Read the filtered subset
    let filtered_data = var.get::<f32, _>((
        time_start..time_end,
        lat_start..lat_end,
        lon_start..lon_end
    ))?;
    
    println!("Filtered data shape: {:?}", filtered_data.shape());
    println!("Time range: {} to {}", time_start, time_end - 1);
    println!("Lat range: {} to {}", lat_start, lat_end - 1);
    println!("Lon range: {} to {}", lon_start, lon_end - 1);
    
    // Method 2: Filter by coordinate values (if coordinate variables exist)
    // ====================================================================
    println!("\n=== Method 2: Filter by Coordinate Values ===");
    
    // Get coordinate variables
    if let Some(time_var) = file.variable("time") {
        if let Some(lat_var) = file.variable("lat") {
            if let Some(lon_var) = file.variable("lon") {
                // Read coordinate values
                let time_coords = time_var.get::<f64, _>(..)?;
                let lat_coords = lat_var.get::<f32, _>(..)?;
                let lon_coords = lon_var.get::<f32, _>(..)?;
                
                // Define coordinate ranges you want to filter by
                let target_time_min = 50.0;
                let target_time_max = 100.0;
                let target_lat_min = 45.0;
                let target_lat_max = 55.0;
                let target_lon_min = -10.0;
                let target_lon_max = 10.0;
                
                // Find indices that match the coordinate criteria
                let time_indices: Vec<usize> = time_coords
                    .iter()
                    .enumerate()
                    .filter(|(_, val)| **val >= target_time_min && **val <= target_time_max)
                    .map(|(idx, _)| idx)
                    .collect();
                
                let lat_indices: Vec<usize> = lat_coords
                    .iter()
                    .enumerate()
                    .filter(|(_, val)| **val >= target_lat_min && **val <= target_lat_max)
                    .map(|(idx, _)| idx)
                    .collect();
                
                let lon_indices: Vec<usize> = lon_coords
                    .iter()
                    .enumerate()
                    .filter(|(_, val)| **val >= target_lon_min && **val <= target_lon_max)
                    .map(|(idx, _)| idx)
                    .collect();
                
                println!("Found {} time indices matching criteria", time_indices.len());
                println!("Found {} lat indices matching criteria", lat_indices.len());
                println!("Found {} lon indices matching criteria", lon_indices.len());
                
                // For contiguous ranges, you can use the first and last indices
                if !time_indices.is_empty() && !lat_indices.is_empty() && !lon_indices.is_empty() {
                    let time_range = time_indices[0]..time_indices[time_indices.len()-1]+1;
                    let lat_range = lat_indices[0]..lat_indices[lat_indices.len()-1]+1;
                    let lon_range = lon_indices[0]..lon_indices[lon_indices.len()-1]+1;
                    
                    let coord_filtered_data = var.get::<f32, _>((
                        time_range.clone(),
                        lat_range.clone(), 
                        lon_range.clone()
                    ))?;
                    
                    println!("Coordinate-filtered data shape: {:?}", coord_filtered_data.shape());
                }
            }
        }
    }
    
    // Method 3: Single time slice with spatial filtering
    // =================================================
    println!("\n=== Method 3: Single Time Slice with Spatial Filter ===");
    
    let time_index = 75; // specific time step
    let spatial_subset = var.get::<f32, _>((
        time_index,
        200..500,  // lat range
        150..450   // lon range
    ))?;
    
    println!("Single time slice shape: {:?}", spatial_subset.shape());
    
    // Method 4: Multiple non-contiguous time steps
    // ===========================================
    println!("\n=== Method 4: Multiple Non-contiguous Time Steps ===");
    
    let selected_times = vec![10, 25, 50, 75, 100];
    let mut multi_time_data = Vec::new();
    
    for &time_idx in &selected_times {
        let time_slice = var.get::<f32, _>((
            time_idx,
            300..400,  // smaller spatial window for efficiency
            250..350
        ))?;
        multi_time_data.push(time_slice);
        println!("  Time {}: shape {:?}", time_idx, multi_time_data.last().unwrap().shape());
    }
    
    // Method 5: Conditional filtering based on data values
    // ===================================================
    println!("\n=== Method 5: Conditional Filtering by Data Values ===");
    
    // Read a small subset to demonstrate value-based filtering
    let sample_data = var.get::<f32, _>((0..5, 100..200, 100..200))?;
    
    // Find locations where temperature is above a threshold
    let temp_threshold = 295.0; // Kelvin
    let mut high_temp_locations = Vec::new();
    
    for (t, time_slice) in sample_data.outer_iter().enumerate() {
        for (i, row) in time_slice.outer_iter().enumerate() {
            for (j, &value) in row.iter().enumerate() {
                if value > temp_threshold {
                    high_temp_locations.push((t, i + 100, j + 100, value)); // adjust indices back to original
                }
            }
        }
    }
    
    println!("Found {} locations with temp > {} K", high_temp_locations.len(), temp_threshold);
    if !high_temp_locations.is_empty() {
        println!("Example high-temp location: time={}, lat_idx={}, lon_idx={}, temp={:.2}K", 
                 high_temp_locations[0].0, high_temp_locations[0].1, 
                 high_temp_locations[0].2, high_temp_locations[0].3);
    }
    
    file.close()?;
    println!("\nFiltering examples completed successfully!");
    
    Ok(())
}