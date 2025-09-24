use netcdf;
use nc2parquet::filters::*;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== NetCDF Filter Trait Examples ===\n");
    
    let file = netcdf::open("hres_fullrange_det_2t.nc")?;
    
    // Example 1: Using filters with different return types through the unified trait
    println!("1. Using NCFilter trait with different filter types:");
    
    let time_filter = NCRangeFilter::new("time", 100.0, 200.0);
    let lat_filter = NCRangeFilter::new("lat", -10.0, -5.0);
    
    let time_result = NCFilter::apply(&time_filter, &file)?;
    let lat_result = NCFilter::apply(&lat_filter, &file)?;
    
    match time_result {
        FilterResult::Single(indices) => {
            println!("  Time filter: found {} indices", indices.len());
            println!("    First 5 indices: {:?}", &indices[..5.min(indices.len())]);
        },
        _ => println!("  Time filter: unexpected result type"),
    }
    
    match lat_result {
        FilterResult::Single(indices) => {
            println!("  Lat filter: found {} indices", indices.len());
            println!("    First 5 indices: {:?}", &indices[..5.min(indices.len())]);
        },
        _ => println!("  Lat filter: unexpected result type"),
    }
    
    // Example 2: Point filter returning coordinate pairs
    println!("\n2. Using NCPointFilter (returns coordinate pairs):");
    
    let point_filter = NCPointFilter::new(
        "lat", 
        "lon", 
        vec![(0.0, -60.0), (5.0, -50.0)], // (lat, lon) points
        2.0 // tolerance
    );
    
    let point_result = NCFilter::apply(&point_filter, &file)?;
    match point_result {
        FilterResult::Pairs(pairs) => {
            println!("  Point filter: found {} coordinate pairs", pairs.len());
            println!("    First 5 pairs: {:?}", &pairs[..5.min(pairs.len())]);
        },
        _ => println!("  Point filter: unexpected result type"),
    }
    
    // Example 3: List filter
    println!("\n3. Using NCListFilter (specific values):");
    
    let list_filter = NCListFilter::new("time", vec![1.0, 25.0, 50.0, 75.0, 100.0]);
    let list_result = NCFilter::apply(&list_filter, &file)?;
    
    match list_result {
        FilterResult::Single(indices) => {
            println!("  List filter: found {} indices", indices.len());
            println!("    All indices: {:?}", indices);
        },
        _ => println!("  List filter: unexpected result type"),
    }
    
    // Example 4: Polymorphic usage with different filter types
    println!("\n4. Polymorphic usage with Box<dyn NCFilter>:");
    
    let filters: Vec<Box<dyn NCFilter>> = vec![
        Box::new(NCRangeFilter::new("time", 50.0, 100.0)),
        Box::new(NCListFilter::new("time", vec![10.0, 20.0, 30.0])),
    ];
    
    for (i, filter) in filters.iter().enumerate() {
        let result = filter.apply(&file)?;
        match result {
            FilterResult::Single(indices) => {
                println!("  Filter {}: found {} single indices", i + 1, indices.len());
            },
            FilterResult::Pairs(pairs) => {
                println!("  Filter {}: found {} coordinate pairs", i + 1, pairs.len());
            },
            FilterResult::Triplets(triplets) => {
                println!("  Filter {}: found {} coordinate triplets", i + 1, triplets.len());
            },
        }
    }
    
    // Example 5: Using filter factory from JSON
    println!("\n5. Creating filters from JSON:");
    
    let range_filter_json = r#"{
        "type": "range",
        "dimension_name": "time",
        "min_value": 75.0,
        "max_value": 125.0
    }"#;
    
    let list_filter_json = r#"{
        "type": "list", 
        "dimension_name": "time",
        "values": [15.0, 45.0, 75.0]
    }"#;
    
    let point_filter_json = r#"{
        "type": "point",
        "lat_dimension_name": "lat",
        "lon_dimension_name": "lon", 
        "points": [[0.0, -50.0], [-5.0, -45.0]],
        "tolerance": 1.5
    }"#;
    
    let json_filters = vec![range_filter_json, list_filter_json, point_filter_json];
    
    for (i, json) in json_filters.iter().enumerate() {
        match filter_factory(json) {
            Ok(filter) => {
                let result = filter.apply(&file)?;
                match result {
                    FilterResult::Single(indices) => {
                        println!("  JSON Filter {}: {} single indices", i + 1, indices.len());
                    },
                    FilterResult::Pairs(pairs) => {
                        println!("  JSON Filter {}: {} coordinate pairs", i + 1, pairs.len());
                    },
                    FilterResult::Triplets(triplets) => {
                        println!("  JSON Filter {}: {} coordinate triplets", i + 1, triplets.len());
                    },
                }
            },
            Err(e) => println!("  JSON Filter {}: Error - {}", i + 1, e),
        }
    }
    
    // Example 6: Practical usage - combining filter results
    println!("\n6. Practical example - using filter results for data extraction:");
    
    let var = file.variable("t2").ok_or("Variable 't2' not found")?;
    let time_filter = NCRangeFilter::new("time", 100.0, 120.0);
    let time_result = NCFilter::apply(&time_filter, &file)?;
    
    if let FilterResult::Single(time_indices) = time_result {
        println!("  Extracting data for {} filtered time steps", time_indices.len());
        
        // Extract a small spatial subset for the filtered time steps
        for (i, &time_idx) in time_indices.iter().take(3).enumerate() {
            let data = var.get::<f32, _>((time_idx, 100..110, 100..110))?;
            let mean_temp: f32 = data.iter().sum::<f32>() / data.len() as f32;
            println!("    Time step {}: mean temperature = {:.2}K", time_idx, mean_temp);
        }
    }
    
    file.close()?;
    println!("\n=== All examples completed successfully! ===");
    
    Ok(())
}