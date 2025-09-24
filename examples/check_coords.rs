use netcdf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let file = netcdf::open("hres_fullrange_det_2t.nc")?;
    
    if let Some(lat_var) = file.variable("lat") {
        let lat_coords = lat_var.get::<f64, _>(..)?;
        println!("Lat range: {} to {}", lat_coords.iter().fold(f64::INFINITY, |a, &b| a.min(b)), lat_coords.iter().fold(f64::NEG_INFINITY, |a, &b| a.max(b)));
        println!("First 10 lat values: {:?}", &lat_coords.iter().take(10).collect::<Vec<_>>());
        println!("Lat around index 500: {:?}", &lat_coords.iter().skip(500).take(10).collect::<Vec<_>>());
    }
    
    if let Some(lon_var) = file.variable("lon") {
        let lon_coords = lon_var.get::<f64, _>(..)?;
        println!("Lon range: {} to {}", lon_coords.iter().fold(f64::INFINITY, |a, &b| a.min(b)), lon_coords.iter().fold(f64::NEG_INFINITY, |a, &b| a.max(b)));
        println!("First 10 lon values: {:?}", &lon_coords.iter().take(10).collect::<Vec<_>>());
        println!("Lon around index 300: {:?}", &lon_coords.iter().skip(300).take(10).collect::<Vec<_>>());
    }
    
    file.close()?;
    Ok(())
}