use std::env;
use std::time::Instant;
use nc2parquet::log::{show_greeting, config_echo, show_farewell_with_timing};
use nc2parquet::input::JobConfig;
use nc2parquet::process_netcdf_job;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    
    if args.len() < 2 {
        panic!("Usage: {} <config.json>", args[0]);
    } else {
        let config_path = &args[1];
        run(config_path)
    }
}

fn run(config_path: &str) -> Result<(), Box<dyn std::error::Error>> {
    let start_time = Instant::now();

    show_greeting(config_path);

    let config = JobConfig::from_file(config_path)?;

    config_echo(&config);
    process_netcdf_job(&config)?;
    
    let elapsed = start_time.elapsed();
    show_farewell_with_timing(elapsed);
    Ok(())
}
