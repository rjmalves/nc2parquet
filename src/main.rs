//! # nc2parquet CLI Application
//!
//! Command-line interface for converting NetCDF files to Parquet format
//! with advanced filtering capabilities and cloud storage support.

use anyhow::{Context, Result};
use clap::{CommandFactory, Parser};
use clap_complete::{Shell, generate};
use indicatif::{ProgressBar, ProgressStyle};
use log::{debug, error, info, warn};
use std::path::Path;
use std::process;
use std::time::Duration;

use nc2parquet::{
    cli::*,
    input::{FilterConfig, JobConfig},
    postprocess::{ProcessingPipelineConfig, ProcessorConfig},
    process_netcdf_job, process_netcdf_job_async,
    storage::{StorageBackend, StorageFactory},
};

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    // Initialize logging
    init_logging(&cli);

    debug!("CLI arguments: {:?}", std::env::args().collect::<Vec<_>>());

    let result = match &cli.command {
        Commands::Convert { .. } => handle_convert_command(&cli).await,
        Commands::Validate { .. } => handle_validate_command(&cli).await,
        Commands::Info { .. } => handle_info_command(&cli).await,
        Commands::Template { .. } => handle_template_command(&cli).await,
        Commands::Completions { .. } => handle_completions_command(&cli).await,
    };

    match result {
        Ok(()) => {
            debug!("Command completed successfully");
        }
        Err(e) => {
            error!("Command failed: {}", e);

            // Show error chain if verbose
            if cli.verbose {
                let mut cause = e.source();
                while let Some(err) = cause {
                    error!("  Caused by: {}", err);
                    cause = err.source();
                }
            }

            process::exit(1);
        }
    }
}

/// Initialize logging based on CLI arguments
fn init_logging(cli: &Cli) {
    let log_level = if cli.quiet {
        "error"
    } else if cli.verbose {
        "debug"
    } else {
        "info"
    };

    unsafe {
        std::env::set_var("RUST_LOG", format!("nc2parquet={}", log_level));
    }
    env_logger::init();

    debug!("Logging initialized at {} level", log_level);
}

/// Handle the convert subcommand
async fn handle_convert_command(cli: &Cli) -> Result<()> {
    if let Commands::Convert {
        input,
        output,
        variable,
        input_override,
        output_override,
        range_filters,
        list_filters,
        point2d_filters,
        point3d_filters,
        force,
        dry_run,
        rename_columns,
        unit_conversions,
        kelvin_to_celsius,
        formulas,
    } = &cli.command
    {
        info!("Starting NetCDF to Parquet conversion");

        // Load configuration
        let mut config = load_configuration(cli, input, output, variable)?;

        // Apply command line overrides
        if let Some(input_path) = input_override {
            config.nc_key = input_path.clone();
            debug!("Overriding input path: {}", input_path);
        }

        if let Some(output_path) = output_override {
            config.parquet_key = output_path.clone();
            debug!("Overriding output path: {}", output_path);
        }

        // Merge CLI and environment variable filters
        let (
            merged_range_filters,
            merged_list_filters,
            merged_point2d_filters,
            merged_point3d_filters,
        ) = merge_filters(
            (**range_filters).clone(),
            (**list_filters).clone(),
            (**point2d_filters).clone(),
            (**point3d_filters).clone(),
        )
        .map_err(|e| anyhow::anyhow!("Filter parsing error: {}", e))?;

        // Add merged filters to configuration
        for range_filter in &merged_range_filters {
            let filter_config = range_filter.clone().into();
            config.filters.push(filter_config);
            debug!(
                "Added range filter: {}:{}-{}",
                range_filter.dimension, range_filter.min_value, range_filter.max_value
            );
        }

        for list_filter in &merged_list_filters {
            let filter_config = list_filter.clone().into();
            config.filters.push(filter_config);
            debug!(
                "Added list filter: {}:{:?}",
                list_filter.dimension, list_filter.values
            );
        }

        for point2d_filter in &merged_point2d_filters {
            let filter_config = point2d_filter.clone().into();
            config.filters.push(filter_config);
            debug!(
                "Added 2D point filter: {},{} at ({},{}) tolerance={}",
                point2d_filter.lat_dimension,
                point2d_filter.lon_dimension,
                point2d_filter.lat,
                point2d_filter.lon,
                point2d_filter.tolerance
            );
        }

        for point3d_filter in &merged_point3d_filters {
            let filter_config = point3d_filter.clone().into();
            config.filters.push(filter_config);
            debug!(
                "Added 3D point filter: {},{},{} at ({},{},{}) tolerance={}",
                point3d_filter.time_dimension,
                point3d_filter.lat_dimension,
                point3d_filter.lon_dimension,
                point3d_filter.time,
                point3d_filter.lat,
                point3d_filter.lon,
                point3d_filter.tolerance
            );
        }

        // Build post-processing pipeline from CLI arguments
        if !rename_columns.is_empty()
            || !unit_conversions.is_empty()
            || !kelvin_to_celsius.is_empty()
            || !formulas.is_empty()
        {
            use std::collections::HashMap;

            let mut processors = Vec::new();

            // Add column rename processors
            if !rename_columns.is_empty() {
                let mut mappings = HashMap::new();
                for rename in rename_columns.iter() {
                    mappings.insert(rename.old_name.clone(), rename.new_name.clone());
                    debug!(
                        "Added column rename: {} -> {}",
                        rename.old_name, rename.new_name
                    );
                }
                processors.push(ProcessorConfig::RenameColumns { mappings });
            }

            // Add unit conversion processors
            for unit_conversion in unit_conversions.iter() {
                processors.push(ProcessorConfig::UnitConvert {
                    column: unit_conversion.column.clone(),
                    from_unit: unit_conversion.from_unit.clone(),
                    to_unit: unit_conversion.to_unit.clone(),
                });
                debug!(
                    "Added unit conversion: {} from {} to {}",
                    unit_conversion.column, unit_conversion.from_unit, unit_conversion.to_unit
                );
            }

            // Add Kelvin to Celsius conversions
            for column in kelvin_to_celsius {
                processors.push(ProcessorConfig::UnitConvert {
                    column: column.clone(),
                    from_unit: "kelvin".to_string(),
                    to_unit: "celsius".to_string(),
                });
                debug!("Added Kelvin to Celsius conversion for column: {}", column);
            }

            // Add formula processors
            for formula in formulas.iter() {
                processors.push(ProcessorConfig::ApplyFormula {
                    target_column: formula.target_column.clone(),
                    formula: formula.formula.clone(),
                    source_columns: formula.source_columns.clone(),
                });
                debug!(
                    "Added formula: {} = {} (sources: {:?})",
                    formula.target_column, formula.formula, formula.source_columns
                );
            }

            if !processors.is_empty() {
                let pipeline_config = ProcessingPipelineConfig {
                    name: Some("CLI Pipeline".to_string()),
                    processors,
                };
                config.postprocessing = Some(pipeline_config);
                info!(
                    "Created post-processing pipeline with {} processors",
                    config.postprocessing.as_ref().unwrap().processors.len()
                );
            }
        }

        // Validate configuration
        validate_config(&config).await?;

        // Check output file exists
        if !force && !*dry_run {
            check_output_overwrite(&config.parquet_key).await?;
        }

        if *dry_run {
            info!("Dry run mode - configuration validated successfully");
            print_config_summary(&config, &cli.output_format);
            return Ok(());
        }

        // Show progress and process
        info!("Processing: {} -> {}", config.nc_key, config.parquet_key);
        info!("Variable: {}", config.variable_name);
        info!("Filters: {} configured", config.filters.len());

        // Create progress bar for non-quiet mode
        let progress = if cli.quiet {
            None
        } else {
            let pb = ProgressBar::new_spinner();
            pb.set_style(
                ProgressStyle::default_spinner()
                    .template("{spinner:.green} {msg}")
                    .unwrap()
                    .tick_strings(&["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]),
            );
            pb.set_message("Initializing conversion...");
            Some(pb)
        };

        // Process the file
        let start_time = std::time::Instant::now();

        if let Some(ref pb) = progress {
            pb.set_message("Reading NetCDF file...");
        }

        if needs_async_processing(&config) {
            if let Some(ref pb) = progress {
                pb.set_message("Processing with async pipeline...");
            }
            process_netcdf_job_async(&config)
                .await
                .map_err(|e| anyhow::anyhow!("{}", e))
                .context("Failed to process NetCDF file with async pipeline")?;
        } else {
            if let Some(ref pb) = progress {
                pb.set_message("Processing with sync pipeline...");
            }
            process_netcdf_job(&config)
                .map_err(|e| anyhow::anyhow!("{}", e))
                .context("Failed to process NetCDF file")?;
        }

        let duration = start_time.elapsed();

        if let Some(pb) = progress {
            let success_message =
                format!("✅ Conversion completed in {:.2}s", duration.as_secs_f64());
            pb.finish_with_message(success_message);
        }

        // Enhanced timing information
        if duration.as_secs() > 1 {
            info!(
                "Conversion completed in {:.2} seconds",
                duration.as_secs_f64()
            );
        } else {
            info!(
                "Conversion completed in {:.0} milliseconds",
                duration.as_millis()
            );
        }

        // Show performance metrics in verbose mode
        if cli.verbose
            && let Ok(file_size) = get_file_size(&config.nc_key).await
        {
            let throughput = file_size as f64 / duration.as_secs_f64() / 1_048_576.0; // MB/s
            info!("Input file size: {:.2} MB", file_size as f64 / 1_048_576.0);
            info!("Processing throughput: {:.2} MB/s", throughput);
        }

        // Show output information
        show_output_info(&config.parquet_key, &cli.output_format).await?;
    } else {
        unreachable!("Convert command handler called with wrong command type");
    }

    Ok(())
}

/// Handle the validate subcommand  
async fn handle_validate_command(cli: &Cli) -> Result<()> {
    if let Commands::Validate {
        config_file,
        detailed,
    } = &cli.command
    {
        info!("Validating configuration");

        // Create progress spinner for validation
        let progress = if cli.quiet {
            None
        } else {
            let progress = ProgressBar::new_spinner();
            progress.enable_steady_tick(Duration::from_millis(80));
            progress.set_style(
                ProgressStyle::with_template("{spinner:.green} {msg}")
                    .unwrap()
                    .tick_strings(&["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]),
            );
            progress.set_message("Validating configuration...");
            Some(progress)
        };

        // Load and validate configuration
        let config = load_configuration(
            cli,
            &config_file
                .as_ref()
                .map(|p| p.to_string_lossy().to_string()),
            &None,
            &None,
        )?;

        if let Some(pb) = &progress {
            pb.set_message("Running configuration checks...");
        }

        validate_config(&config).await?;

        if let Some(pb) = &progress {
            pb.finish_with_message("✓ Configuration valid!");
        }

        if *detailed {
            show_detailed_validation(&config, &cli.output_format).await?;
        } else {
            println!("Configuration validation passed successfully");
        }

        Ok(())
    } else {
        unreachable!("Validate command handler called with wrong command type");
    }
}

/// Handle the info subcommand
async fn handle_info_command(cli: &Cli) -> Result<()> {
    if let Commands::Info {
        file,
        detailed,
        variable,
        format,
    } = &cli.command
    {
        info!("Gathering file information: {}", file);

        // Create progress spinner for file analysis
        let progress = if cli.quiet {
            None
        } else {
            let pb = ProgressBar::new_spinner();
            pb.set_style(
                ProgressStyle::default_spinner()
                    .template("{spinner:.cyan} {msg}")
                    .unwrap()
                    .tick_strings(&["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]),
            );
            pb.set_message("Analyzing NetCDF file...");
            Some(pb)
        };

        let output_format = format.as_ref().unwrap_or(&cli.output_format);

        let file_info = get_netcdf_info(file, variable.as_deref(), *detailed).await?;

        if let Some(pb) = progress {
            pb.finish_with_message("✅ File analysis completed");
        }

        match output_format {
            OutputFormat::Human => print_file_info_human(&file_info),
            OutputFormat::Json => print_file_info_json(&file_info)?,
            OutputFormat::Yaml => print_file_info_yaml(&file_info)?,
            OutputFormat::Csv => print_file_info_csv(&file_info)?,
        }
    } else {
        unreachable!("Info command handler called with wrong command type");
    }

    Ok(())
}

/// Handle the template subcommand
async fn handle_template_command(cli: &Cli) -> Result<()> {
    if let Commands::Template {
        template_type,
        output,
        format,
    } = &cli.command
    {
        let template = generate_template(template_type, format)?;

        match output {
            Some(path) => {
                std::fs::write(path, &template).context("Failed to write template file")?;
                info!("Template written to: {}", path.display());
            }
            None => {
                println!("{}", template);
            }
        }
    } else {
        unreachable!("Template command handler called with wrong command type");
    }

    Ok(())
}

/// Handle the completions subcommand
async fn handle_completions_command(cli: &Cli) -> Result<()> {
    if let Commands::Completions { shell, output } = &cli.command {
        info!("Generating shell completions for: {:?}", shell);

        let mut cmd = Cli::command();
        let name = cmd.get_name().to_string();

        let completions = match shell {
            Shell::Bash => {
                let mut buf = Vec::new();
                generate(Shell::Bash, &mut cmd, name, &mut buf);
                String::from_utf8(buf).context("Failed to generate bash completions")?
            }
            Shell::Zsh => {
                let mut buf = Vec::new();
                generate(Shell::Zsh, &mut cmd, name, &mut buf);
                String::from_utf8(buf).context("Failed to generate zsh completions")?
            }
            Shell::Fish => {
                let mut buf = Vec::new();
                generate(Shell::Fish, &mut cmd, name, &mut buf);
                String::from_utf8(buf).context("Failed to generate fish completions")?
            }
            Shell::PowerShell => {
                let mut buf = Vec::new();
                generate(Shell::PowerShell, &mut cmd, name, &mut buf);
                String::from_utf8(buf).context("Failed to generate PowerShell completions")?
            }
            _ => {
                return Err(anyhow::anyhow!("Unsupported shell: {:?}", shell));
            }
        };

        match output {
            Some(path) => {
                std::fs::write(path, &completions).context("Failed to write completions file")?;
                info!("Completions written to: {}", path.display());
            }
            None => {
                print!("{}", completions);
            }
        }
    } else {
        unreachable!("Completions command handler called with wrong command type");
    }

    Ok(())
}

/// Load configuration from various sources
fn load_configuration(
    cli: &Cli,
    input: &Option<String>,
    output: &Option<String>,
    variable: &Option<String>,
) -> Result<JobConfig> {
    // Priority system: CLI args > Environment variables > Config file

    // Start with potential environment variable fallbacks
    let env_input = std::env::var("NC2PARQUET_INPUT").ok();
    let env_output = std::env::var("NC2PARQUET_OUTPUT").ok();
    let env_variable = std::env::var("NC2PARQUET_VARIABLE").ok();

    // Try to load from config file first (lowest priority)
    if let Some(config_path) = &cli.config {
        debug!("Loading configuration from file: {}", config_path.display());
        let mut config = load_config_file(config_path)?;

        // Override with environment variables (medium priority)
        if let Some(env_input_path) = &env_input
            && input.is_none()
        {
            // Only use env if CLI argument not provided
            config.nc_key = env_input_path.clone();
            debug!("Using input from environment: {}", env_input_path);
        }
        if let Some(env_output_path) = &env_output
            && output.is_none()
        {
            // Only use env if CLI argument not provided
            config.parquet_key = env_output_path.clone();
            debug!("Using output from environment: {}", env_output_path);
        }
        if let Some(env_var_name) = &env_variable
            && variable.is_none()
        {
            // Only use env if CLI argument not provided
            config.variable_name = env_var_name.clone();
            debug!("Using variable from environment: {}", env_var_name);
        }

        // Override with command line arguments (highest priority)
        if let Some(input_path) = input {
            config.nc_key = input_path.clone();
        }
        if let Some(output_path) = output {
            config.parquet_key = output_path.clone();
        }
        if let Some(var_name) = variable {
            config.variable_name = var_name.clone();
        }

        return Ok(config);
    }

    // No config file - build from CLI args and environment variables
    // Priority: CLI args > Environment vars
    let input_path = input.as_ref()
        .or(env_input.as_ref())
        .context("Input file path is required (use --config file, provide INPUT argument, or set NC2PARQUET_INPUT environment variable)")?;

    let output_path = output.as_ref()
        .or(env_output.as_ref())
        .context("Output file path is required (use --config file, provide OUTPUT argument, or set NC2PARQUET_OUTPUT environment variable)")?;

    let var_name = variable.as_ref()
        .or(env_variable.as_ref())
        .context("Variable name is required (use --config file, --variable option, or set NC2PARQUET_VARIABLE environment variable)")?;

    debug!(
        "Created configuration from CLI/environment - input: {}, output: {}, variable: {}",
        input_path, output_path, var_name
    );

    Ok(JobConfig {
        nc_key: input_path.clone(),
        variable_name: var_name.clone(),
        parquet_key: output_path.clone(),
        filters: Vec::new(),
        postprocessing: None,
    })
}

/// Load configuration file (JSON or YAML)
fn load_config_file(path: &Path) -> Result<JobConfig> {
    let content = std::fs::read_to_string(path)
        .with_context(|| format!("Failed to read configuration file: {}", path.display()))?;

    // Try to determine format by extension, fallback to JSON
    let config = if path.extension().and_then(|s| s.to_str()) == Some("yaml")
        || path.extension().and_then(|s| s.to_str()) == Some("yml")
    {
        serde_yaml::from_str(&content).context("Failed to parse YAML configuration")?
    } else {
        serde_json::from_str(&content).context("Failed to parse JSON configuration")?
    };

    debug!("Configuration loaded successfully from {}", path.display());
    Ok(config)
}

/// Validate configuration
async fn validate_config(config: &JobConfig) -> Result<()> {
    let mut errors = Vec::new();
    let mut warnings = Vec::new();

    // Basic validation
    if config.nc_key.is_empty() {
        errors.push("Input NetCDF path cannot be empty".to_string());
    } else {
        // Check if input path is valid
        if !config.nc_key.starts_with("s3://") {
            let path = std::path::Path::new(&config.nc_key);
            if !path.exists() {
                warnings.push(format!("Input file does not exist: {}", config.nc_key));
            } else if !path.is_file() {
                errors.push(format!("Input path is not a file: {}", config.nc_key));
            }
        }

        // Check file extension
        if !config.nc_key.ends_with(".nc") && !config.nc_key.ends_with(".nc4") {
            warnings.push(format!(
                "Input file does not have a typical NetCDF extension (.nc or .nc4): {}",
                config.nc_key
            ));
        }
    }

    if config.parquet_key.is_empty() {
        errors.push("Output Parquet path cannot be empty".to_string());
    } else {
        // Check output directory exists (for local files)
        if !config.parquet_key.starts_with("s3://") {
            let output_path = std::path::Path::new(&config.parquet_key);
            if let Some(parent) = output_path.parent()
                && !parent.exists()
            {
                warnings.push(format!(
                    "Output directory does not exist: {}",
                    parent.display()
                ));
            }
        }

        // Check file extension
        if !config.parquet_key.ends_with(".parquet") && !config.parquet_key.ends_with(".pq") {
            warnings.push(format!(
                "Output file does not have a typical Parquet extension (.parquet or .pq): {}",
                config.parquet_key
            ));
        }
    }

    if config.variable_name.is_empty() {
        errors.push("Variable name cannot be empty".to_string());
    } else if config.variable_name.contains(" ") || config.variable_name.contains("\t") {
        errors.push(format!(
            "Variable name contains whitespace: '{}'",
            config.variable_name
        ));
    }

    // Validate filters
    for (i, filter) in config.filters.iter().enumerate() {
        match filter.to_filter() {
            Ok(_) => {
                // Additional filter-specific validation
                match filter {
                    nc2parquet::input::FilterConfig::Range { params } => {
                        if params.min_value >= params.max_value {
                            errors.push(format!(
                                "Filter {}: Range min_value ({}) must be less than max_value ({})",
                                i + 1,
                                params.min_value,
                                params.max_value
                            ));
                        }
                        if params.dimension_name.is_empty() {
                            errors.push(format!(
                                "Filter {}: Range dimension_name cannot be empty",
                                i + 1
                            ));
                        }
                    }
                    nc2parquet::input::FilterConfig::List { params } => {
                        if params.values.is_empty() {
                            warnings.push(format!(
                                "Filter {}: List filter has no values (will match nothing)",
                                i + 1
                            ));
                        }
                        if params.dimension_name.is_empty() {
                            errors.push(format!(
                                "Filter {}: List dimension_name cannot be empty",
                                i + 1
                            ));
                        }
                    }
                    nc2parquet::input::FilterConfig::Point2D { params } => {
                        if params.points.is_empty() {
                            warnings.push(format!(
                                "Filter {}: 2D point filter has no points (will match nothing)",
                                i + 1
                            ));
                        }
                        if params.tolerance < 0.0 {
                            errors.push(format!(
                                "Filter {}: 2D point tolerance cannot be negative: {}",
                                i + 1,
                                params.tolerance
                            ));
                        }
                        if params.lat_dimension_name.is_empty()
                            || params.lon_dimension_name.is_empty()
                        {
                            errors.push(format!("Filter {}: 2D point latitude and longitude dimension names cannot be empty", i + 1));
                        }
                    }
                    nc2parquet::input::FilterConfig::Point3D { params } => {
                        if params.points.is_empty() || params.steps.is_empty() {
                            warnings.push(format!("Filter {}: 3D point filter has no points or steps (will match nothing)", i + 1));
                        }
                        if params.tolerance < 0.0 {
                            errors.push(format!(
                                "Filter {}: 3D point tolerance cannot be negative: {}",
                                i + 1,
                                params.tolerance
                            ));
                        }
                        if params.time_dimension_name.is_empty()
                            || params.lat_dimension_name.is_empty()
                            || params.lon_dimension_name.is_empty()
                        {
                            errors.push(format!("Filter {}: 3D point time, latitude, and longitude dimension names cannot be empty", i + 1));
                        }
                    }
                }
            }
            Err(e) => {
                errors.push(format!("Invalid filter at index {}: {}", i + 1, e));
            }
        }
    }

    // Environment variable validation (if any are set)
    if std::env::var("NC2PARQUET_CONFIG").is_ok()
        && std::env::var("NC2PARQUET_CONFIG").unwrap().is_empty()
    {
        warnings.push("NC2PARQUET_CONFIG environment variable is set but empty".to_string());
    }

    // Output warnings
    for warning in &warnings {
        warn!("Configuration warning: {}", warning);
    }

    // Check for errors
    if !errors.is_empty() {
        let error_msg = format!(
            "Configuration validation failed with {} error(s):\n{}",
            errors.len(),
            errors
                .iter()
                .enumerate()
                .map(|(i, e)| format!("  {}. {}", i + 1, e))
                .collect::<Vec<_>>()
                .join("\n")
        );
        return Err(anyhow::anyhow!(error_msg));
    }

    if warnings.is_empty() {
        info!("Configuration validation passed");
    } else {
        info!(
            "Configuration validation passed with {} warning(s)",
            warnings.len()
        );
    }

    debug!("Configuration validation completed successfully");
    Ok(())
}

/// Check if output file exists and handle overwrite logic
async fn check_output_overwrite(output_path: &str) -> Result<()> {
    let storage = StorageFactory::from_path(output_path).await?;

    if storage.exists(output_path).await? {
        return Err(anyhow::anyhow!(
            "Output file already exists: {}. Use --force to overwrite",
            output_path
        ));
    }

    Ok(())
}

/// Check if async processing is needed (for S3 paths)
fn needs_async_processing(config: &JobConfig) -> bool {
    config.nc_key.starts_with("s3://") || config.parquet_key.starts_with("s3://")
}

/// Print configuration summary
fn print_config_summary(config: &JobConfig, format: &OutputFormat) {
    match format {
        OutputFormat::Human => {
            println!("\nConfiguration Summary:");
            println!("  Input:    {}", config.nc_key);
            println!("  Variable: {}", config.variable_name);
            println!("  Output:   {}", config.parquet_key);
            println!("  Filters:  {}", config.filters.len());

            for (i, filter) in config.filters.iter().enumerate() {
                println!("    {}: {}", i + 1, filter.kind());
            }
        }
        OutputFormat::Json => {
            if let Ok(json) = serde_json::to_string_pretty(config) {
                println!("{}", json);
            }
        }
        _ => {
            // For other formats, fall back to human readable
            print_config_summary(config, &OutputFormat::Human);
        }
    }
}

/// Show output file information
async fn show_output_info(output_path: &str, format: &OutputFormat) -> Result<()> {
    let storage = StorageFactory::from_path(output_path).await?;

    if !storage.exists(output_path).await? {
        warn!("Output file was not created: {}", output_path);
        return Ok(());
    }

    match format {
        OutputFormat::Human => {
            info!("Output file created successfully: {}", output_path);
        }
        OutputFormat::Json => {
            let info = serde_json::json!({
                "output_file": output_path,
                "status": "created"
            });
            println!("{}", serde_json::to_string_pretty(&info)?);
        }
        _ => {
            info!("Output: {}", output_path);
        }
    }

    Ok(())
}

use nc2parquet::info::{
    get_netcdf_info, print_file_info_csv, print_file_info_human, print_file_info_json,
    print_file_info_yaml,
};

async fn show_detailed_validation(config: &JobConfig, format: &OutputFormat) -> Result<()> {
    println!("\n=== Detailed Validation Report ===");

    // Configuration summary
    println!("\n1. Configuration Summary:");
    println!("   Input:        {}", config.nc_key);
    println!("   Variable:     {}", config.variable_name);
    println!("   Output:       {}", config.parquet_key);
    println!("   Format:       {:?}", format);

    // Storage information
    println!("\n2. Storage Information:");
    let input_storage = if config.nc_key.starts_with("s3://") {
        "S3"
    } else {
        "Local"
    };
    let output_storage = if config.parquet_key.starts_with("s3://") {
        "S3"
    } else {
        "Local"
    };
    println!("   Input Storage:  {}", input_storage);
    println!("   Output Storage: {}", output_storage);

    // Filter information
    if !config.filters.is_empty() {
        println!("\n3. Filters Applied:");
        println!("   Total Filters: {}", config.filters.len());

        for (i, filter) in config.filters.iter().enumerate() {
            match filter {
                FilterConfig::Range { params } => {
                    println!(
                        "     {}. Range Filter: {} ({} to {})",
                        i + 1,
                        params.dimension_name,
                        params.min_value,
                        params.max_value
                    );
                }
                FilterConfig::List { params } => {
                    println!(
                        "     {}. List Filter: {} {:?}",
                        i + 1,
                        params.dimension_name,
                        params.values
                    );
                }
                FilterConfig::Point2D { params } => {
                    println!(
                        "     {}. Point2D Filter: {},{} {} points ±{}",
                        i + 1,
                        params.lat_dimension_name,
                        params.lon_dimension_name,
                        params.points.len(),
                        params.tolerance
                    );
                    for (j, (lat, lon)) in params.points.iter().enumerate() {
                        if j < 3 {
                            // Show up to 3 points
                            println!("         Point {}: ({}, {})", j + 1, lat, lon);
                        } else if j == 3 {
                            println!("         ... and {} more", params.points.len() - 3);
                            break;
                        }
                    }
                }
                FilterConfig::Point3D { params } => {
                    println!(
                        "     {}. Point3D Filter: {},{},{} {} points, {} steps ±{}",
                        i + 1,
                        params.time_dimension_name,
                        params.lat_dimension_name,
                        params.lon_dimension_name,
                        params.points.len(),
                        params.steps.len(),
                        params.tolerance
                    );
                    for (j, (lat, lon)) in params.points.iter().enumerate() {
                        if j < 2 {
                            // Show up to 2 points for 3D
                            println!("         Point {}: ({}, {})", j + 1, lat, lon);
                        } else if j == 2 {
                            println!("         ... and {} more", params.points.len() - 2);
                            break;
                        }
                    }
                }
            }
        }
    } else {
        println!("\n3. Filters Applied: None");
    }

    // Post-processing information
    if let Some(postprocessing) = &config.postprocessing {
        println!("\n4. Post-Processing:");
        println!(
            "   Pipeline: {} processors defined",
            postprocessing.processors.len()
        );
        for (i, processor) in postprocessing.processors.iter().enumerate() {
            let processor_type = match processor {
                ProcessorConfig::RenameColumns { .. } => "Rename Columns",
                ProcessorConfig::DatetimeConvert { .. } => "Datetime Convert",
                ProcessorConfig::UnitConvert { .. } => "Unit Convert",
                ProcessorConfig::Aggregate { .. } => "Aggregate",
                ProcessorConfig::ApplyFormula { .. } => "Apply Formula",
            };
            println!("     {}. {}", i + 1, processor_type);
        }
    } else {
        println!("\n4. Post-Processing: None");
    }

    println!("\n✓ All validation checks passed");
    Ok(())
}

/// Generate configuration template
fn generate_template(template_type: &TemplateType, format: &ConfigFormat) -> Result<String> {
    let config = match template_type {
        TemplateType::Basic => JobConfig {
            nc_key: "input.nc".to_string(),
            variable_name: "temperature".to_string(),
            parquet_key: "output.parquet".to_string(),
            filters: vec![],
            postprocessing: None,
        },
        TemplateType::S3 => JobConfig {
            nc_key: "s3://my-bucket/input.nc".to_string(),
            variable_name: "temperature".to_string(),
            parquet_key: "s3://my-bucket/output.parquet".to_string(),
            filters: vec![],
            postprocessing: None,
        },
        TemplateType::MultiFilter => JobConfig {
            nc_key: "weather_data.nc".to_string(),
            variable_name: "temperature".to_string(),
            parquet_key: "filtered_weather.parquet".to_string(),
            filters: vec![
                nc2parquet::input::FilterConfig::Range {
                    params: nc2parquet::input::RangeParams {
                        dimension_name: "latitude".to_string(),
                        min_value: 30.0,
                        max_value: 60.0,
                    },
                },
                nc2parquet::input::FilterConfig::List {
                    params: nc2parquet::input::ListParams {
                        dimension_name: "pressure".to_string(),
                        values: vec![1000.0, 850.0, 500.0],
                    },
                },
            ],
            postprocessing: None,
        },
        TemplateType::Weather => JobConfig {
            nc_key: "weather_station_data.nc".to_string(),
            variable_name: "temperature".to_string(),
            parquet_key: "weather_analysis.parquet".to_string(),
            filters: vec![nc2parquet::input::FilterConfig::Range {
                params: nc2parquet::input::RangeParams {
                    dimension_name: "time".to_string(),
                    min_value: 20230101.0,
                    max_value: 20231231.0,
                },
            }],
            postprocessing: None,
        },
        TemplateType::Ocean => JobConfig {
            nc_key: "ocean_temperature.nc".to_string(),
            variable_name: "sea_surface_temperature".to_string(),
            parquet_key: "sst_analysis.parquet".to_string(),
            filters: vec![nc2parquet::input::FilterConfig::Range {
                params: nc2parquet::input::RangeParams {
                    dimension_name: "depth".to_string(),
                    min_value: 0.0,
                    max_value: 10.0,
                },
            }],
            postprocessing: None,
        },
    };

    match format {
        ConfigFormat::Json => {
            serde_json::to_string_pretty(&config).context("Failed to serialize template to JSON")
        }
        ConfigFormat::Yaml => {
            serde_yaml::to_string(&config).context("Failed to serialize template to YAML")
        }
    }
}

/// Get file size for performance metrics
async fn get_file_size(file_path: &str) -> Result<u64> {
    if file_path.starts_with("s3://") {
        // For S3 files, we'd need to get the content-length from S3
        // For now, just return 0 for S3 files
        Ok(0)
    } else {
        let metadata = tokio::fs::metadata(file_path)
            .await
            .context("Failed to get file metadata")?;
        Ok(metadata.len())
    }
}
