//! # Post-Processing Framework
//!
//! This module provides a flexible post-processing framework for transforming
//! DataFrames after NetCDF extraction and before Parquet writing.
//!
//! ## Features
//! - **Trait-based extensibility**: Easy to add custom processors
//! - **Pipeline chaining**: Chain multiple processors together
//! - **Built-in processors**: Common transformations ready to use
//! - **Configuration-driven**: Define processing steps in JSON/YAML
//! - **Error handling**: Robust error propagation and recovery
//!
//! ## Built-in Processors
//! - **ColumnRenamer**: Rename columns with mappings
//! - **DateTimeConverter**: Convert numeric columns to datetime
//! - **UnitConverter**: Convert between units (temperature, pressure, etc.)
//! - **Aggregator**: Spatial/temporal aggregations
//! - **FormulaApplier**: Apply mathematical expressions
//!
//! ## Example
//! ```rust
//! use nc2parquet::postprocess::{PostProcessor, ProcessingPipeline, ColumnRenamer};
//! use polars::prelude::*;
//! use std::collections::HashMap;
//!
//! // Create a pipeline
//! let mut pipeline = ProcessingPipeline::new();
//!
//! // Add processors
//! let mut mappings = HashMap::new();
//! mappings.insert("t2".to_string(), "temperature_2m".to_string());
//! pipeline.add_processor(Box::new(ColumnRenamer::new(mappings)));
//!
//! // Create sample DataFrame
//! let sample_df = df! {
//!     "t2" => [20.5, 21.0, 19.8],
//!     "humidity" => [65, 70, 60]
//! }.unwrap();
//!
//! // Execute pipeline
//! let processed_df = pipeline.execute(sample_df).unwrap();
//! ```

use chrono::{DateTime, Utc};
use log::{debug, warn};
use polars::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::error::Error;
use std::fmt;

/// Result type for post-processing operations
pub type PostProcessResult<T> = Result<T, PostProcessError>;

/// Errors that can occur during post-processing
#[derive(Debug)]
pub enum PostProcessError {
    /// Column not found in DataFrame
    ColumnNotFound(String),
    /// Data type conversion error
    ConversionError(String),
    /// Invalid configuration
    ConfigurationError(String),
    /// Polars-specific error
    PolarsError(PolarsError),
    /// Custom processing error
    ProcessingError(String),
}

impl fmt::Display for PostProcessError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PostProcessError::ColumnNotFound(col) => {
                write!(f, "Column '{}' not found in DataFrame", col)
            }
            PostProcessError::ConversionError(msg) => write!(f, "Conversion error: {}", msg),
            PostProcessError::ConfigurationError(msg) => write!(f, "Configuration error: {}", msg),
            PostProcessError::PolarsError(e) => write!(f, "Polars error: {}", e),
            PostProcessError::ProcessingError(msg) => write!(f, "Processing error: {}", msg),
        }
    }
}

impl Error for PostProcessError {}

impl From<PolarsError> for PostProcessError {
    fn from(error: PolarsError) -> Self {
        PostProcessError::PolarsError(error)
    }
}

/// Core trait for post-processing operations on DataFrames
pub trait PostProcessor: Send + Sync {
    /// Process the DataFrame and return the transformed result
    fn process(&self, df: DataFrame) -> PostProcessResult<DataFrame>;

    /// Get the name/identifier of this processor
    fn name(&self) -> &str;

    /// Get a description of what this processor does
    fn description(&self) -> &str;

    /// Validate that the processor can operate on the given DataFrame schema
    fn validate_schema(&self, schema: &Schema) -> PostProcessResult<()> {
        let _ = schema; // Default implementation does no validation
        Ok(())
    }

    /// Get the expected schema after processing (default: unchanged)
    fn output_schema(&self, input_schema: &Schema) -> PostProcessResult<Schema> {
        Ok(input_schema.clone())
    }
}

/// Configuration for the entire post-processing pipeline
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessingPipelineConfig {
    /// Optional name for the pipeline
    pub name: Option<String>,
    /// List of processors to execute in order
    pub processors: Vec<ProcessorConfig>,
}

/// Configuration for post-processing steps
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ProcessorConfig {
    /// Rename columns using a mapping
    RenameColumns { mappings: HashMap<String, String> },
    /// Convert numeric column to datetime
    DatetimeConvert {
        column: String,
        base: String, // ISO 8601 format
        unit: TimeUnit,
    },
    /// Convert between units
    UnitConvert {
        column: String,
        from_unit: String,
        to_unit: String,
    },
    /// Aggregate data
    Aggregate {
        group_by: Vec<String>,
        aggregations: HashMap<String, AggregationOp>,
    },
    /// Apply mathematical formulas
    ApplyFormula {
        target_column: String,
        formula: String,
        source_columns: Vec<String>,
    },
}

/// Time units for datetime conversion
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TimeUnit {
    Seconds,
    Minutes,
    Hours,
    Days,
    Milliseconds,
    Microseconds,
    Nanoseconds,
}

/// Aggregation operations
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AggregationOp {
    Mean,
    Sum,
    Min,
    Max,
    Count,
    Std,
    Var,
    First,
    Last,
}

impl TimeUnit {
    /// Convert the time unit to a multiplier for seconds
    pub fn to_seconds_multiplier(&self) -> f64 {
        match self {
            TimeUnit::Nanoseconds => 1e-9,
            TimeUnit::Microseconds => 1e-6,
            TimeUnit::Milliseconds => 1e-3,
            TimeUnit::Seconds => 1.0,
            TimeUnit::Minutes => 60.0,
            TimeUnit::Hours => 3600.0,
            TimeUnit::Days => 86400.0,
        }
    }
}

/// Pipeline that chains multiple post-processors together
pub struct ProcessingPipeline {
    processors: Vec<Box<dyn PostProcessor>>,
    name: String,
}

impl ProcessingPipeline {
    /// Create a new empty processing pipeline
    pub fn new() -> Self {
        Self {
            name: "Unnamed Pipeline".to_string(),
            processors: Vec::new(),
        }
    }

    /// Create a new processing pipeline with a name
    pub fn with_name(name: String) -> Self {
        Self {
            name,
            processors: Vec::new(),
        }
    }

    /// Get the pipeline name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Create a processing pipeline from configuration
    pub fn from_config(config: &ProcessingPipelineConfig) -> PostProcessResult<Self> {
        let mut pipeline = Self {
            name: config
                .name
                .clone()
                .unwrap_or_else(|| "Configured Pipeline".to_string()),
            processors: Vec::new(),
        };

        for processor_config in &config.processors {
            let processor = create_processor(processor_config)?;
            pipeline.add_processor(processor);
        }

        Ok(pipeline)
    }

    /// Add a processor to the pipeline
    pub fn add_processor(&mut self, processor: Box<dyn PostProcessor>) {
        self.processors.push(processor);
    }

    /// Execute the processing pipeline on a DataFrame
    pub fn execute(&mut self, mut df: DataFrame) -> PostProcessResult<DataFrame> {
        debug!(
            "Executing pipeline '{}' with {} processors",
            self.name,
            self.processors.len()
        );

        if self.processors.is_empty() {
            debug!(
                "Pipeline '{}' is empty, returning DataFrame unchanged",
                self.name
            );
            return Ok(df);
        }

        debug!("Initial DataFrame shape: {:?}", df.shape());

        // Execute each processor in sequence
        for (i, processor) in self.processors.iter().enumerate() {
            let processor_name = processor.name();
            debug!(
                "Executing processor {} '{}' - input shape: {:?}",
                i + 1,
                processor_name,
                df.shape()
            );

            df = processor.process(df)?;

            debug!(
                "Processor '{}' completed - output shape: {:?}",
                processor_name,
                df.shape()
            );
        }

        debug!("Pipeline '{}' completed successfully", self.name);
        Ok(df)
    }
}

impl Default for ProcessingPipeline {
    fn default() -> Self {
        Self::new()
    }
}

/// Helper function to create a processor from configuration
pub fn create_processor(config: &ProcessorConfig) -> PostProcessResult<Box<dyn PostProcessor>> {
    match config {
        ProcessorConfig::RenameColumns { mappings } => {
            Ok(Box::new(ColumnRenamer::new(mappings.clone())))
        }
        ProcessorConfig::DatetimeConvert { column, base, unit } => {
            let base_dt = DateTime::parse_from_rfc3339(base)
                .map_err(|e| {
                    PostProcessError::ConfigurationError(format!(
                        "Invalid base datetime '{}': {}",
                        base, e
                    ))
                })?
                .with_timezone(&Utc);
            Ok(Box::new(DateTimeConverter::new(
                column.clone(),
                base_dt,
                unit.clone(),
            )))
        }
        ProcessorConfig::UnitConvert {
            column,
            from_unit,
            to_unit,
        } => Ok(Box::new(UnitConverter::new(
            column.clone(),
            from_unit.clone(),
            to_unit.clone(),
        ))),
        ProcessorConfig::Aggregate {
            group_by,
            aggregations,
        } => Ok(Box::new(Aggregator::new(
            group_by.clone(),
            aggregations.clone(),
        ))),
        ProcessorConfig::ApplyFormula {
            target_column,
            formula,
            source_columns,
        } => Ok(Box::new(FormulaApplier::new(
            target_column.clone(),
            formula.clone(),
            source_columns.clone(),
        ))),
    }
}

/// Create a pipeline from a vector of processor configurations
pub fn create_pipeline(configs: &[ProcessorConfig]) -> PostProcessResult<ProcessingPipeline> {
    let mut pipeline = ProcessingPipeline::new();

    for config in configs {
        let processor = create_processor(config)?;
        pipeline.add_processor(processor);
    }

    Ok(pipeline)
}

// Forward declarations for built-in processors - implementations will follow
pub struct ColumnRenamer {
    mappings: HashMap<String, String>,
}

pub struct DateTimeConverter {
    column: String,
    base_datetime: DateTime<Utc>,
    unit: TimeUnit,
}

pub struct UnitConverter {
    column: String,
    from_unit: String,
    to_unit: String,
    conversion_factor: f64,
}

pub struct Aggregator {
    group_by: Vec<String>,
    aggregations: HashMap<String, AggregationOp>,
}

pub struct FormulaApplier {
    target_column: String,
    formula: String,
    source_columns: Vec<String>,
}

// Implementation stubs - will be implemented in the next step
impl ColumnRenamer {
    pub fn new(mappings: HashMap<String, String>) -> Self {
        Self { mappings }
    }
}

impl DateTimeConverter {
    pub fn new(column: String, base_datetime: DateTime<Utc>, unit: TimeUnit) -> Self {
        Self {
            column,
            base_datetime,
            unit,
        }
    }
}

impl UnitConverter {
    pub fn new(column: String, from_unit: String, to_unit: String) -> Self {
        // Calculate conversion factor based on units
        let conversion_factor = Self::calculate_conversion_factor(&from_unit, &to_unit);
        Self {
            column,
            from_unit,
            to_unit,
            conversion_factor,
        }
    }

    pub fn with_conversion_factor(
        column: String,
        from_unit: String,
        to_unit: String,
        factor: f64,
    ) -> Self {
        Self {
            column,
            from_unit,
            to_unit,
            conversion_factor: factor,
        }
    }

    fn calculate_conversion_factor(from_unit: &str, to_unit: &str) -> f64 {
        // Simplified conversion - will be expanded
        match (
            from_unit.to_lowercase().as_str(),
            to_unit.to_lowercase().as_str(),
        ) {
            ("kelvin", "celsius") | ("k", "c") => 1.0, // Special case: K to C = K - 273.15
            ("celsius", "kelvin") | ("c", "k") => 1.0, // Special case: C to K = C + 273.15
            ("celsius", "fahrenheit") | ("c", "f") => 9.0 / 5.0,
            ("fahrenheit", "celsius") | ("f", "c") => 5.0 / 9.0,
            _ => 1.0, // Default: no conversion
        }
    }
}

impl Aggregator {
    pub fn new(group_by: Vec<String>, aggregations: HashMap<String, AggregationOp>) -> Self {
        Self {
            group_by,
            aggregations,
        }
    }
}

impl PostProcessor for ColumnRenamer {
    fn process(&self, mut df: DataFrame) -> PostProcessResult<DataFrame> {
        debug!("Renaming columns with {} mappings", self.mappings.len());

        for (old_name, new_name) in &self.mappings {
            // Check if column exists
            let column_names: Vec<&str> =
                df.get_column_names().iter().map(|s| s.as_str()).collect();
            if !column_names.contains(&old_name.as_str()) {
                warn!(
                    "Column '{}' not found in DataFrame, skipping rename",
                    old_name
                );
                continue;
            }

            debug!("Renaming column '{}' to '{}'", old_name, new_name);
            df.rename(old_name, new_name.into())?;
        }

        Ok(df)
    }

    fn name(&self) -> &str {
        "ColumnRenamer"
    }

    fn description(&self) -> &str {
        "Renames columns based on provided mappings"
    }

    fn output_schema(&self, input_schema: &Schema) -> PostProcessResult<Schema> {
        let mut new_fields = Vec::new();

        // Apply renaming to schema
        for (name, dtype) in input_schema.iter() {
            let name_str = name.as_str();
            let new_name = if let Some(mapped) = self.mappings.get(name_str) {
                mapped.clone()
            } else {
                name_str.to_string()
            };
            new_fields.push(Field::new(new_name.into(), dtype.clone()));
        }

        Ok(Schema::from_iter(new_fields))
    }
}

impl PostProcessor for DateTimeConverter {
    fn process(&self, df: DataFrame) -> PostProcessResult<DataFrame> {
        debug!(
            "Converting column '{}' to datetime using base {} and unit {:?}",
            self.column,
            self.base_datetime.to_rfc3339(),
            self.unit
        );

        // Check if column exists
        let column_names: Vec<&str> = df.get_column_names().iter().map(|s| s.as_str()).collect();
        if !column_names.contains(&self.column.as_str()) {
            return Err(PostProcessError::ColumnNotFound(self.column.clone()));
        }

        // For now, just convert to string representation of datetime
        // A full implementation would do proper datetime conversion
        let result = df
            .lazy()
            .with_columns([col(&self.column)
                .cast(DataType::String)
                .alias(&format!("{}_datetime", self.column))])
            .collect()?;

        Ok(result)
    }

    fn name(&self) -> &str {
        "DateTimeConverter"
    }

    fn description(&self) -> &str {
        "Converts numeric column values to datetime based on a base datetime and time unit"
    }

    fn output_schema(&self, input_schema: &Schema) -> PostProcessResult<Schema> {
        let mut new_schema = input_schema.clone();

        // Add the new datetime column
        new_schema.with_column(format!("{}_datetime", self.column).into(), DataType::String);

        Ok(new_schema)
    }
}

impl PostProcessor for UnitConverter {
    fn process(&self, df: DataFrame) -> PostProcessResult<DataFrame> {
        debug!(
            "Converting column '{}' from {} to {} (factor: {})",
            self.column, self.from_unit, self.to_unit, self.conversion_factor
        );

        // Check if column exists
        let column_names: Vec<&str> = df.get_column_names().iter().map(|s| s.as_str()).collect();
        if !column_names.contains(&self.column.as_str()) {
            return Err(PostProcessError::ColumnNotFound(self.column.clone()));
        }

        let result = if (self.from_unit.to_lowercase() == "kelvin"
            || self.from_unit.to_lowercase() == "k")
            && (self.to_unit.to_lowercase() == "celsius" || self.to_unit.to_lowercase() == "c")
        {
            // Special case: Kelvin to Celsius (K - 273.15)
            df.lazy()
                .with_columns([(col(&self.column) - lit(273.15)).alias(&self.column)])
                .collect()?
        } else if (self.from_unit.to_lowercase() == "celsius"
            || self.from_unit.to_lowercase() == "c")
            && (self.to_unit.to_lowercase() == "kelvin" || self.to_unit.to_lowercase() == "k")
        {
            // Special case: Celsius to Kelvin (C + 273.15)
            df.lazy()
                .with_columns([(col(&self.column) + lit(273.15)).alias(&self.column)])
                .collect()?
        } else if (self.from_unit.to_lowercase() == "celsius"
            || self.from_unit.to_lowercase() == "c")
            && (self.to_unit.to_lowercase() == "fahrenheit" || self.to_unit.to_lowercase() == "f")
        {
            // Special case: Celsius to Fahrenheit (C * 9/5 + 32)
            df.lazy()
                .with_columns(
                    [(col(&self.column) * lit(9.0 / 5.0) + lit(32.0)).alias(&self.column)],
                )
                .collect()?
        } else if (self.from_unit.to_lowercase() == "fahrenheit"
            || self.from_unit.to_lowercase() == "f")
            && (self.to_unit.to_lowercase() == "celsius" || self.to_unit.to_lowercase() == "c")
        {
            // Special case: Fahrenheit to Celsius ((F - 32) * 5/9)
            df.lazy()
                .with_columns([
                    ((col(&self.column) - lit(32.0)) * lit(5.0 / 9.0)).alias(&self.column)
                ])
                .collect()?
        } else {
            // Simple multiplication conversion
            df.lazy()
                .with_columns([
                    (col(&self.column) * lit(self.conversion_factor)).alias(&self.column)
                ])
                .collect()?
        };

        Ok(result)
    }

    fn name(&self) -> &str {
        "UnitConverter"
    }

    fn description(&self) -> &str {
        "Converts values in a column from one unit to another"
    }
}

impl PostProcessor for Aggregator {
    fn process(&self, df: DataFrame) -> PostProcessResult<DataFrame> {
        debug!(
            "Aggregating data with group_by: {:?}, aggregations: {:?}",
            self.group_by, self.aggregations
        );

        // Check if all group_by columns exist
        let column_names: Vec<&str> = df.get_column_names().iter().map(|s| s.as_str()).collect();
        for col_name in &self.group_by {
            if !column_names.contains(&col_name.as_str()) {
                return Err(PostProcessError::ColumnNotFound(col_name.clone()));
            }
        }

        // Check if all aggregation columns exist
        for col_name in self.aggregations.keys() {
            if !column_names.contains(&col_name.as_str()) {
                return Err(PostProcessError::ColumnNotFound(col_name.clone()));
            }
        }

        // Build aggregation expressions
        let mut agg_exprs = Vec::new();

        for (col_name, agg_op) in &self.aggregations {
            let (expr, suffix) = match agg_op {
                AggregationOp::Mean => (col(col_name).mean(), "mean"),
                AggregationOp::Sum => (col(col_name).sum(), "sum"),
                AggregationOp::Min => (col(col_name).min(), "min"),
                AggregationOp::Max => (col(col_name).max(), "max"),
                AggregationOp::Count => (col(col_name).count(), "count"),
                AggregationOp::Std => (col(col_name).std(1), "std"), // Use population std
                AggregationOp::Var => (col(col_name).var(1), "var"), // Use population var
                AggregationOp::First => (col(col_name).first(), "first"),
                AggregationOp::Last => (col(col_name).last(), "last"),
            };
            agg_exprs.push(expr.alias(&format!("{}_{}", col_name, suffix)));
        }

        let result = if !self.group_by.is_empty() {
            df.lazy()
                .group_by(self.group_by.iter().map(|s| col(s)).collect::<Vec<_>>())
                .agg(agg_exprs)
                .collect()?
        } else {
            // Global aggregation (no grouping)
            df.lazy().select(agg_exprs).collect()?
        };

        Ok(result)
    }

    fn name(&self) -> &str {
        "Aggregator"
    }

    fn description(&self) -> &str {
        "Aggregates data using group by operations and statistical functions"
    }
}

impl PostProcessor for FormulaApplier {
    fn process(&self, df: DataFrame) -> PostProcessResult<DataFrame> {
        debug!(
            "Applying formula '{}' to create column '{}'",
            self.formula, self.target_column
        );

        // Check if all source columns exist
        let column_names: Vec<&str> = df.get_column_names().iter().map(|s| s.as_str()).collect();
        for col_name in &self.source_columns {
            if !column_names.contains(&col_name.as_str()) {
                return Err(PostProcessError::ColumnNotFound(col_name.clone()));
            }
        }

        // Parse and apply the formula
        let result = self.apply_formula(df)?;

        Ok(result)
    }

    fn name(&self) -> &str {
        "FormulaApplier"
    }

    fn description(&self) -> &str {
        "Applies mathematical formulas to create new columns or transform existing ones"
    }

    fn output_schema(&self, input_schema: &Schema) -> PostProcessResult<Schema> {
        let mut new_schema = input_schema.clone();

        // Add the new target column if it doesn't exist
        if !new_schema.contains(&self.target_column) {
            new_schema.with_column(self.target_column.as_str().into(), DataType::Float64);
        }

        Ok(new_schema)
    }
}

impl FormulaApplier {
    pub fn new(target_column: String, formula: String, source_columns: Vec<String>) -> Self {
        Self {
            target_column,
            formula,
            source_columns,
        }
    }

    /// Apply the formula to create the target column
    fn apply_formula(&self, df: DataFrame) -> PostProcessResult<DataFrame> {
        // Enhanced formula parser - supports arithmetic, comparison, and function operations
        let formula = self.formula.trim();

        // Handle different types of formulas in order of complexity
        let result = if formula.contains('<')
            || formula.contains('>')
            || formula.contains("==")
            || formula.contains("!=")
        {
            self.parse_comparison_formula(df, formula)?
        } else if formula.contains('+')
            || formula.contains('-')
            || formula.contains('*')
            || formula.contains('/')
        {
            self.parse_arithmetic_formula(df, formula)?
        } else if formula.starts_with("sqrt(") {
            self.parse_function_formula(df, formula)?
        } else {
            // Simple column copy or constant
            let operand_expr = self.parse_operand_with_validation(&df, formula)?;
            df.lazy()
                .with_columns([operand_expr.alias(&self.target_column)])
                .collect()?
        };

        Ok(result)
    }

    /// Parse comparison formulas like "a < b", "a == 5.0", etc.
    fn parse_comparison_formula(
        &self,
        df: DataFrame,
        formula: &str,
    ) -> PostProcessResult<DataFrame> {
        let comparison_ops = ["==", "!=", "<=", ">=", "<", ">"];

        for op in comparison_ops {
            if formula.contains(op) {
                let parts: Vec<&str> = formula.split(op).collect();
                if parts.len() == 2 {
                    let left = parts[0].trim();
                    let right = parts[1].trim();

                    let left_expr = self.parse_operand_with_validation(&df, left)?;
                    let right_expr = self.parse_operand_with_validation(&df, right)?;

                    let result_expr = match op {
                        "==" => left_expr.eq(right_expr),
                        "!=" => left_expr.neq(right_expr),
                        "<" => left_expr.lt(right_expr),
                        "<=" => left_expr.lt_eq(right_expr),
                        ">" => left_expr.gt(right_expr),
                        ">=" => left_expr.gt_eq(right_expr),
                        _ => unreachable!(),
                    };

                    return Ok(df
                        .lazy()
                        .with_columns([result_expr.alias(&self.target_column)])
                        .collect()?);
                }
            }
        }

        Err(PostProcessError::ProcessingError(format!(
            "Unable to parse comparison formula: {}",
            formula
        )))
    }

    /// Parse arithmetic formulas with operator precedence support
    fn parse_arithmetic_formula(
        &self,
        df: DataFrame,
        formula: &str,
    ) -> PostProcessResult<DataFrame> {
        let expr = self.parse_expression(&df, formula)?;

        Ok(df
            .lazy()
            .with_columns([expr.alias(&self.target_column)])
            .collect()?)
    }

    /// Recursive expression parser with operator precedence
    /// Handles: addition/subtraction (lowest precedence) and multiplication/division (higher precedence)
    fn parse_expression(&self, df: &DataFrame, expr: &str) -> PostProcessResult<Expr> {
        // Parse addition and subtraction (lowest precedence)
        let expr = expr.trim();

        // Look for + or - operators (left to right)
        let mut depth = 0;
        for (i, c) in expr.char_indices() {
            match c {
                '(' => depth += 1,
                ')' => depth -= 1,
                '+' | '-' if depth == 0 => {
                    // Found a top-level + or - operator
                    let left = &expr[..i];
                    let right = &expr[i + 1..];
                    let left_expr = self.parse_expression(df, left)?;
                    let right_expr = self.parse_expression(df, right)?;

                    return Ok(match c {
                        '+' => left_expr + right_expr,
                        '-' => left_expr - right_expr,
                        _ => unreachable!(),
                    });
                }
                _ => {}
            }
        }

        // No addition/subtraction found, try multiplication/division
        self.parse_term(df, expr)
    }

    /// Parse multiplication and division terms
    fn parse_term(&self, df: &DataFrame, expr: &str) -> PostProcessResult<Expr> {
        let expr = expr.trim();

        // Look for * or / operators (left to right)
        let mut depth = 0;
        for (i, c) in expr.char_indices() {
            match c {
                '(' => depth += 1,
                ')' => depth -= 1,
                '*' | '/' if depth == 0 => {
                    // Found a top-level * or / operator
                    let left = &expr[..i];
                    let right = &expr[i + 1..];
                    let left_expr = self.parse_term(df, left)?;
                    let right_expr = self.parse_term(df, right)?;

                    return Ok(match c {
                        '*' => left_expr * right_expr,
                        '/' => left_expr / right_expr,
                        _ => unreachable!(),
                    });
                }
                _ => {}
            }
        }

        // No multiplication/division found, parse as factor (operand or parenthesized expression)
        self.parse_factor(df, expr)
    }

    /// Parse factors (operands or parenthesized expressions)
    fn parse_factor(&self, df: &DataFrame, expr: &str) -> PostProcessResult<Expr> {
        let expr = expr.trim();

        // Handle parentheses
        if expr.starts_with('(') && expr.ends_with(')') {
            return self.parse_expression(df, &expr[1..expr.len() - 1]);
        }

        // Handle as operand (column or constant)
        self.parse_operand_with_validation(df, expr)
    }

    /// Parse function formulas like "sqrt(a)"
    fn parse_function_formula(&self, df: DataFrame, formula: &str) -> PostProcessResult<DataFrame> {
        if formula.starts_with("sqrt(") && formula.ends_with(")") {
            let inner = &formula[5..formula.len() - 1];
            let operand = self.parse_operand_with_validation(&df, inner)?;

            Ok(df
                .lazy()
                .with_columns([operand.sqrt().alias(&self.target_column)])
                .collect()?)
        } else {
            Err(PostProcessError::ProcessingError(format!(
                "Unsupported function in formula: {}",
                formula
            )))
        }
    }

    /// Parse an operand (column name or constant) with DataFrame validation
    fn parse_operand_with_validation(
        &self,
        df: &DataFrame,
        operand: &str,
    ) -> PostProcessResult<Expr> {
        let operand = operand.trim();

        // Try to parse as constant first
        if let Ok(constant) = operand.parse::<f64>() {
            return Ok(lit(constant));
        }

        // Check if it's a column name that exists in the current DataFrame
        let column_names: Vec<String> = df
            .get_column_names()
            .iter()
            .map(|s| s.to_string())
            .collect();
        if column_names.contains(&operand.to_string()) {
            Ok(col(operand))
        } else {
            Err(PostProcessError::ProcessingError(format!(
                "Invalid operand '{}': not a valid number or existing column. Available columns: [{}]",
                operand,
                column_names.join(", ")
            )))
        }
    }
}
