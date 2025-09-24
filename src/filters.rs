use serde::{Deserialize};

#[derive(Debug, Clone, PartialEq)]
pub enum FilterResult {
    Single(Vec<usize>),
    Pairs(Vec<(usize, usize)>),
    Triplets(Vec<(usize, usize, usize)>),
}

impl FilterResult {
    pub fn as_single(&self) -> Option<&Vec<usize>> {
        match self {
            FilterResult::Single(indices) => Some(indices),
            _ => None,
        }
    }
    
    pub fn as_pairs(&self) -> Option<&Vec<(usize, usize)>> {
        match self {
            FilterResult::Pairs(pairs) => Some(pairs),
            _ => None,
        }
    }
    
    pub fn as_triplets(&self) -> Option<&Vec<(usize, usize, usize)>> {
        match self {
            FilterResult::Triplets(triplets) => Some(triplets),
            _ => None,
        }
    }
    
    pub fn len(&self) -> usize {
        match self {
            FilterResult::Single(v) => v.len(),
            FilterResult::Pairs(v) => v.len(),
            FilterResult::Triplets(v) => v.len(),
        }
    }
    
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

pub trait NCFilter {
    fn apply(&self, file: &netcdf::File) -> Result<FilterResult, Box<dyn std::error::Error>>;
}

#[derive(Deserialize)]
pub struct NCRangeFilter {
    pub dimension_name: String,
    pub min_value: f64,
    pub max_value: f64,
}

impl NCRangeFilter {
    pub fn new(dimension_name: &str, min_value: f64, max_value: f64) -> Self {
        NCRangeFilter {
            dimension_name:  dimension_name.to_string(),
            min_value,
            max_value,
        }
    }

    pub fn from_json(json_str: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let f: NCRangeFilter = serde_json::from_str(json_str)?;
        Ok(f)
    }
}

impl NCFilter for NCRangeFilter {
    fn apply(&self, file: &netcdf::File) -> Result<FilterResult, Box<dyn std::error::Error>> {
        if let Some(var) = file.variable(&self.dimension_name) {
            let values = var.get::<f64, _>(..)?;
            let filtered_indices: Vec<usize> = values
                .iter()
                .enumerate()
                .filter(|(_, val)| **val >= self.min_value && **val <= self.max_value)
                .map(|(idx, _)| idx)
                .collect();
            Ok(FilterResult::Single(filtered_indices))
        } else {
            Err(format!("Dimension variable '{}' not found", self.dimension_name).into())
        }
    }
}

#[derive(Deserialize)]
pub struct NCListFilter {
    pub dimension_name: String,
    pub values: Vec<f64>,
}

impl NCListFilter {
    pub fn new(dimension_name: &str, values: Vec<f64>) -> Self {
        NCListFilter {
            dimension_name: dimension_name.to_string(),
            values,
        }
    }
 
    pub fn from_json(json_str: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let f: NCListFilter = serde_json::from_str(json_str)?;
        Ok(f)
    }
}

impl NCFilter for NCListFilter {
    fn apply(&self, file: &netcdf::File) -> Result<FilterResult, Box<dyn std::error::Error>> {
        if let Some(var) = file.variable(&self.dimension_name) {
            let coord_values = var.get::<f64, _>(..)?;
            let filtered_indices: Vec<usize> = coord_values
                .iter()
                .enumerate()
                .filter(|(_, val)| self.values.contains(val))
                .map(|(idx, _)| idx)
                .collect();
            Ok(FilterResult::Single(filtered_indices))
        } else {
            Err(format!("Dimension variable '{}' not found", self.dimension_name).into())
        }
    }
}

#[derive(Deserialize)]
pub struct NC2DPointFilter {
    pub lat_dimension_name: String,
    pub lon_dimension_name: String,
    pub points: Vec<(f64, f64)>,
    pub tolerance: f64,
}

impl NC2DPointFilter {
    pub fn new(lat_dimension_name: &str, lon_dimension_name: &str, points: Vec<(f64, f64)>, tolerance: f64) -> Self {
        NC2DPointFilter {
            lat_dimension_name: lat_dimension_name.to_string(),
            lon_dimension_name: lon_dimension_name.to_string(),
            points,
            tolerance,
        }
    }
    
    pub fn from_json(json_str: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let f: NC2DPointFilter = serde_json::from_str(json_str)?;
        Ok(f)
    }
}

impl NCFilter for NC2DPointFilter {
    fn apply(&self, file: &netcdf::File) -> Result<FilterResult, Box<dyn std::error::Error>> {
        let lat_var = file.variable(&self.lat_dimension_name)
            .ok_or(format!("Latitude variable '{}' not found", self.lat_dimension_name))?;
        let lon_var = file.variable(&self.lon_dimension_name)
            .ok_or(format!("Longitude variable '{}' not found", self.lon_dimension_name))?;
        
        let lat_values = lat_var.get::<f64, _>(..)?;
        let lon_values = lon_var.get::<f64, _>(..)?;
        
        let mut filtered_indices = Vec::new();
        
        for &(target_lat, target_lon) in &self.points {
            for (i, &lat) in lat_values.iter().enumerate() {
                if (lat - target_lat).abs() <= self.tolerance {
                    for (j, &lon) in lon_values.iter().enumerate() {
                        if (lon - target_lon).abs() <= self.tolerance {
                            filtered_indices.push((i, j));
                        }
                    }
                }
            }
        }

        Ok(FilterResult::Pairs(filtered_indices))
    }
}

#[derive(Deserialize)]
pub struct NC3DPointFilter {
    pub time_dimension_name: String,
    pub lat_dimension_name: String,
    pub lon_dimension_name: String,
    pub steps: Vec<f64>,
    pub points: Vec<(f64, f64)>,
    pub tolerance: f64,
}

impl NC3DPointFilter {
    pub fn new(time_dimension_name: &str, lat_dimension_name: &str, lon_dimension_name: &str, steps: Vec<f64>, points: Vec<(f64, f64)>, tolerance: f64) -> Self {
        NC3DPointFilter {
            time_dimension_name: time_dimension_name.to_string(),
            lat_dimension_name: lat_dimension_name.to_string(),
            lon_dimension_name: lon_dimension_name.to_string(),
            steps,
            points,
            tolerance,
        }
    }
    
    pub fn from_json(json_str: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let f: NC3DPointFilter = serde_json::from_str(json_str)?;
        Ok(f)
    }
}

impl NCFilter for NC3DPointFilter {
    fn apply(&self, file: &netcdf::File) -> Result<FilterResult, Box<dyn std::error::Error>> {
        let time_var = file.variable(&self.time_dimension_name)
            .ok_or(format!("Time variable '{}' not found", self.time_dimension_name))?;
        let lat_var = file.variable(&self.lat_dimension_name)
            .ok_or(format!("Latitude variable '{}' not found", self.lat_dimension_name))?;
        let lon_var = file.variable(&self.lon_dimension_name)
            .ok_or(format!("Longitude variable '{}' not found", self.lon_dimension_name))?;
        let time_values = time_var.get::<f64, _>(..)?;
        let lat_values = lat_var.get::<f64, _>(..)?;
        let lon_values = lon_var.get::<f64, _>(..)?;

        let filtered_time_indices: Vec<usize> = time_values
                        .iter()
                        .enumerate()
                        .filter(|(_, val)| self.steps.contains(val))
                        .map(|(idx, _)| idx)
                        .collect();

        let mut filtered_indices = Vec::new();
        
        for &(target_lat, target_lon) in &self.points {
            for (i, &lat) in lat_values.iter().enumerate() {
                if (lat - target_lat).abs() <= self.tolerance {
                    for (j, &lon) in lon_values.iter().enumerate() {
                        if (lon - target_lon).abs() <= self.tolerance {
                            for &t_idx in &filtered_time_indices {
                                filtered_indices.push((t_idx, i, j));
                            }
                        }
                    }
                }
            }
        }

        Ok(FilterResult::Triplets(filtered_indices))
    }
}

pub fn filter_factory(json_str: &str) -> Result<Box<dyn NCFilter>, Box<dyn std::error::Error>> {
    let v: serde_json::Value = serde_json::from_str(json_str)?;
    if let Some(filter_kind) = v.get("kind").and_then(|t| t.as_str()) {
        match filter_kind {
            "range" => {
                let filter = NCRangeFilter::from_json(json_str)?;
                Ok(Box::new(filter))
            },
            "list" => {
                let filter = NCListFilter::from_json(json_str)?;
                Ok(Box::new(filter))
            },
            "2d_point" => {
                let filter = NC2DPointFilter::from_json(json_str)?;
                Ok(Box::new(filter))
            },
            "3d_point" => {
                let filter = NC3DPointFilter::from_json(json_str)?;
                Ok(Box::new(filter))
            },
            _ => Err(format!("Unknown filter kind: {}", filter_kind).into()),
        }
    } else {
        Err("Missing 'kind' field in JSON".into())
    }
}