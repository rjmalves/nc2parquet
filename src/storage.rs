//! # Storage Abstraction Module
//! 
//! This module provides a unified interface for reading and writing files from different storage backends,
//! including local filesystem and Amazon S3. The abstraction allows the application to seamlessly work
//! with different storage systems based on the file path pattern.
//! 
//! ## Features
//! 
//! - **Unified API**: Same interface for local and S3 operations
//! - **Path-based detection**: Automatically detects storage backend from path (s3:// vs local)
//! - **Async operations**: Full async support for all storage operations
//! - **Error handling**: Comprehensive error types with detailed context
//! - **Credential management**: AWS credentials from environment variables
//! 
//! ## Path Patterns
//! 
//! - **S3 paths**: `s3://bucket-name/path/to/file.nc`
//! - **Local paths**: `/absolute/path/to/file.nc` or `relative/path/to/file.nc`
//! 
//! ## Usage Example
//! 
//! ```rust,no_run
//! use nc2parquet::storage::{StorageFactory, StorageBackend};
//! 
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Automatically detect storage type from path
//!     let storage = StorageFactory::from_path("s3://my-bucket/data.nc").await?;
//!     
//!     // Read data using unified interface
//!     let data = storage.read("s3://my-bucket/data.nc").await?;
//!     
//!     // Write data back
//!     storage.write("s3://my-bucket/output.parquet", &data).await?;
//!     
//!     Ok(())
//! }
//! ```

use aws_config::BehaviorVersion;
use aws_sdk_s3::Client as S3Client;
use std::path::Path;
use thiserror::Error;
use tokio::fs;

/// Errors that can occur during storage operations
#[derive(Error, Debug)]
pub enum StorageError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    
    #[error("AWS S3 GetObject error: {0}")]
    S3GetObject(#[from] aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::get_object::GetObjectError>),
    
    #[error("AWS S3 PutObject error: {0}")]
    S3PutObject(#[from] aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::put_object::PutObjectError>),
    
    #[error("AWS S3 HeadObject error: {0}")]
    S3HeadObject(#[from] aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::head_object::HeadObjectError>),
    
    #[error("AWS ByteStream error: {0}")]
    ByteStream(String),
    
    #[error("Invalid S3 path format: {0}")]
    InvalidS3Path(String),
    
    #[error("Path not found: {0}")]
    PathNotFound(String),
    
    #[error("Permission denied: {0}")]
    PermissionDenied(String),
    
    #[error("Invalid path format: {0}")]
    InvalidPath(String),
}

/// Result type for storage operations
pub type StorageResult<T> = Result<T, StorageError>;

/// Trait defining the interface for storage backends
/// 
/// This trait provides a unified interface for different storage systems.
/// All operations are async to support both local and remote operations efficiently.
#[async_trait::async_trait]
pub trait StorageBackend: Send + Sync {
    /// Reads the entire contents of a file
    /// 
    /// # Arguments
    /// * `path` - The path to the file to read
    /// 
    /// # Returns
    /// Returns the file contents as bytes on success
    /// 
    /// # Errors
    /// Returns `StorageError` if the file cannot be read
    async fn read(&self, path: &str) -> StorageResult<Vec<u8>>;
    
    /// Writes data to a file, creating it if it doesn't exist
    /// 
    /// # Arguments
    /// * `path` - The path where to write the file
    /// * `data` - The data to write
    /// 
    /// # Returns
    /// Returns `()` on successful write
    /// 
    /// # Errors
    /// Returns `StorageError` if the file cannot be written
    async fn write(&self, path: &str, data: &[u8]) -> StorageResult<()>;
    
    /// Checks if a file exists at the given path
    /// 
    /// # Arguments
    /// * `path` - The path to check
    /// 
    /// # Returns
    /// Returns `true` if the file exists, `false` otherwise
    /// 
    /// # Errors
    /// Returns `StorageError` if the existence cannot be determined
    async fn exists(&self, path: &str) -> StorageResult<bool>;
}

/// Local filesystem storage backend
/// 
/// Implements storage operations for local files using tokio's async file operations.
#[derive(Debug, Clone)]
pub struct LocalStorage;

#[async_trait::async_trait]
impl StorageBackend for LocalStorage {
    async fn read(&self, path: &str) -> StorageResult<Vec<u8>> {
        match fs::read(path).await {
            Ok(data) => Ok(data),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                Err(StorageError::PathNotFound(path.to_string()))
            }
            Err(e) if e.kind() == std::io::ErrorKind::PermissionDenied => {
                Err(StorageError::PermissionDenied(path.to_string()))
            }
            Err(e) => Err(StorageError::Io(e)),
        }
    }
    
    async fn write(&self, path: &str, data: &[u8]) -> StorageResult<()> {
        // Create parent directories if they don't exist
        if let Some(parent) = Path::new(path).parent() {
            fs::create_dir_all(parent).await.map_err(StorageError::Io)?;
        }
        
        match fs::write(path, data).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::PermissionDenied => {
                Err(StorageError::PermissionDenied(path.to_string()))
            }
            Err(e) => Err(StorageError::Io(e)),
        }
    }
    
    async fn exists(&self, path: &str) -> StorageResult<bool> {
        match fs::metadata(path).await {
            Ok(_) => Ok(true),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(StorageError::Io(e)),
        }
    }
}

/// Amazon S3 storage backend
/// 
/// Implements storage operations for S3 objects using the AWS SDK.
/// Credentials are automatically loaded from environment variables or AWS configuration.
#[derive(Debug, Clone)]
pub struct S3Storage {
    client: S3Client,
}

impl S3Storage {
    /// Creates a new S3Storage instance with default AWS configuration
    /// 
    /// This will load AWS credentials from:
    /// - Environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
    /// - AWS credentials file
    /// - IAM roles (when running on AWS infrastructure)
    /// 
    /// # Returns
    /// Returns a configured S3Storage instance
    pub async fn new() -> StorageResult<Self> {
        let config = aws_config::defaults(BehaviorVersion::latest()).load().await;
        let client = S3Client::new(&config);
        
        Ok(S3Storage { client })
    }
    
    /// Creates a new S3Storage instance with custom configuration
    /// 
    /// # Arguments
    /// * `config` - AWS SDK configuration
    /// 
    /// # Returns
    /// Returns a configured S3Storage instance
    pub fn from_config(config: &aws_config::SdkConfig) -> Self {
        let client = S3Client::new(config);
        S3Storage { client }
    }
    
    /// Parses an S3 path into bucket and key components
    /// 
    /// # Arguments
    /// * `s3_path` - S3 path in format s3://bucket/key
    /// 
    /// # Returns
    /// Returns (bucket, key) tuple on success
    /// 
    /// # Errors
    /// Returns `StorageError::InvalidS3Path` if the path format is invalid
    fn parse_s3_path(s3_path: &str) -> StorageResult<(String, String)> {
        if !s3_path.starts_with("s3://") {
            return Err(StorageError::InvalidS3Path(format!(
                "S3 path must start with 's3://': {}", s3_path
            )));
        }
        
        let path_without_scheme = &s3_path[5..]; // Remove "s3://"
        let parts: Vec<&str> = path_without_scheme.splitn(2, '/').collect();
        
        if parts.len() != 2 || parts[0].is_empty() || parts[1].is_empty() {
            return Err(StorageError::InvalidS3Path(format!(
                "Invalid S3 path format. Expected 's3://bucket/key': {}", s3_path
            )));
        }
        
        Ok((parts[0].to_string(), parts[1].to_string()))
    }
}

#[async_trait::async_trait]
impl StorageBackend for S3Storage {
    async fn read(&self, path: &str) -> StorageResult<Vec<u8>> {
        let (bucket, key) = Self::parse_s3_path(path)?;
        
        let response = self.client
            .get_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| match &e {
                aws_sdk_s3::error::SdkError::ServiceError(service_err) 
                    if service_err.err().is_no_such_key() => {
                    StorageError::PathNotFound(path.to_string())
                }
                _ => StorageError::S3GetObject(e),
            })?;
        
        let data = response.body.collect().await
            .map_err(|e| StorageError::ByteStream(e.to_string()))?
            .into_bytes()
            .to_vec();
        
        Ok(data)
    }
    
    async fn write(&self, path: &str, data: &[u8]) -> StorageResult<()> {
        let (bucket, key) = Self::parse_s3_path(path)?;
        
        self.client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(aws_sdk_s3::primitives::ByteStream::from(data.to_vec()))
            .send()
            .await
            .map_err(StorageError::S3PutObject)?;
        
        Ok(())
    }
    
    async fn exists(&self, path: &str) -> StorageResult<bool> {
        let (bucket, key) = Self::parse_s3_path(path)?;
        
        match self.client
            .head_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
        {
            Ok(_) => Ok(true),
            Err(aws_sdk_s3::error::SdkError::ServiceError(service_err)) 
                if service_err.err().is_not_found() => Ok(false),
            Err(e) => Err(StorageError::S3HeadObject(e)),
        }
    }
}

/// Storage backend enumeration
/// 
/// Represents the different types of storage backends available.
#[derive(Debug)]
pub enum Storage {
    Local(LocalStorage),
    S3(S3Storage),
}

#[async_trait::async_trait]
impl StorageBackend for Storage {
    async fn read(&self, path: &str) -> StorageResult<Vec<u8>> {
        match self {
            Storage::Local(storage) => storage.read(path).await,
            Storage::S3(storage) => storage.read(path).await,
        }
    }
    
    async fn write(&self, path: &str, data: &[u8]) -> StorageResult<()> {
        match self {
            Storage::Local(storage) => storage.write(path, data).await,
            Storage::S3(storage) => storage.write(path, data).await,
        }
    }
    
    async fn exists(&self, path: &str) -> StorageResult<bool> {
        match self {
            Storage::Local(storage) => storage.exists(path).await,
            Storage::S3(storage) => storage.exists(path).await,
        }
    }
}

/// Factory for creating storage backends based on path patterns
/// 
/// This factory automatically detects the appropriate storage backend based on the file path:
/// - Paths starting with "s3://" use S3Storage
/// - All other paths use LocalStorage
pub struct StorageFactory;

impl StorageFactory {
    /// Creates a storage backend based on the path format
    /// 
    /// # Arguments
    /// * `path` - The file path to analyze
    /// 
    /// # Returns
    /// Returns the appropriate storage backend
    /// 
    /// # Examples
    /// 
    /// ```rust,no_run
    /// use nc2parquet::storage::StorageFactory;
    /// 
    /// #[tokio::main]
    /// async fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     // S3 storage for s3:// paths
    ///     let s3_storage = StorageFactory::from_path("s3://my-bucket/file.nc").await?;
    ///     
    ///     // Local storage for other paths
    ///     let local_storage = StorageFactory::from_path("/local/path/file.nc").await?;
    ///     
    ///     Ok(())
    /// }
    /// ```
    pub async fn from_path(path: &str) -> StorageResult<Storage> {
        if path.starts_with("s3://") {
            let s3_storage = S3Storage::new().await?;
            Ok(Storage::S3(s3_storage))
        } else {
            Ok(Storage::Local(LocalStorage))
        }
    }
    
    /// Determines if a path is an S3 path
    /// 
    /// # Arguments
    /// * `path` - The path to check
    /// 
    /// # Returns
    /// Returns `true` if the path is an S3 path, `false` otherwise
    pub fn is_s3_path(path: &str) -> bool {
        path.starts_with("s3://")
    }
    
    /// Determines if a path is a local path
    /// 
    /// # Arguments
    /// * `path` - The path to check
    /// 
    /// # Returns
    /// Returns `true` if the path is a local path, `false` otherwise
    pub fn is_local_path(path: &str) -> bool {
        !Self::is_s3_path(path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    
    #[tokio::test]
    async fn test_local_storage_write_read() -> Result<(), Box<dyn std::error::Error>> {
        let storage = LocalStorage;
        let temp_dir = TempDir::new()?;
        let file_path = temp_dir.path().join("test_file.txt");
        let file_path_str = file_path.to_str().unwrap();
        
        let test_data = b"Hello, world!";
        
        // Test write
        storage.write(file_path_str, test_data).await?;
        
        // Test read
        let read_data = storage.read(file_path_str).await?;
        assert_eq!(read_data, test_data);
        
        // Test exists
        assert!(storage.exists(file_path_str).await?);
        
        Ok(())
    }
    
    #[tokio::test]
    async fn test_local_storage_not_found() -> Result<(), Box<dyn std::error::Error>> {
        let storage = LocalStorage;
        
        let result = storage.read("/nonexistent/path/file.txt").await;
        assert!(matches!(result, Err(StorageError::PathNotFound(_))));
        
        assert!(!storage.exists("/nonexistent/path/file.txt").await?);
        
        Ok(())
    }
    
    #[test]
    fn test_s3_path_parsing() {
        // Valid S3 paths
        let (bucket, key) = S3Storage::parse_s3_path("s3://my-bucket/path/to/file.nc").unwrap();
        assert_eq!(bucket, "my-bucket");
        assert_eq!(key, "path/to/file.nc");
        
        let (bucket, key) = S3Storage::parse_s3_path("s3://bucket/file.nc").unwrap();
        assert_eq!(bucket, "bucket");
        assert_eq!(key, "file.nc");
        
        // Invalid S3 paths
        assert!(S3Storage::parse_s3_path("http://bucket/file.nc").is_err());
        assert!(S3Storage::parse_s3_path("s3://").is_err());
        assert!(S3Storage::parse_s3_path("s3://bucket").is_err());
        assert!(S3Storage::parse_s3_path("s3:///file.nc").is_err());
    }
    
    #[tokio::test]
    async fn test_storage_factory_path_detection() -> Result<(), Box<dyn std::error::Error>> {
        // Test S3 path detection
        assert!(StorageFactory::is_s3_path("s3://my-bucket/file.nc"));
        assert!(!StorageFactory::is_s3_path("/local/path/file.nc"));
        assert!(!StorageFactory::is_s3_path("relative/path/file.nc"));
        
        // Test local path detection
        assert!(StorageFactory::is_local_path("/local/path/file.nc"));
        assert!(StorageFactory::is_local_path("relative/path/file.nc"));
        assert!(!StorageFactory::is_local_path("s3://my-bucket/file.nc"));
        
        // Test factory creation for local paths
        let local_storage = StorageFactory::from_path("/local/path/file.nc").await?;
        assert!(matches!(local_storage, Storage::Local(_)));
        
        Ok(())
    }
    
    #[tokio::test]
    async fn test_storage_enum_local_operations() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = TempDir::new()?;
        let file_path = temp_dir.path().join("test_file.txt");
        let file_path_str = file_path.to_str().unwrap();
        
        let storage = Storage::Local(LocalStorage);
        let test_data = b"Test data for storage enum";
        
        // Test write and read through enum
        storage.write(file_path_str, test_data).await?;
        let read_data = storage.read(file_path_str).await?;
        assert_eq!(read_data, test_data);
        
        // Test exists
        assert!(storage.exists(file_path_str).await?);
        
        Ok(())
    }
}

#[cfg(test)]
mod integration_tests {
    use super::*;
    
    #[tokio::test]
    #[ignore] // Ignore by default as it requires AWS credentials and S3 access
    async fn test_s3_storage_real_aws() -> Result<(), Box<dyn std::error::Error>> {
        // This test requires real AWS credentials and an S3 bucket
        // Set these environment variables to run:
        // AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION
        // TEST_S3_BUCKET (e.g., "my-test-bucket")
        
        let test_bucket = match std::env::var("TEST_S3_BUCKET") {
            Ok(bucket) => bucket,
            Err(_) => {
                println!("Skipping S3 integration test - set TEST_S3_BUCKET environment variable");
                return Ok(());
            }
        };
        
        let storage = S3Storage::new().await?;
        let test_data = b"Integration test data for real S3";
        let s3_path = format!("s3://{}/test-integration/test-file.txt", test_bucket);
        
        // Test write
        storage.write(&s3_path, test_data).await?;
        
        // Test exists  
        assert!(storage.exists(&s3_path).await?);
        
        // Test read
        let read_data = storage.read(&s3_path).await?;
        assert_eq!(read_data, test_data);
        
        println!("S3 integration test passed with bucket: {}", test_bucket);
        Ok(())
    }
    
    #[tokio::test]
    #[ignore] // Ignore by default as it requires AWS credentials
    async fn test_storage_factory_real_s3() -> Result<(), Box<dyn std::error::Error>> {
        let test_bucket = match std::env::var("TEST_S3_BUCKET") {
            Ok(bucket) => bucket,
            Err(_) => {
                println!("Skipping S3 factory test - set TEST_S3_BUCKET environment variable");
                return Ok(());
            }
        };
        
        let storage = StorageFactory::from_path(&format!("s3://{}/test.txt", test_bucket)).await?;
        assert!(matches!(storage, Storage::S3(_)));
        
        let test_data = b"Factory test data";
        let s3_path = format!("s3://{}/test-factory/factory-test.txt", test_bucket);
        
        storage.write(&s3_path, test_data).await?;
        let read_data = storage.read(&s3_path).await?;
        assert_eq!(read_data, test_data);
        
        println!("S3 factory test passed with bucket: {}", test_bucket);
        Ok(())
    }
}