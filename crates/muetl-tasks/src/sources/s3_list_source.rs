//! S3ListSource - listens for new objects in an S3 bucket.
//!
//! This source polls an S3 bucket for objects and emits events for each
//! new object discovered. It tracks previously seen objects to avoid
//! emitting duplicates.
//!
//! S3 object metadata is passed as event headers:
//! - `s3_bucket`: The bucket name
//! - `s3_key`: The object key (path)
//! - `s3_size`: Size in bytes (as string)
//! - `s3_etag`: ETag (entity tag), if available
//! - `s3_last_modified`: Last modified timestamp as ISO 8601 string, if available

use std::collections::HashSet;

use async_trait::async_trait;
use aws_sdk_s3::Client as S3Client;
use muetl::{impl_config_template, impl_source_handler, prelude::*};
use regex::Regex;

/// S3ListSource polls an S3 bucket for objects and emits events for new objects.
///
/// Configuration:
/// - `bucket` (required): The S3 bucket name to monitor
/// - `prefix` (optional): Only list objects with this key prefix
/// - `region` (optional): AWS region (defaults to AWS SDK default resolution)
/// - `endpoint` (optional): Custom S3-compatible endpoint URL (e.g., for MinIO)
/// - `access_key_id` (optional): AWS access key ID (defaults to AWS SDK credential chain)
/// - `secret_access_key` (optional): AWS secret access key (defaults to AWS SDK credential chain)
///
/// Filter options:
/// - `extensions` (optional): Array of file extensions to include (e.g., ["json", "csv"])
/// - `min_size` (optional): Minimum file size in bytes (inclusive)
/// - `max_size` (optional): Maximum file size in bytes (inclusive)
/// - `key_pattern` (optional): Regex pattern that keys must match
/// - `exclude_pattern` (optional): Regex pattern to exclude matching keys
///
/// Emits events on the "object" connection with S3 metadata as headers:
/// - `s3_bucket`, `s3_key`, `s3_size`, `s3_etag`, `s3_last_modified`
///
/// The event data payload is a unit value `()`.
///
/// Note: This source runs indefinitely, polling S3 for new objects. It tracks
/// seen objects in memory to avoid emitting duplicates across polls.
pub struct S3ListSource {
    client: Option<S3Client>,
    bucket: String,
    prefix: Option<String>,
    region: Option<String>,
    endpoint: Option<String>,
    access_key_id: Option<String>,
    secret_access_key: Option<String>,
    seen_keys: HashSet<String>,
    initialized: bool,
    // Filter options
    extensions: Option<Vec<String>>,
    min_size: Option<i64>,
    max_size: Option<i64>,
    key_pattern: Option<Regex>,
    exclude_pattern: Option<Regex>,
}

impl S3ListSource {
    pub fn new(config: &TaskConfig) -> Result<Box<dyn Source>, String> {
        let bucket = config.require_str("bucket").to_string();
        let prefix = config.get_str("prefix").map(|s| s.to_string());
        let region = config.get_str("region").map(|s| s.to_string());
        let endpoint = config.get_str("endpoint").map(|s| s.to_string());
        let access_key_id = config.get_str("access_key_id").map(|s| s.to_string());
        let secret_access_key = config.get_str("secret_access_key").map(|s| s.to_string());

        // Parse filter options
        let extensions = config.get_arr("extensions").map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_lowercase()))
                .collect()
        });

        let min_size = config.get_i64("min_size");
        let max_size = config.get_i64("max_size");

        let key_pattern = config
            .get_str("key_pattern")
            .map(|p| Regex::new(p).map_err(|e| format!("invalid key_pattern regex: {}", e)))
            .transpose()?;

        let exclude_pattern = config
            .get_str("exclude_pattern")
            .map(|p| Regex::new(p).map_err(|e| format!("invalid exclude_pattern regex: {}", e)))
            .transpose()?;

        Ok(Box::new(S3ListSource {
            client: None,
            bucket,
            prefix,
            region,
            endpoint,
            access_key_id,
            secret_access_key,
            seen_keys: HashSet::new(),
            initialized: false,
            extensions,
            min_size,
            max_size,
            key_pattern,
            exclude_pattern,
        }))
    }

    /// Check if an S3 object passes all configured filters.
    fn passes_filters(&self, key: &str, size: i64) -> bool {
        // Check extension filter
        if let Some(extensions) = &self.extensions {
            let key_lower = key.to_lowercase();
            let has_matching_ext = extensions
                .iter()
                .any(|ext| key_lower.ends_with(&format!(".{}", ext)));
            if !has_matching_ext {
                return false;
            }
        }

        // Check size filters
        if let Some(min) = self.min_size {
            if size < min {
                return false;
            }
        }
        if let Some(max) = self.max_size {
            if size > max {
                return false;
            }
        }

        // Check key pattern (must match)
        if let Some(pattern) = &self.key_pattern {
            if !pattern.is_match(key) {
                return false;
            }
        }

        // Check exclude pattern (must not match)
        if let Some(pattern) = &self.exclude_pattern {
            if pattern.is_match(key) {
                return false;
            }
        }

        true
    }

    async fn ensure_client(&mut self) {
        if self.client.is_some() {
            return;
        }

        let mut config_loader = aws_config::defaults(aws_config::BehaviorVersion::latest());

        if let Some(region) = &self.region {
            config_loader = config_loader.region(aws_config::Region::new(region.clone()));
        }

        if let (Some(access_key), Some(secret_key)) = (&self.access_key_id, &self.secret_access_key)
        {
            let credentials =
                aws_sdk_s3::config::Credentials::new(access_key, secret_key, None, None, "config");
            config_loader = config_loader.credentials_provider(credentials);
        }

        let config = config_loader.load().await;

        let client = if let Some(endpoint) = &self.endpoint {
            let s3_config = aws_sdk_s3::config::Builder::from(&config)
                .endpoint_url(endpoint)
                .force_path_style(true)
                .build();
            S3Client::from_conf(s3_config)
        } else {
            S3Client::new(&config)
        };

        self.client = Some(client);
    }
}

impl TaskDef for S3ListSource {}

impl Output<()> for S3ListSource {
    const conn_name: &'static str = "object";
}

#[async_trait]
impl Source for S3ListSource {
    async fn run(&mut self, ctx: &MuetlContext) {
        self.ensure_client().await;

        let client = self.client.as_ref().unwrap();

        let mut request = client.list_objects_v2().bucket(&self.bucket);

        if let Some(prefix) = &self.prefix {
            request = request.prefix(prefix);
        }

        match request.send().await {
            Ok(output) => {
                let contents = output.contents();

                for object in contents {
                    if let Some(key) = object.key() {
                        let size = object.size().unwrap_or(0);

                        // Skip if already seen or doesn't pass filters
                        if self.seen_keys.contains(key) || !self.passes_filters(key, size) {
                            continue;
                        }

                        self.seen_keys.insert(key.to_string());

                        // Build headers with S3 metadata
                        let mut headers = HashMap::new();
                        headers.insert("s3_bucket".to_string(), self.bucket.clone());
                        headers.insert("s3_key".to_string(), key.to_string());
                        headers.insert("s3_size".to_string(), size.to_string());
                        if let Some(etag) = object.e_tag() {
                            headers.insert("s3_etag".to_string(), etag.to_string());
                        }
                        if let Some(last_modified) = object.last_modified() {
                            headers
                                .insert("s3_last_modified".to_string(), last_modified.to_string());
                        }

                        let event_name = format!("s3://{}/{}", self.bucket, key);
                        ctx.results
                            .send(Event::new(
                                event_name,
                                "object".to_string(),
                                headers,
                                Arc::new(()),
                            ))
                            .await
                            .unwrap();
                    }
                }

                self.initialized = true;
            }
            Err(e) => {
                tracing::error!(error = ?e, bucket = %self.bucket, "Error listing S3 objects");
            }
        }
    }
}

impl_source_handler!(S3ListSource, task_id = "s3_list_source", "object" => ());
impl_config_template!(
    S3ListSource,
    bucket: Str!,
    prefix: Str,
    region: Str,
    endpoint: Str,
    access_key_id: Str,
    secret_access_key: Str,
    key_pattern: Str,
    exclude_pattern: Str,
    min_size: Int,
    max_size: Int,
);
