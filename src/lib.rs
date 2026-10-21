use bytes::Bytes;
use phf::phf_map;
use s3::{bucket::Bucket, creds::Credentials, region::Region};
use std::{
    collections::VecDeque,
    error::Error,
    io::{Read, Seek},
};
use strum_macros::Display;
use url::Url;

const CACHE_ENTRIES: usize = 16;
const MIN_READ: u64 = 8 * 1024 * 1024; // 8 MB

#[derive(Debug)]
struct FileTypeLUTEntry {
    front_cache: Option<u64>,
    back_cache: Option<u64>,
}

const FILETYPE_LUT: phf::Map<&str, FileTypeLUTEntry> = phf_map! {
    "zip" => FileTypeLUTEntry { front_cache: Some(8192), back_cache: Some(8192) },
    "tif" => FileTypeLUTEntry { front_cache: Some(8192), back_cache: Some(8192) },
};

#[derive(Debug)]
pub struct S3IO {
    client: Box<Bucket>,
    pos: u64,
    bucket: String,
    key: String,
    size: u64,
    min_read: u64,
    cache_ranges: VecDeque<Cache>,
    stats: IOStats,
}
#[derive(Debug, Default)]
pub struct IOStats {
    non_cached_reads: u64,
    non_cached_bytes_read: u64,
    cached_reads: u64,
    cached_bytes_read: u64,
    s3_reads: u64,
    s3_bytes_read: u64,
}

impl Seek for S3IO {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        let new_pos = match pos {
            std::io::SeekFrom::Start(offset) => offset,
            std::io::SeekFrom::End(offset) => {
                if offset < 0 {
                    self.size.checked_sub(offset.unsigned_abs()).ok_or_else(|| {
                        std::io::Error::new(std::io::ErrorKind::InvalidInput, "Seek before start of file")
                    })?
                } else {
                    self.size.checked_add(offset.cast_unsigned()).ok_or_else(|| {
                        std::io::Error::new(std::io::ErrorKind::InvalidInput, "Seek beyond end of file")
                    })?
                }
            }
            std::io::SeekFrom::Current(offset) => {
                if offset < 0 {
                    self.pos.checked_sub(offset.unsigned_abs()).ok_or_else(|| {
                        std::io::Error::new(std::io::ErrorKind::InvalidInput, "Seek before start of file")
                    })?
                } else {
                    self.pos.checked_add(offset.cast_unsigned()).ok_or_else(|| {
                        std::io::Error::new(std::io::ErrorKind::InvalidInput, "Seek beyond end of file")
                    })?
                }
            }
        };
        if new_pos > self.size {
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "Seek beyond end of file"));
        }
        self.pos = new_pos;
        tracing::trace!("S3IO::seek - Bucket: {}, Key: {}, NewPos: {}", self.bucket, self.key, self.pos);
        Ok(self.pos)
    }
}

impl Read for S3IO {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let requested_read_size = buf.len() as u64;
        tracing::trace!(
            "S3IO::read - Bucket: {}, Key: {}, Pos: {}, BufLen: {}, ReadSize: {requested_read_size}",
            self.bucket,
            self.key,
            self.pos,
            buf.len()
        );
        let start = self.pos;
        // don't read beyond the end of the file
        let end = self.pos + requested_read_size - 1;
        tracing::trace!("S3IO::read - Reading from {start} to {end}");
        for cache_range in &self.cache_ranges {
            // fully within an existing cache range
            if start >= cache_range.start && end <= cache_range.end {
                tracing::trace!(
                    "S3IO::read - Using cached range: {} -> {}",
                    cache_range.start,
                    cache_range.end
                );
                let cache_start = usize::try_from(start - cache_range.start)
                    .map_err(|e| std::io::Error::other(format!("Failed to calculate cache start: {e}")))?;
                let cache_end = usize::try_from(end - cache_range.start)
                    .map_err(|e| std::io::Error::other(format!("Failed to calculate cache end: {e}")))?;
                let bytes = &cache_range.bytes[cache_start..cache_end];
                buf[..bytes.len()].copy_from_slice(bytes);
                self.pos += bytes.len() as u64;
                self.stats.cached_reads += 1;
                self.stats.cached_bytes_read += bytes.len() as u64;
                return Ok(bytes.len());
            }
        }
        let actual_read_size = if requested_read_size < self.min_read {
            tracing::trace!(
                "S3IO::read - Requested read size {requested_read_size} is less than min_read {}, using min_read",
                self.min_read
            );
            std::cmp::min(self.size - self.pos, self.min_read) // don't read beyond the end of the file
        } else {
            requested_read_size
        };
        // not within an existing cache range
        tracing::trace!(
            "S3IO::read - Not in cache, reading from S3: {start} -> {}",
            start + actual_read_size as u64 - 1
        );
        let end = self.pos + actual_read_size as u64 - 1;
        let bytes = self
            .client
            .get_object_range(&self.key, start, Some(end))
            .map_err(|e| std::io::Error::other(format!("Failed to read from S3: {e}")))?
            .into_bytes();
        self.stats.s3_reads += 1;
        self.stats.s3_bytes_read += bytes.len() as u64;
        // copy what was requested into the buffer, even if we read more than requested
        let buflen = buf.len();
        buf[..buflen].copy_from_slice(&bytes[..buflen]);
        self.pos += requested_read_size;
        tracing::trace!("S3IO::read - Read {requested_read_size} bytes from S3, pos is now {}", self.pos);
        self.stats.non_cached_reads += 1;
        self.stats.non_cached_bytes_read += requested_read_size;
        // add the new range to the cache_ranges vector
        tracing::trace!("S3IO::read - Adding new cache range: {start} -> {end}");
        if self.cache_ranges.len() == CACHE_ENTRIES {
            tracing::trace!("S3IO::read - Cache is full, removing oldest cache range");
            self.cache_ranges.pop_front(); // Removes the oldest cache range
        }
        self.cache_ranges.push_back(Cache { start, end, bytes });
        Ok(buflen)
    }
}

#[derive(Debug)]
pub struct Cache {
    start: u64,
    end: u64,
    bytes: Bytes,
}

#[derive(Clone, Copy, Debug, Default, Display)]
pub enum FileType {
    #[default]
    Any,
    Tif,
    Zip,
}

#[derive(Clone, Debug, Default)]
pub struct S3IOOpts {
    pub filetype: Option<FileType>,
    pub min_read: Option<u64>,
}

impl S3IO {
    /// # Errors
    ///
    /// Returns an error if the S3 object cannot be accessed or if the content length cannot
    /// be determined.
    pub fn new(bucket: String, key: String, options: &S3IOOpts) -> Result<Self, Box<dyn Error>> {
        let credentials = Credentials::from_instance_metadata(false)?;
        let client = Bucket::new(&bucket, Region::CaCentral1, credentials)?;
        let (meta, code) = client.head_object(&key)?;
        if code != 200 {
            return Err(format!(
                "Failed to get metadata for S3 object: s3://{bucket}/{key} - HTTP code: {code}"
            )
            .into());
        }
        let Some(content_length) = meta.content_length else {
            return Err("Failed to get content length from S3 object metadata".into());
        };
        let size = u64::try_from(content_length)?;
        tracing::trace!("S3IO::new - Bucket: {bucket}, Key: {key}, Size: {size}");
        let mut cache_ranges = VecDeque::with_capacity(CACHE_ENTRIES); // Initialize cache_ranges as an empty vector

        // lookup filetype cache options
        let filetype = match options.filetype {
            Some(FileType::Tif) => "tif",
            Some(FileType::Zip) => "zip",
            _ => "any",
        };
        let cache_options =
            FILETYPE_LUT.get(filetype).unwrap_or(&FileTypeLUTEntry { front_cache: None, back_cache: None });
        let min_read = options.min_read.unwrap_or(MIN_READ);
        tracing::trace!(
            "S3IO::new - FileType: {filetype}, FrontCache: {:?}, BackCache: {:?}, MinRead: {min_read}",
            cache_options.front_cache,
            cache_options.back_cache,
        );

        let mut stats = IOStats::default();
        if let Some(cache_read_size) = cache_options.front_cache {
            let range_start = 0;
            let range_end = range_start + cache_read_size - 1;
            let bytes = client.get_object_range(&key, range_start, Some(range_end))?.into_bytes();
            stats.s3_reads += 1;
            stats.s3_bytes_read += bytes.len() as u64;
            let len = bytes.len();
            tracing::trace!(
                "{key} - Filled front cache with range: {range_start} -> {range_end} - Length: {len}",
            );
            cache_ranges.push_back(Cache { start: range_start, end: range_end, bytes });
            stats.non_cached_reads += 1;
            stats.non_cached_bytes_read += len as u64;
        }
        if let Some(cache_read_size) = cache_options.back_cache {
            let range_start = size - cache_read_size;
            let range_end = size - 1;
            let bytes = client.get_object_range(&key, range_start, Some(range_end))?.into_bytes();
            stats.s3_reads += 1;
            stats.s3_bytes_read += bytes.len() as u64;
            let len = bytes.len();
            tracing::trace!(
                "{key} - Filled back cache with range: {range_start} -> {range_end} - Length: {len}",
            );
            cache_ranges.push_back(Cache { start: range_start, end: range_end, bytes });
            stats.non_cached_reads += 1;
            stats.non_cached_bytes_read += len as u64;
        }
        Ok(Self { client, pos: 0, bucket, key, size, min_read, cache_ranges, stats })
    }
    pub fn stats(&self) -> &IOStats {
        tracing::trace!(
            "S3IO::stats - Bucket: {}, Key: {}, NonCachedReads: {}, NonCachedBytesRead: {}, CachedReads: {}, CachedBytesRead: {}",
            self.bucket,
            self.key,
            self.stats.non_cached_reads,
            self.stats.non_cached_bytes_read,
            self.stats.cached_reads,
            self.stats.cached_bytes_read
        );
        &self.stats
    }
}

/// # Errors
///
/// Returns an error string if the URL is invalid or missing components.
pub fn s3url_to_bucket_key(url: &str) -> Result<(String, String), String> {
    let url = Url::parse(url).map_err(|e| e.to_string())?;
    if url.path().len() < 2 {
        return Err("Missing Key".to_string());
    }
    let url_path = url.path().to_string();
    let key = url_path.strip_prefix("/").ok_or_else(|| "Missing Key".to_string())?.to_string();
    let host = url.host().ok_or_else(|| "Invalid URL".to_string())?.to_string();
    Ok((host, key))
}
