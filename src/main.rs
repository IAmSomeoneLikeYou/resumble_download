use anyhow;
use aws_config;
use aws_sdk_s3;
use aws_sdk_s3::presigning::PresigningConfig;
use aws_smithy_http::byte_stream::ByteStream;
use aws_smithy_http::response;
use reqwest;
use std::fs::OpenOptions;
use std::fs::{self, File};
use std::io::{Seek, SeekFrom, Write};
use std::os::unix::fs::MetadataExt;
use std::path::PathBuf;
use std::time::Duration;
use tokio;
use regex::Regex;


#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 创建 S3 客户端
    let config = aws_config::load_from_env().await;
    let client = aws_sdk_s3::Client::new(&config);

    // 设置下载对象的参数
    let bucket_name = "dawei01";
    let key = "robot.png";
    let file_path = "s3_download_file.png";

    //生成presigned url
    let expires_in = Duration::from_secs(3600 * 24);
    let presigned_request = client
        .get_object()
        .bucket(bucket_name)
        .key(key)
        .presigned(PresigningConfig::expires_in(expires_in)?)
        .await?;
    let presigned_url = presigned_request.uri();

    let  range_size = 1024*3;
    let mut file = OpenOptions::new()
        .write(true)
        .append(true)
        .create(true)
        .open(file_path)?;

    let mut start = fs::metadata(file_path)?.size();
    let req_client = reqwest::Client::new();

    loop {
        let range = format!("bytes={}-{}", start, start + range_size);
        let response = req_client
        .get(presigned_url)
        .header("Range", range)
        .send()
        .await?;
        match response.status().as_u16() {
            206 => {
                println!("Partial content received.");
                let content_range = response
                    .headers()
                    .get("Content-Range")
                    .and_then(|value| value.to_str().ok())
                    .unwrap_or("");
                println!("Content-Range: {}", content_range);
                let re = Regex::new(r"bytes (\d+)-(\d+)/\d+").unwrap();
             
                if let Some(caps) = re.captures(content_range) {
                     let begin = caps.get(1).map_or("", |m| m.as_str()).parse::<u64>().unwrap();
                     let end = caps.get(2).map_or("", |m| m.as_str()).parse::<u64>().unwrap();
                 
                println!("begin: {}, end: {}", begin, end);
                let actual_content_length = end - begin;
                let actual_content_length = actual_content_length as usize;

                let body_bytes = response.bytes().await?;
                let body_vec = body_bytes.to_vec();
                let body = body_vec.as_slice();
                let body_slice = &body[..actual_content_length];
                // file.seek(SeekFrom::Start(begin))?;
                file.write_all(&body_slice)?;
                }
            }
            416 => {
                println!("OK received.");
                return Ok(());
            }
            _ => {
                println!("Unexpected status code: {:?}", response.status());
                panic!()
            }
        }
        start = start + range_size;
    }

  
}
