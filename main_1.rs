use std::fs::{self, File};
use std::io::Write;
use std::os::unix::fs::MetadataExt;
use aws_sdk_s3;
use aws_config;
use anyhow;
use tokio;
use aws_smithy_http::byte_stream::{ByteStream};
use std::path::PathBuf;
use std::fs::OpenOptions;

#[tokio::main]
async fn main() -> anyhow::Result<()> {

    // 创建 S3 客户端
    let config =  aws_config::load_from_env().await;
    let client = aws_sdk_s3::Client::new(&config);


    // 设置下载对象的参数
    let bucket_name = "dawei01";
    let key = "ddfile10M";
    let file_path = "s3_download_file";

    let mut range_size = 1024;
    let mut file = OpenOptions::new().write(true).append(true).create(true).open(file_path)?;

    let mut start = fs::metadata(file_path)?.size();
    let object_size = client.head_object().bucket(bucket_name).key(key).send().await?.content_length().unwrap() as u64;

    while start<object_size{

    if start +range_size > object_size {
        range_size = object_size - start;
    }

    // 创建一个 Range 对象来指定下载范围
    let range = format!("bytes={}-{}",start,start + range_size);


    // 发送 GetObject 请求
    let res = client
        .get_object()
        .bucket(bucket_name)
        .key(key)
        .range(range)
        .send()
        .await?;

    // 将响应体写入本地文件
    let body_stream = res.body.collect().await?.to_vec();
    let body = body_stream.as_slice();
    file.write(body)?;
    start = start+range_size;
    }

    Ok(())
}

