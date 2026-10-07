//! 备份存储适配器的测试入口

#[cfg(target_os = "linux")]
mod pg_dump;
mod s3;
