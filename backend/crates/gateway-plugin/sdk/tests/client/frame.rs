//! 二进制帧往返、长度预算与不完整帧拒绝测试

use gateway_plugin_sdk::{
    Frame, FrameError, Message,
    client::{read_frame, write_frame},
};
use serde_json::json;

#[tokio::test]
async fn binary_payload_round_trips_without_json_encoding() {
    let frame = Frame {
        message: Message::Stream {
            id: 17,
            sequence: 2,
        },
        payload: vec![0, 255, 13, 10, 128],
    };
    let mut bytes = Vec::new();
    write_frame(&mut bytes, &frame).await.unwrap();
    assert!(bytes.ends_with(&frame.payload));
    assert_eq!(read_frame(&mut bytes.as_slice()).await.unwrap(), frame);
}

#[tokio::test]
async fn malicious_lengths_fail_before_reading_or_allocating_payload() {
    let bytes = [0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0];
    assert!(matches!(
        read_frame(&mut bytes.as_slice()).await,
        Err(FrameError::Length)
    ));
}

#[tokio::test]
async fn large_payload_is_exact_across_multiple_io_buffers_and_following_frames() {
    let frame = Frame {
        message: Message::Result {
            id: 1,
            result: json!({}),
        },
        payload: (0..101 * 1024 * 1024 + 13)
            .map(|index| (index % 251) as u8)
            .collect(),
    };
    let next = Frame::control(Message::Cancel { id: 3 });
    let (mut writer, mut reader) = tokio::io::duplex(16 * 1024);
    let writing = tokio::spawn(async move {
        write_frame(&mut writer, &frame).await.unwrap();
        write_frame(&mut writer, &next).await.unwrap();
        (frame, next)
    });
    let received = read_frame(&mut reader).await.unwrap();
    let following = read_frame(&mut reader).await.unwrap();
    let (original, next) = writing.await.unwrap();
    assert_eq!(received, original);
    assert_eq!(following, next);
}

#[tokio::test]
async fn truncated_large_payload_never_becomes_a_successful_message() {
    let message = serde_json::to_vec(&Message::Result {
        id: 1,
        result: json!({}),
    })
    .unwrap();
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&(message.len() as u32).to_be_bytes());
    bytes.extend_from_slice(&(101_u64 * 1024 * 1024).to_be_bytes());
    bytes.extend_from_slice(&message);
    bytes.extend_from_slice(&[1, 2, 3]);
    assert!(matches!(
        read_frame(&mut bytes.as_slice()).await,
        Err(FrameError::Io(_))
    ));
}

#[tokio::test]
async fn truncated_frame_never_becomes_a_successful_message() {
    let mut bytes = Vec::new();
    write_frame(&mut bytes, &Frame::control(Message::Cancel { id: 3 }))
        .await
        .unwrap();
    bytes.pop();
    assert!(matches!(
        read_frame(&mut bytes.as_slice()).await,
        Err(FrameError::Io(_))
    ));
}
