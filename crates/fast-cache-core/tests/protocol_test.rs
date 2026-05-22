mod common;

use fast_cache::protocol::{Frame, RespCodec};

#[test]
fn decodes_pipelined_requests() {
    let mut bytes = Vec::new();
    RespCodec::encode(
        &Frame::Array(vec![
            Frame::BlobString(b"PING".to_vec()),
            Frame::BlobString(b"hello".to_vec()),
        ]),
        &mut bytes,
    );
    RespCodec::encode(
        &Frame::Array(vec![
            Frame::BlobString(b"SET".to_vec()),
            Frame::BlobString(b"alpha".to_vec()),
            Frame::BlobString(b"beta".to_vec()),
        ]),
        &mut bytes,
    );

    let (first, first_consumed) = RespCodec::decode(&bytes).unwrap().unwrap();
    match first {
        Frame::Array(parts) => assert_eq!(parts.len(), 2),
        other => panic!("unexpected frame: {other:?}"),
    }

    let (second, _) = RespCodec::decode(&bytes[first_consumed..])
        .unwrap()
        .unwrap();
    match second {
        Frame::Array(parts) => assert_eq!(parts.len(), 3),
        other => panic!("unexpected frame: {other:?}"),
    }
}

#[test]
fn encodes_null_and_integer_responses() {
    let frame = Frame::Array(vec![Frame::Null, Frame::Integer(7)]);
    let mut encoded = Vec::new();
    RespCodec::encode(&frame, &mut encoded);
    let decoded = RespCodec::decode(&encoded).unwrap().unwrap().0;
    assert_eq!(decoded, frame);
}
