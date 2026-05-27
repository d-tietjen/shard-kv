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

#[test]
fn decodes_resp3_maps_sets_and_scalars() {
    let frame = Frame::Map(vec![
        (Frame::BlobString(b"proto".to_vec()), Frame::Integer(3)),
        (
            Frame::BlobString(b"features".to_vec()),
            Frame::Set(vec![
                Frame::SimpleString("resp3".into()),
                Frame::Boolean(true),
            ]),
        ),
        (
            Frame::BlobString(b"ratio".to_vec()),
            Frame::Double("1.5".into()),
        ),
        (
            Frame::BlobString(b"doc".to_vec()),
            Frame::VerbatimString {
                format: "txt".into(),
                value: b"hello".to_vec(),
            },
        ),
    ]);

    let mut encoded = Vec::new();
    RespCodec::encode(&frame, &mut encoded);
    let decoded = RespCodec::decode(&encoded).unwrap().unwrap().0;

    assert_eq!(decoded, frame);
}

#[test]
fn decodes_resp3_attributes_pushes_and_big_numbers() {
    let frame = Frame::Attribute {
        attributes: vec![(
            Frame::BlobString(b"ttl".to_vec()),
            Frame::BigNumber("123456789012345678901234567890".into()),
        )],
        data: Box::new(Frame::Push(vec![
            Frame::SimpleString("invalidate".into()),
            Frame::BlobString(b"cache-key".to_vec()),
        ])),
    };

    let mut encoded = Vec::new();
    RespCodec::encode(&frame, &mut encoded);
    let decoded = RespCodec::decode(&encoded).unwrap().unwrap().0;

    assert_eq!(decoded, frame);
}

#[test]
fn decodes_resp3_blob_errors() {
    let decoded = RespCodec::decode(b"!21\r\nSYNTAX invalid syntax\r\n")
        .unwrap()
        .unwrap()
        .0;

    assert_eq!(decoded, Frame::Error("SYNTAX invalid syntax".into()));
}
