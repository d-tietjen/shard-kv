use super::*;

fn run(store: &EmbeddedStore, parts: &[&[u8]]) -> Frame {
    let raw = RespTestHarness::exec_resp_sequence_raw(store, &[parts], TransactionMode::Disabled);
    decode_resp_stream(&raw)
        .into_iter()
        .next()
        .expect("one frame")
}

fn bulk_string(value: &[u8]) -> Frame {
    Frame::BlobString(value.to_vec())
}

#[test]
fn probabilistic_module_commands_are_storage_backed() {
    let store = EmbeddedStore::new(8);

    assert_eq!(run(&store, &[b"BF.ADD", b"bf", b"a"]), Frame::Integer(1));
    assert_eq!(run(&store, &[b"BF.ADD", b"bf", b"a"]), Frame::Integer(0));
    assert_eq!(
        run(&store, &[b"BF.MEXISTS", b"bf", b"a", b"b"]),
        Frame::Array(vec![Frame::Integer(1), Frame::Integer(0)])
    );
    assert_eq!(run(&store, &[b"BF.CARD", b"bf"]), Frame::Integer(1));

    assert_eq!(run(&store, &[b"CF.ADDNX", b"cf", b"a"]), Frame::Integer(1));
    assert_eq!(run(&store, &[b"CF.ADDNX", b"cf", b"a"]), Frame::Integer(0));
    assert_eq!(run(&store, &[b"CF.COUNT", b"cf", b"a"]), Frame::Integer(1));
    assert_eq!(run(&store, &[b"CF.DEL", b"cf", b"a"]), Frame::Integer(1));
    assert_eq!(run(&store, &[b"CF.EXISTS", b"cf", b"a"]), Frame::Integer(0));

    assert_eq!(
        run(&store, &[b"CMS.INITBYDIM", b"cms", b"10", b"5"]),
        Frame::SimpleString("OK".into())
    );
    assert_eq!(
        run(&store, &[b"CMS.INCRBY", b"cms", b"a", b"2", b"b", b"3"]),
        Frame::Array(vec![Frame::Integer(2), Frame::Integer(3)])
    );
    assert_eq!(
        run(&store, &[b"CMS.QUERY", b"cms", b"a", b"b", b"c"]),
        Frame::Array(vec![
            Frame::Integer(2),
            Frame::Integer(3),
            Frame::Integer(0)
        ])
    );
}

#[test]
fn json_timeseries_tdigest_and_roaring_commands_mutate_state() {
    let store = EmbeddedStore::new(8);

    assert_eq!(
        run(
            &store,
            &[
                b"JSON.SET",
                b"doc",
                b"$",
                br#"{"a":1,"arr":[1],"n":2,"s":"x","flag":true}"#
            ]
        ),
        Frame::SimpleString("OK".into())
    );
    assert_eq!(
        run(&store, &[b"JSON.OBJLEN", b"doc", b"$"]),
        Frame::Integer(5)
    );
    assert_eq!(
        run(&store, &[b"JSON.ARRAPPEND", b"doc", b".arr", b"2"]),
        Frame::Integer(2)
    );
    assert_eq!(
        run(&store, &[b"JSON.ARRINDEX", b"doc", b".arr", b"2"]),
        Frame::Integer(1)
    );
    assert!(matches!(
        run(&store, &[b"JSON.NUMMULTBY", b"doc", b".n", b"3"]),
        Frame::BlobString(_)
    ));
    assert_eq!(
        run(&store, &[b"JSON.STRAPPEND", b"doc", b".s", br#""y""#]),
        Frame::Integer(2)
    );
    assert_eq!(
        run(&store, &[b"JSON.TOGGLE", b"doc", b".flag"]),
        Frame::Integer(0)
    );
    assert!(matches!(
        run(&store, &[b"JSON.DEBUG", b"MEMORY", b"doc", b"$"]),
        Frame::Integer(value) if value > 0
    ));

    assert_eq!(
        run(&store, &[b"TS.ADD", b"ts", b"1", b"1.5"]),
        Frame::Integer(1)
    );
    assert_eq!(
        run(&store, &[b"TS.ADD", b"ts", b"2", b"2.5"]),
        Frame::Integer(2)
    );
    assert!(matches!(
        run(&store, &[b"TS.RANGE", b"ts", b"-", b"+"]),
        Frame::Array(items) if items.len() == 2
    ));
    assert_eq!(
        run(&store, &[b"TS.DEL", b"ts", b"1", b"1"]),
        Frame::Integer(1)
    );
    assert!(matches!(
        run(&store, &[b"TS.GET", b"ts"]),
        Frame::Array(items) if items == vec![Frame::Integer(2), bulk_string(b"2.5")]
    ));

    assert_eq!(
        run(&store, &[b"TDIGEST.CREATE", b"td"]),
        Frame::SimpleString("OK".into())
    );
    assert_eq!(
        run(&store, &[b"TDIGEST.ADD", b"td", b"1", b"2", b"3"]),
        Frame::SimpleString("OK".into())
    );
    assert_eq!(run(&store, &[b"TDIGEST.MIN", b"td"]), bulk_string(b"1"));
    assert_eq!(run(&store, &[b"TDIGEST.MAX", b"td"]), bulk_string(b"3"));
    assert_eq!(
        run(&store, &[b"TDIGEST.RANK", b"td", b"2"]),
        Frame::Array(vec![Frame::Integer(1)])
    );

    assert_eq!(
        run(&store, &[b"R.SETBIT", b"rb", b"7", b"1"]),
        Frame::Integer(0)
    );
    assert_eq!(
        run(&store, &[b"R.SETBIT", b"rb", b"7", b"1"]),
        Frame::Integer(1)
    );
    assert_eq!(run(&store, &[b"R.BITPOS", b"rb", b"1"]), Frame::Integer(7));
    assert_eq!(run(&store, &[b"R.BITPOS", b"rb", b"0"]), Frame::Integer(0));
    assert_eq!(run(&store, &[b"R.BITCOUNT", b"rb"]), Frame::Integer(1));
    assert_eq!(run(&store, &[b"R.MIN", b"rb"]), Frame::Integer(7));
    assert_eq!(run(&store, &[b"R.MAX", b"rb"]), Frame::Integer(7));
}

#[test]
fn registry_style_module_commands_preserve_metadata() {
    let store = EmbeddedStore::new(8);

    assert_eq!(
        run(
            &store,
            &[b"FT.CREATE", b"idx", b"SCHEMA", b"title", b"TEXT"]
        ),
        Frame::SimpleString("OK".into())
    );
    assert!(matches!(
        run(&store, &[b"FT.INFO", b"idx"]),
        Frame::Array(items) if items.contains(&bulk_string(b"index_name"))
    ));

    assert!(matches!(
        run(&store, &[b"GRAPH.QUERY", b"graph", b"RETURN 1"]),
        Frame::Array(items) if items.len() == 3
    ));
    assert!(matches!(
        run(&store, &[b"GRAPH.LIST"]),
        Frame::Array(items) if items.contains(&bulk_string(b"graph"))
    ));

    assert_eq!(
        run(
            &store,
            &[b"AI.TENSORSET", b"tensor", b"FLOAT", b"1", b"VALUES", b"1"]
        ),
        Frame::SimpleString("OK".into())
    );
    assert!(matches!(
        run(&store, &[b"AI.TENSORGET", b"tensor"]),
        Frame::Array(items) if !items.is_empty()
    ));

    assert!(matches!(
        run(&store, &[b"RG.PYEXECUTE", b"GB().run()"]),
        Frame::BlobString(value) if value.starts_with(b"exec-")
    ));
    assert!(matches!(
        run(&store, &[b"RG.DUMPEXECUTIONS"]),
        Frame::Array(items) if !items.is_empty()
    ));

    assert_eq!(
        run(&store, &[b"SG.CREATE", b"gate"]),
        Frame::SimpleString("OK".into())
    );
    assert_eq!(run(&store, &[b"SG.VALIDATE", b"gate"]), Frame::Integer(1));
    assert_eq!(run(&store, &[b"SNOWFLAKE.NEXT"]), Frame::Integer(1));
    assert_eq!(run(&store, &[b"SNOWFLAKE.NEXT"]), Frame::Integer(2));
}
