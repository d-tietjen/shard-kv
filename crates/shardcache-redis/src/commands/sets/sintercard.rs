use crate::commands::redis::{
    define_redis_command, eq_ignore_ascii_case, error, int, parse_usize, wrong_arity, wrongtype,
};
use crate::protocol::Frame;
use crate::storage::{EmbeddedStore, RedisObjectError};

define_redis_command!(SInterCard, "SINTERCARD", false);

impl crate::commands::redis::RedisCommand for SInterCard {
    fn execute(store: &EmbeddedStore, args: &[&[u8]]) -> Frame {
        let [numkeys, rest @ ..] = args else {
            return wrong_arity("SINTERCARD");
        };
        let Ok(numkeys) = parse_usize(numkeys) else {
            return error("ERR numkeys should be greater than 0");
        };
        if numkeys == 0 {
            return error("ERR numkeys should be greater than 0");
        }
        if rest.len() < numkeys {
            return error("ERR Number of keys can't be greater than number of args");
        }
        let (keys, options) = rest.split_at(numkeys);

        let mut limit = 0usize;
        let mut index = 0;
        while index < options.len() {
            match options[index] {
                option if eq_ignore_ascii_case(option, b"LIMIT") => {
                    let Some(raw) = options.get(index + 1) else {
                        return error("ERR syntax error");
                    };
                    let Ok(value) = parse_usize(raw) else {
                        return error("ERR LIMIT can't be negative");
                    };
                    limit = value;
                    index += 2;
                }
                _ => return error("ERR syntax error"),
            }
        }

        // Mirror compute_set_op's reads: set_members returns sorted Vec<Vec<u8>>;
        // a missing key is the empty set, a non-set value is WRONGTYPE.
        let mut sets: Vec<Vec<Vec<u8>>> = Vec::with_capacity(keys.len());
        for key in keys {
            match store.set_members(key) {
                Ok(members) => sets.push(members),
                Err(RedisObjectError::MissingKey) => return int(0),
                Err(RedisObjectError::WrongType) => return wrongtype(),
            }
        }

        let (first, others) = sets.split_first().expect("numkeys >= 1");
        let mut count = 0usize;
        for member in first {
            if others.iter().all(|set| set.binary_search(member).is_ok()) {
                count += 1;
                if limit != 0 && count >= limit {
                    break;
                }
            }
        }
        // set_members yields unique members, so `count` is the intersection
        // cardinality (already capped by LIMIT above).
        int(count as i64)
    }
}
