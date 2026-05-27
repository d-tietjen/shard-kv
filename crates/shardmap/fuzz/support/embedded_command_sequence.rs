use std::collections::{BTreeMap, BTreeSet, VecDeque};

use shardmap::storage::{EmbeddedStore, RedisObjectResult, RedisStringLookup};

const MAX_STEPS: usize = 384;
const MAX_VALUE_BYTES: usize = 64;
const MAX_FIELD_BYTES: usize = 24;
const MAX_MULTI_VALUES: usize = 4;

#[derive(Debug, Clone, PartialEq, Eq)]
enum ModelValue {
    String(Vec<u8>),
    Hash(BTreeMap<Vec<u8>, Vec<u8>>),
    List(VecDeque<Vec<u8>>),
    Set(BTreeSet<Vec<u8>>),
    ZSet(BTreeMap<Vec<u8>, i64>),
}

pub fn run(data: &[u8]) {
    if data.is_empty() {
        return;
    }

    let mut input = Input::new(data);
    let shard_count = match input.byte() & 0b11 {
        0 => 1,
        1 => 4,
        2 => 8,
        _ => 16,
    };
    let store = EmbeddedStore::new(shard_count);
    let mut model = BTreeMap::<Vec<u8>, ModelValue>::new();
    let steps = 1 + (input.byte() as usize % MAX_STEPS);

    for step in 0..steps {
        match input.byte() % 31 {
            0 => op_set(&store, &mut model, &mut input),
            1 => op_get(&store, &model, &mut input),
            2 => op_delete(&store, &mut model, &mut input),
            3 => op_batch_set(&store, &mut model, &mut input),
            4 => op_batch_get(&store, &model, &mut input),
            5 => op_hset(&store, &mut model, &mut input),
            6 => op_hget(&store, &model, &mut input),
            7 => op_hdel(&store, &mut model, &mut input),
            8 => op_hlen(&store, &model, &mut input),
            9 => op_hmget(&store, &model, &mut input),
            10 => op_lpush(&store, &mut model, &mut input),
            11 => op_rpush(&store, &mut model, &mut input),
            12 => op_lpop(&store, &mut model, &mut input),
            13 => op_rpop(&store, &mut model, &mut input),
            14 => op_llen(&store, &model, &mut input),
            15 => op_lindex(&store, &model, &mut input),
            16 => op_lrange(&store, &model, &mut input),
            17 => op_sadd(&store, &mut model, &mut input),
            18 => op_srem(&store, &mut model, &mut input),
            19 => op_sismember(&store, &model, &mut input),
            20 => op_scard(&store, &model, &mut input),
            21 => op_smembers(&store, &model, &mut input),
            22 => op_zadd(&store, &mut model, &mut input),
            23 => op_zrem(&store, &mut model, &mut input),
            24 => op_zscore(&store, &model, &mut input),
            25 => op_zcard(&store, &model, &mut input),
            26 => op_zrange(&store, &model, &mut input),
            27 => op_zrank(&store, &model, &mut input, false),
            28 => op_zrank(&store, &model, &mut input, true),
            29 => op_zcount(&store, &model, &mut input),
            _ => op_exists_and_type_checks(&store, &model, &mut input),
        }

        if step % 16 == 0 {
            assert_key_index(&store, &model);
        }
    }

    assert_key_index(&store, &model);
    assert_all_values(&store, &model);
}

fn op_set(store: &EmbeddedStore, model: &mut BTreeMap<Vec<u8>, ModelValue>, input: &mut Input<'_>) {
    let key = input.key();
    let value = input.value();
    store.set(key.clone(), value.clone(), None);
    model.insert(key, ModelValue::String(value));
}

fn op_get(store: &EmbeddedStore, model: &BTreeMap<Vec<u8>, ModelValue>, input: &mut Input<'_>) {
    let key = input.key();
    assert_string_lookup(store, model, &key);
}

fn op_delete(
    store: &EmbeddedStore,
    model: &mut BTreeMap<Vec<u8>, ModelValue>,
    input: &mut Input<'_>,
) {
    let key = input.key();
    let expected = model.remove(&key).is_some();
    assert_eq!(store.delete(&key), expected);
}

fn op_batch_set(
    store: &EmbeddedStore,
    model: &mut BTreeMap<Vec<u8>, ModelValue>,
    input: &mut Input<'_>,
) {
    let count = 1 + (input.byte() as usize % MAX_MULTI_VALUES);
    let mut items = Vec::with_capacity(count);
    for _ in 0..count {
        let key = input.key();
        let value = input.value();
        model.insert(key.clone(), ModelValue::String(value.clone()));
        items.push((key, value));
    }
    store.batch_set(items, None);
}

fn op_batch_get(
    store: &EmbeddedStore,
    model: &BTreeMap<Vec<u8>, ModelValue>,
    input: &mut Input<'_>,
) {
    let count = 1 + (input.byte() as usize % MAX_MULTI_VALUES);
    let keys = (0..count).map(|_| input.key()).collect::<Vec<_>>();
    let expected = keys
        .iter()
        .map(|key| match model.get(key) {
            Some(ModelValue::String(value)) => Some(value.clone()),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(store.batch_get(keys), expected);
}

fn op_hset(
    store: &EmbeddedStore,
    model: &mut BTreeMap<Vec<u8>, ModelValue>,
    input: &mut Input<'_>,
) {
    let key = input.key();
    let field = input.field();
    let value = input.value();
    let result = store.hset(&key, &field, &value);
    match model.get_mut(&key) {
        Some(ModelValue::Hash(hash)) => {
            let inserted = hash.insert(field, value).is_none();
            expect_integer(result, inserted as i64);
        }
        Some(_) => expect_wrong_type(result),
        None => {
            let mut hash = BTreeMap::new();
            hash.insert(field, value);
            model.insert(key, ModelValue::Hash(hash));
            expect_integer(result, 1);
        }
    }
}

fn op_hget(store: &EmbeddedStore, model: &BTreeMap<Vec<u8>, ModelValue>, input: &mut Input<'_>) {
    let key = input.key();
    let field = input.field();
    match model.get(&key) {
        Some(ModelValue::Hash(hash)) => {
            expect_bulk(store.hget(&key, &field), hash.get(&field).cloned());
        }
        Some(_) => expect_wrong_type(store.hget(&key, &field)),
        None => expect_bulk(store.hget(&key, &field), None),
    }
}

fn op_hdel(
    store: &EmbeddedStore,
    model: &mut BTreeMap<Vec<u8>, ModelValue>,
    input: &mut Input<'_>,
) {
    let key = input.key();
    let field = input.field();
    let result = store.hdel(&key, &field);
    match model.get_mut(&key) {
        Some(ModelValue::Hash(hash)) => {
            let removed = hash.remove(&field).is_some();
            let empty = hash.is_empty();
            expect_integer(result, removed as i64);
            if empty {
                model.remove(&key);
            }
        }
        Some(_) => expect_wrong_type(result),
        None => expect_integer(result, 0),
    }
}

fn op_hlen(store: &EmbeddedStore, model: &BTreeMap<Vec<u8>, ModelValue>, input: &mut Input<'_>) {
    let key = input.key();
    match model.get(&key) {
        Some(ModelValue::Hash(hash)) => expect_integer(store.hlen(&key), hash.len() as i64),
        Some(_) => expect_wrong_type(store.hlen(&key)),
        None => expect_integer(store.hlen(&key), 0),
    }
}

fn op_hmget(store: &EmbeddedStore, model: &BTreeMap<Vec<u8>, ModelValue>, input: &mut Input<'_>) {
    let key = input.key();
    let fields = input.fields();
    let field_refs = fields.iter().map(Vec::as_slice).collect::<Vec<_>>();
    match model.get(&key) {
        Some(ModelValue::Hash(hash)) => {
            let expected = fields
                .iter()
                .map(|field| hash.get(field).cloned())
                .collect::<Vec<_>>();
            expect_array(store.hmget(&key, &field_refs), expected);
        }
        Some(_) => expect_wrong_type(store.hmget(&key, &field_refs)),
        None => expect_array(store.hmget(&key, &field_refs), vec![None; fields.len()]),
    }
}

fn op_lpush(
    store: &EmbeddedStore,
    model: &mut BTreeMap<Vec<u8>, ModelValue>,
    input: &mut Input<'_>,
) {
    push_list(store, model, input, true);
}

fn op_rpush(
    store: &EmbeddedStore,
    model: &mut BTreeMap<Vec<u8>, ModelValue>,
    input: &mut Input<'_>,
) {
    push_list(store, model, input, false);
}

fn push_list(
    store: &EmbeddedStore,
    model: &mut BTreeMap<Vec<u8>, ModelValue>,
    input: &mut Input<'_>,
    front: bool,
) {
    let key = input.key();
    let values = input.values();
    let value_refs = values.iter().map(Vec::as_slice).collect::<Vec<_>>();
    let result = match front {
        true => store.lpush(&key, &value_refs),
        false => store.rpush(&key, &value_refs),
    };
    match model.get_mut(&key) {
        Some(ModelValue::List(list)) => {
            push_model_list(list, values, front);
            expect_integer(result, list.len() as i64);
        }
        Some(_) => expect_wrong_type(result),
        None => {
            let mut list = VecDeque::new();
            push_model_list(&mut list, values, front);
            let len = list.len() as i64;
            model.insert(key, ModelValue::List(list));
            expect_integer(result, len);
        }
    }
}

fn op_lpop(
    store: &EmbeddedStore,
    model: &mut BTreeMap<Vec<u8>, ModelValue>,
    input: &mut Input<'_>,
) {
    pop_list(store, model, input, true);
}

fn op_rpop(
    store: &EmbeddedStore,
    model: &mut BTreeMap<Vec<u8>, ModelValue>,
    input: &mut Input<'_>,
) {
    pop_list(store, model, input, false);
}

fn pop_list(
    store: &EmbeddedStore,
    model: &mut BTreeMap<Vec<u8>, ModelValue>,
    input: &mut Input<'_>,
    front: bool,
) {
    let key = input.key();
    let result = match front {
        true => store.lpop(&key),
        false => store.rpop(&key),
    };
    match model.get_mut(&key) {
        Some(ModelValue::List(list)) => {
            let expected = match front {
                true => list.pop_front(),
                false => list.pop_back(),
            };
            let empty = list.is_empty();
            expect_bulk(result, expected);
            if empty {
                model.remove(&key);
            }
        }
        Some(_) => expect_wrong_type(result),
        None => expect_bulk(result, None),
    }
}

fn op_llen(store: &EmbeddedStore, model: &BTreeMap<Vec<u8>, ModelValue>, input: &mut Input<'_>) {
    let key = input.key();
    match model.get(&key) {
        Some(ModelValue::List(list)) => expect_integer(store.llen(&key), list.len() as i64),
        Some(_) => expect_wrong_type(store.llen(&key)),
        None => expect_integer(store.llen(&key), 0),
    }
}

fn op_lindex(store: &EmbeddedStore, model: &BTreeMap<Vec<u8>, ModelValue>, input: &mut Input<'_>) {
    let key = input.key();
    let index = input.index();
    match model.get(&key) {
        Some(ModelValue::List(list)) => {
            let expected =
                normalize_index(index, list.len()).and_then(|idx| list.get(idx).cloned());
            expect_bulk(store.lindex(&key, index), expected);
        }
        Some(_) => expect_wrong_type(store.lindex(&key, index)),
        None => expect_bulk(store.lindex(&key, index), None),
    }
}

fn op_lrange(store: &EmbeddedStore, model: &BTreeMap<Vec<u8>, ModelValue>, input: &mut Input<'_>) {
    let key = input.key();
    let (start, stop) = input.range();
    match model.get(&key) {
        Some(ModelValue::List(list)) => {
            let expected = list_range(list, start, stop);
            expect_array(store.lrange(&key, start, stop), expected);
        }
        Some(_) => expect_wrong_type(store.lrange(&key, start, stop)),
        None => expect_array(store.lrange(&key, start, stop), Vec::new()),
    }
}

fn op_sadd(
    store: &EmbeddedStore,
    model: &mut BTreeMap<Vec<u8>, ModelValue>,
    input: &mut Input<'_>,
) {
    let key = input.key();
    let members = input.values();
    let member_refs = members.iter().map(Vec::as_slice).collect::<Vec<_>>();
    let result = store.sadd(&key, &member_refs);
    match model.get_mut(&key) {
        Some(ModelValue::Set(set)) => {
            let inserted = members
                .into_iter()
                .filter(|member| set.insert(member.clone()))
                .count();
            expect_integer(result, inserted as i64);
        }
        Some(_) => expect_wrong_type(result),
        None => {
            let mut set = BTreeSet::new();
            let inserted = members
                .into_iter()
                .filter(|member| set.insert(member.clone()))
                .count();
            model.insert(key, ModelValue::Set(set));
            expect_integer(result, inserted as i64);
        }
    }
}

fn op_srem(
    store: &EmbeddedStore,
    model: &mut BTreeMap<Vec<u8>, ModelValue>,
    input: &mut Input<'_>,
) {
    let key = input.key();
    let members = input.values();
    let member_refs = members.iter().map(Vec::as_slice).collect::<Vec<_>>();
    let result = store.srem(&key, &member_refs);
    match model.get_mut(&key) {
        Some(ModelValue::Set(set)) => {
            let removed = members.iter().filter(|member| set.remove(*member)).count();
            let empty = set.is_empty();
            expect_integer(result, removed as i64);
            if empty {
                model.remove(&key);
            }
        }
        Some(_) => expect_wrong_type(result),
        None => expect_integer(result, 0),
    }
}

fn op_sismember(
    store: &EmbeddedStore,
    model: &BTreeMap<Vec<u8>, ModelValue>,
    input: &mut Input<'_>,
) {
    let key = input.key();
    let member = input.field();
    match model.get(&key) {
        Some(ModelValue::Set(set)) => {
            expect_integer(store.sismember(&key, &member), set.contains(&member) as i64);
        }
        Some(_) => expect_wrong_type(store.sismember(&key, &member)),
        None => expect_integer(store.sismember(&key, &member), 0),
    }
}

fn op_scard(store: &EmbeddedStore, model: &BTreeMap<Vec<u8>, ModelValue>, input: &mut Input<'_>) {
    let key = input.key();
    match model.get(&key) {
        Some(ModelValue::Set(set)) => expect_integer(store.scard(&key), set.len() as i64),
        Some(_) => expect_wrong_type(store.scard(&key)),
        None => expect_integer(store.scard(&key), 0),
    }
}

fn op_smembers(
    store: &EmbeddedStore,
    model: &BTreeMap<Vec<u8>, ModelValue>,
    input: &mut Input<'_>,
) {
    let key = input.key();
    match model.get(&key) {
        Some(ModelValue::Set(set)) => {
            let expected = set.iter().cloned().map(Some).collect::<Vec<_>>();
            expect_array(store.smembers(&key), expected);
        }
        Some(_) => expect_wrong_type(store.smembers(&key)),
        None => expect_array(store.smembers(&key), Vec::new()),
    }
}

fn op_zadd(
    store: &EmbeddedStore,
    model: &mut BTreeMap<Vec<u8>, ModelValue>,
    input: &mut Input<'_>,
) {
    let key = input.key();
    let member = input.field();
    let score = input.score();
    let result = store.zadd(&key, score as f64, &member);
    match model.get_mut(&key) {
        Some(ModelValue::ZSet(zset)) => {
            let inserted = zset.insert(member, score).is_none();
            expect_integer(result, inserted as i64);
        }
        Some(_) => expect_wrong_type(result),
        None => {
            let mut zset = BTreeMap::new();
            zset.insert(member, score);
            model.insert(key, ModelValue::ZSet(zset));
            expect_integer(result, 1);
        }
    }
}

fn op_zrem(
    store: &EmbeddedStore,
    model: &mut BTreeMap<Vec<u8>, ModelValue>,
    input: &mut Input<'_>,
) {
    let key = input.key();
    let member = input.field();
    let result = store.zrem(&key, &member);
    match model.get_mut(&key) {
        Some(ModelValue::ZSet(zset)) => {
            let removed = zset.remove(&member).is_some();
            let empty = zset.is_empty();
            expect_integer(result, removed as i64);
            if empty {
                model.remove(&key);
            }
        }
        Some(_) => expect_wrong_type(result),
        None => expect_integer(result, 0),
    }
}

fn op_zscore(store: &EmbeddedStore, model: &BTreeMap<Vec<u8>, ModelValue>, input: &mut Input<'_>) {
    let key = input.key();
    let member = input.field();
    match model.get(&key) {
        Some(ModelValue::ZSet(zset)) => {
            expect_bulk(
                store.zscore(&key, &member),
                zset.get(&member).map(format_score),
            );
        }
        Some(_) => expect_wrong_type(store.zscore(&key, &member)),
        None => expect_bulk(store.zscore(&key, &member), None),
    }
}

fn op_zcard(store: &EmbeddedStore, model: &BTreeMap<Vec<u8>, ModelValue>, input: &mut Input<'_>) {
    let key = input.key();
    match model.get(&key) {
        Some(ModelValue::ZSet(zset)) => expect_integer(store.zcard(&key), zset.len() as i64),
        Some(_) => expect_wrong_type(store.zcard(&key)),
        None => expect_integer(store.zcard(&key), 0),
    }
}

fn op_zrange(store: &EmbeddedStore, model: &BTreeMap<Vec<u8>, ModelValue>, input: &mut Input<'_>) {
    let key = input.key();
    let (start, stop) = input.range();
    match model.get(&key) {
        Some(ModelValue::ZSet(zset)) => {
            expect_array(
                store.zrange(&key, start, stop),
                zset_range(zset, start, stop),
            );
        }
        Some(_) => expect_wrong_type(store.zrange(&key, start, stop)),
        None => expect_array(store.zrange(&key, start, stop), Vec::new()),
    }
}

fn op_zrank(
    store: &EmbeddedStore,
    model: &BTreeMap<Vec<u8>, ModelValue>,
    input: &mut Input<'_>,
    rev: bool,
) {
    let key = input.key();
    let member = input.field();
    match model.get(&key) {
        Some(ModelValue::ZSet(zset)) => {
            expect_integer(
                store.zrank(&key, &member, rev),
                zset_rank(zset, &member, rev),
            );
        }
        Some(_) => expect_wrong_type(store.zrank(&key, &member, rev)),
        None => expect_integer(store.zrank(&key, &member, rev), -1),
    }
}

fn op_zcount(store: &EmbeddedStore, model: &BTreeMap<Vec<u8>, ModelValue>, input: &mut Input<'_>) {
    let key = input.key();
    let left = input.score();
    let right = input.score();
    let (min, max) = match left <= right {
        true => (left, right),
        false => (right, left),
    };
    match model.get(&key) {
        Some(ModelValue::ZSet(zset)) => {
            expect_integer(
                store.zcount(&key, min as f64, max as f64),
                zset_count(zset, min, max),
            );
        }
        Some(_) => expect_wrong_type(store.zcount(&key, min as f64, max as f64)),
        None => expect_integer(store.zcount(&key, min as f64, max as f64), 0),
    }
}

fn op_exists_and_type_checks(
    store: &EmbeddedStore,
    model: &BTreeMap<Vec<u8>, ModelValue>,
    input: &mut Input<'_>,
) {
    let key = input.key();
    assert_eq!(store.exists(&key), model.contains_key(&key));
    assert_string_lookup(store, model, &key);
}

fn assert_string_lookup(store: &EmbeddedStore, model: &BTreeMap<Vec<u8>, ModelValue>, key: &[u8]) {
    let mut observed = None;
    let lookup = store.get_string_value_into(key, |value| observed = Some(value.to_vec()));
    match model.get(key) {
        Some(ModelValue::String(expected)) => {
            assert_eq!(lookup, RedisStringLookup::Hit);
            assert_eq!(observed.as_deref(), Some(expected.as_slice()));
        }
        Some(_) => {
            assert_eq!(lookup, RedisStringLookup::WrongType);
            assert!(observed.is_none());
        }
        None => {
            assert_eq!(lookup, RedisStringLookup::Miss);
            assert!(observed.is_none());
        }
    }
}

fn assert_key_index(store: &EmbeddedStore, model: &BTreeMap<Vec<u8>, ModelValue>) {
    let actual = store.key_snapshot();
    let expected = model.keys().cloned().collect::<Vec<_>>();
    assert_eq!(actual, expected);
    assert_eq!(store.len(), expected.len());
    for key in expected {
        assert!(store.exists(&key));
    }
}

fn assert_all_values(store: &EmbeddedStore, model: &BTreeMap<Vec<u8>, ModelValue>) {
    for (key, value) in model {
        assert_string_lookup(store, model, key);
        match value {
            ModelValue::String(_) => {
                expect_wrong_type(store.hlen(key));
                expect_wrong_type(store.llen(key));
                expect_wrong_type(store.scard(key));
                expect_wrong_type(store.zcard(key));
            }
            ModelValue::Hash(hash) => {
                expect_integer(store.hlen(key), hash.len() as i64);
                for (field, value) in hash {
                    expect_bulk(store.hget(key, field), Some(value.clone()));
                }
            }
            ModelValue::List(list) => {
                expect_integer(store.llen(key), list.len() as i64);
                expect_array(
                    store.lrange(key, 0, -1),
                    list.iter().cloned().map(Some).collect(),
                );
            }
            ModelValue::Set(set) => {
                expect_integer(store.scard(key), set.len() as i64);
                expect_array(store.smembers(key), set.iter().cloned().map(Some).collect());
            }
            ModelValue::ZSet(zset) => {
                expect_integer(store.zcard(key), zset.len() as i64);
                expect_array(store.zrange(key, 0, -1), zset_range(zset, 0, -1));
                let entries = zset_ordered_entries(zset);
                for (index, (member, score)) in entries.iter().enumerate() {
                    expect_bulk(store.zscore(key, member), Some(format_score(score)));
                    expect_integer(store.zrank(key, member, false), index as i64);
                    expect_integer(
                        store.zrank(key, member, true),
                        (entries.len() - index - 1) as i64,
                    );
                    expect_integer(
                        store.zcount(key, *score as f64, *score as f64),
                        entries
                            .iter()
                            .filter(|(_, candidate_score)| candidate_score == score)
                            .count() as i64,
                    );
                }
            }
        }
    }
}

fn push_model_list(list: &mut VecDeque<Vec<u8>>, values: Vec<Vec<u8>>, front: bool) {
    for value in values {
        match front {
            true => list.push_front(value),
            false => list.push_back(value),
        }
    }
}

fn list_range(list: &VecDeque<Vec<u8>>, start: i64, stop: i64) -> Vec<Option<Vec<u8>>> {
    let Some((start, stop)) = normalize_range(start, stop, list.len()) else {
        return Vec::new();
    };
    list.iter()
        .skip(start)
        .take(stop - start + 1)
        .cloned()
        .map(Some)
        .collect()
}

fn zset_range(zset: &BTreeMap<Vec<u8>, i64>, start: i64, stop: i64) -> Vec<Option<Vec<u8>>> {
    let entries = zset_ordered_entries(zset);
    let Some((start, stop)) = normalize_range(start, stop, entries.len()) else {
        return Vec::new();
    };
    entries
        .into_iter()
        .skip(start)
        .take(stop - start + 1)
        .map(|(member, _)| Some(member))
        .collect()
}

fn zset_rank(zset: &BTreeMap<Vec<u8>, i64>, member: &[u8], rev: bool) -> i64 {
    let entries = zset_ordered_entries(zset);
    let rank = match rev {
        true => entries
            .iter()
            .rev()
            .position(|(existing, _)| existing.as_slice() == member),
        false => entries
            .iter()
            .position(|(existing, _)| existing.as_slice() == member),
    };
    rank.map(|rank| rank as i64).unwrap_or(-1)
}

fn zset_count(zset: &BTreeMap<Vec<u8>, i64>, min: i64, max: i64) -> i64 {
    zset.values()
        .filter(|score| (min..=max).contains(score))
        .count() as i64
}

fn zset_ordered_entries(zset: &BTreeMap<Vec<u8>, i64>) -> Vec<(Vec<u8>, i64)> {
    let mut entries = zset
        .iter()
        .map(|(member, score)| (member.clone(), *score))
        .collect::<Vec<_>>();
    entries.sort_by(|(left_member, left_score), (right_member, right_score)| {
        left_score
            .cmp(right_score)
            .then_with(|| left_member.cmp(right_member))
    });
    entries
}

fn normalize_index(index: i64, len: usize) -> Option<usize> {
    if len == 0 {
        return None;
    }
    let len = len as i64;
    let index = match index < 0 {
        true => len + index,
        false => index,
    };
    (0..len).contains(&index).then_some(index as usize)
}

fn normalize_range(start: i64, stop: i64, len: usize) -> Option<(usize, usize)> {
    if len == 0 {
        return None;
    }
    let len = len as i64;
    let mut start = match start < 0 {
        true => len + start,
        false => start,
    };
    let mut stop = match stop < 0 {
        true => len + stop,
        false => stop,
    };
    if start < 0 {
        start = 0;
    }
    if stop < 0 || start >= len {
        return None;
    }
    if stop >= len {
        stop = len - 1;
    }
    (start <= stop).then_some((start as usize, stop as usize))
}

fn format_score(score: &i64) -> Vec<u8> {
    score.to_string().into_bytes()
}

fn expect_integer(result: RedisObjectResult, expected: i64) {
    assert_eq!(result, RedisObjectResult::Integer(expected));
}

fn expect_bulk(result: RedisObjectResult, expected: Option<Vec<u8>>) {
    assert_eq!(result, RedisObjectResult::Bulk(expected));
}

fn expect_array(result: RedisObjectResult, expected: Vec<Option<Vec<u8>>>) {
    assert_eq!(result, RedisObjectResult::Array(expected));
}

fn expect_wrong_type(result: RedisObjectResult) {
    assert_eq!(result, RedisObjectResult::WrongType);
}

struct Input<'a> {
    data: &'a [u8],
    offset: usize,
}

impl<'a> Input<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, offset: 0 }
    }

    fn byte(&mut self) -> u8 {
        let byte = self.data[self.offset % self.data.len()];
        self.offset = self.offset.wrapping_add(1);
        byte
    }

    fn key(&mut self) -> Vec<u8> {
        let mut key = vec![self.byte() % 64];
        if self.byte() & 0b11 == 0 {
            key.push(self.byte() % 16);
        }
        key
    }

    fn field(&mut self) -> Vec<u8> {
        self.bytes(MAX_FIELD_BYTES)
    }

    fn value(&mut self) -> Vec<u8> {
        self.bytes(MAX_VALUE_BYTES)
    }

    fn values(&mut self) -> Vec<Vec<u8>> {
        let count = 1 + (self.byte() as usize % MAX_MULTI_VALUES);
        (0..count).map(|_| self.value()).collect()
    }

    fn fields(&mut self) -> Vec<Vec<u8>> {
        let count = 1 + (self.byte() as usize % MAX_MULTI_VALUES);
        (0..count).map(|_| self.field()).collect()
    }

    fn bytes(&mut self, max_len: usize) -> Vec<u8> {
        let len = self.byte() as usize % (max_len + 1);
        (0..len).map(|_| self.byte()).collect()
    }

    fn score(&mut self) -> i64 {
        self.byte() as i8 as i64
    }

    fn index(&mut self) -> i64 {
        (self.byte() as i64 % 33) - 16
    }

    fn range(&mut self) -> (i64, i64) {
        let start = self.index();
        let width = self.byte() as i64 % 24;
        let stop = match self.byte() & 1 {
            0 => start + width,
            _ => start - width,
        };
        (start, stop)
    }
}
