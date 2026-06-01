#![allow(dead_code, unused_imports)]

use super::super::*;

#[cfg(feature = "redis-module-json")]
pub(crate) fn json_path_is_root(path: &[u8]) -> bool {
    matches!(path, b"$" | b".")
}

#[cfg(feature = "redis-module-json")]
pub(crate) fn json_field_name(path: &[u8]) -> Option<&str> {
    let raw = std::str::from_utf8(path).ok()?;
    raw.strip_prefix("$.")
        .or_else(|| raw.strip_prefix('.'))
        .filter(|field| !field.is_empty() && !field.contains('.'))
}

#[cfg(feature = "redis-module-json")]
pub(crate) fn json_path<'a>(
    value: &'a serde_json::Value,
    path: &[u8],
) -> Option<&'a serde_json::Value> {
    if json_path_is_root(path) {
        return Some(value);
    }
    let field = json_field_name(path)?;
    value.as_object()?.get(field)
}

#[cfg(feature = "redis-module-json")]
pub(crate) fn json_path_mut<'a>(
    value: &'a mut serde_json::Value,
    path: &[u8],
) -> Option<&'a mut serde_json::Value> {
    if json_path_is_root(path) {
        return Some(value);
    }
    let field = json_field_name(path)?;
    value.as_object_mut()?.get_mut(field)
}

#[cfg(feature = "redis-module-json")]
pub(crate) fn json_path_or_root_mut<'a>(
    value: &'a mut serde_json::Value,
    path: &[u8],
) -> &'a mut serde_json::Value {
    if json_path_is_root(path) {
        return value;
    }
    if json_path(value, path).is_some() {
        return json_path_mut(value, path).expect("path existence was checked");
    }
    value
}

#[cfg(feature = "redis-module-json")]
pub(crate) fn json_set_path(
    value: &mut serde_json::Value,
    path: &[u8],
    replacement: serde_json::Value,
) {
    if json_path_is_root(path) {
        *value = replacement;
        return;
    }
    if !value.is_object() {
        *value = serde_json::Value::Object(Default::default());
    }
    if let Some(field) = json_field_name(path) {
        value
            .as_object_mut()
            .expect("object was just created")
            .insert(field.to_string(), replacement);
    }
}

#[cfg(feature = "redis-module-json")]
pub(crate) fn json_remove_path(value: &mut serde_json::Value, path: &[u8]) -> bool {
    let Some(field) = json_field_name(path) else {
        return false;
    };
    value
        .as_object_mut()
        .is_some_and(|object| object.remove(field).is_some())
}

#[cfg(feature = "redis-module-json")]
pub(crate) fn json_bulk_result(value: serde_json::Value) -> RedisModuleApiResult {
    result_bulk_string(serde_json::to_string(&value).unwrap_or_else(|_| "null".to_string()))
}

#[cfg(feature = "redis-module-json")]
pub(crate) fn json_type_name(value: &serde_json::Value) -> &'static str {
    match value {
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(_) => "boolean",
        serde_json::Value::Number(_) => "number",
        serde_json::Value::String(_) => "string",
        serde_json::Value::Array(_) => "array",
        serde_json::Value::Object(_) => "object",
    }
}

#[cfg(feature = "redis-module-json")]
pub(crate) fn json_clear_value(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Array(values) => values.clear(),
        serde_json::Value::Object(values) => values.clear(),
        serde_json::Value::String(value) => value.clear(),
        serde_json::Value::Number(_) => *value = serde_json::Value::Number(0.into()),
        serde_json::Value::Bool(value) => *value = false,
        serde_json::Value::Null => {}
    }
}

#[cfg(feature = "redis-module-json")]
pub(crate) fn json_merge_value(target: &mut serde_json::Value, patch: serde_json::Value) {
    match (target, patch) {
        (serde_json::Value::Object(target), serde_json::Value::Object(patch)) => {
            for (key, value) in patch {
                if value.is_null() {
                    target.remove(&key);
                } else {
                    json_merge_value(target.entry(key).or_insert(serde_json::Value::Null), value);
                }
            }
        }
        (target, value) => *target = value,
    }
}
