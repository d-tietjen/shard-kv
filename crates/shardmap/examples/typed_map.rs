use shardmap::{ShardMap, ShardMapOptions};

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct UserKey {
    tenant: String,
    id: u64,
}

#[derive(Clone, Debug, PartialEq)]
struct User {
    name: String,
    active: bool,
}

fn main() {
    let users: ShardMap<String, String> = ShardMap::with_options(ShardMapOptions {
        capacity_hint: Some(128),
        default_ttl_ms: None,
    });

    users.insert("user:42".to_owned(), "ready".to_owned());
    assert_eq!(users.get("user:42").as_deref(), Some("ready"));

    {
        let value = users.get_ref("user:42").unwrap();
        assert_eq!(value.value(), "ready");
    }

    users.insert("user:42".to_owned(), "done".to_owned());
    assert_eq!(users.remove("user:42").as_deref(), Some("done"));

    let custom: ShardMap<UserKey, User> = ShardMap::new();
    let key = UserKey {
        tenant: "acme".to_owned(),
        id: 7,
    };
    custom.insert(
        key.clone(),
        User {
            name: "Devon".to_owned(),
            active: true,
        },
    );

    assert_eq!(
        custom.get(&key),
        Some(User {
            name: "Devon".to_owned(),
            active: true,
        })
    );
}
