use shardmap::{SemanticCacheError, ShardCache};

struct User<'a> {
    tenant: &'a str,
    groups: &'a [&'a str],
}

fn can_use_cached_answer(user: &User<'_>, metadata: &[u8]) -> bool {
    let Ok(metadata) = std::str::from_utf8(metadata) else {
        return false;
    };

    let tenant_ok = metadata.contains(&format!("tenant={}", user.tenant));
    let group_ok = user
        .groups
        .iter()
        .any(|group| metadata.contains(&format!("group={group}")));
    tenant_ok && group_ok
}

fn main() -> Result<(), SemanticCacheError> {
    let cache = ShardCache::new();

    cache.insert_semantic_slice(
        b"prompt:shipping",
        b"Standard shipping takes three to five business days.",
        &[1.0, 0.0],
    )?;
    cache.insert_semantic_slice_with_governance(
        b"prompt:refunds",
        b"Refunds are available within 30 days.",
        &[0.0, 1.0],
        b"tenant=acme;group=support;doc=refund-policy;policy=7",
    )?;

    let shipping = cache.semantic_search(&[0.95, 0.05], 0.75)?.unwrap();
    assert_eq!(
        shipping.value.as_ref(),
        b"Standard shipping takes three to five business days."
    );
    assert!(shipping.governance.is_none());

    let support_user = User {
        tenant: "acme",
        groups: &["support"],
    };
    let refund = cache
        .semantic_search_with_governance_filter(&[0.05, 0.95], 0.75, |metadata| {
            metadata.is_some_and(|bytes| can_use_cached_answer(&support_user, bytes))
        })?
        .unwrap();
    assert_eq!(
        refund.value.as_ref(),
        b"Refunds are available within 30 days."
    );

    let sales_user = User {
        tenant: "acme",
        groups: &["sales"],
    };
    let blocked =
        cache.semantic_search_with_governance_filter(&[0.05, 0.95], 0.75, |metadata| {
            metadata.is_some_and(|bytes| can_use_cached_answer(&sales_user, bytes))
        })?;
    assert!(blocked.is_none());

    Ok(())
}
