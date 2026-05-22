#[cfg(creusot)]
use creusot_std::macros::ensures;

/// Pure model of the transaction router policy used by the Redis-compatible
/// direct server.
#[cfg(any(test, creusot))]
#[derive(Debug, Clone, Copy)]
#[cfg_attr(not(creusot), derive(PartialEq, Eq))]
pub(crate) enum RedisTransactionPolicy {
    ShardLocal,
    CoordinatedCrossShard,
}

#[cfg(any(test, creusot))]
#[cfg_attr(
    creusot,
    ensures(result == match policy {
        RedisTransactionPolicy::ShardLocal => !dirty && affected_shards@ <= 1,
        RedisTransactionPolicy::CoordinatedCrossShard => !dirty,
    })
)]
pub(crate) fn transaction_exec_allowed(
    policy: RedisTransactionPolicy,
    dirty: bool,
    affected_shards: usize,
) -> bool {
    if dirty {
        return false;
    }
    match policy {
        RedisTransactionPolicy::ShardLocal => affected_shards <= 1,
        RedisTransactionPolicy::CoordinatedCrossShard => true,
    }
}

#[cfg(any(test, creusot))]
#[cfg_attr(creusot, ensures(result))]
pub(crate) fn dirty_transaction_never_executes(
    policy: RedisTransactionPolicy,
    affected_shards: usize,
) -> bool {
    !transaction_exec_allowed(policy, true, affected_shards)
}

#[cfg(any(test, creusot))]
#[cfg_attr(creusot, ensures(result))]
pub(crate) fn shard_local_transaction_is_single_shard(affected_shards: usize) -> bool {
    transaction_exec_allowed(RedisTransactionPolicy::ShardLocal, false, affected_shards)
        == (affected_shards <= 1)
}

#[cfg(any(test, creusot))]
#[cfg_attr(creusot, ensures(result))]
pub(crate) fn coordinated_transaction_allows_clean_cross_shard(affected_shards: usize) -> bool {
    transaction_exec_allowed(
        RedisTransactionPolicy::CoordinatedCrossShard,
        false,
        affected_shards,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transaction_policy_laws_hold_for_representative_inputs() {
        for affected_shards in 0..8 {
            assert!(dirty_transaction_never_executes(
                RedisTransactionPolicy::ShardLocal,
                affected_shards
            ));
            assert!(dirty_transaction_never_executes(
                RedisTransactionPolicy::CoordinatedCrossShard,
                affected_shards
            ));
            assert!(shard_local_transaction_is_single_shard(affected_shards));
            assert!(coordinated_transaction_allows_clean_cross_shard(
                affected_shards
            ));
        }
    }
}
