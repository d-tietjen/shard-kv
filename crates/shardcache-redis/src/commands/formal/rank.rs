#[cfg(creusot)]
use creusot_std::macros::ensures;

#[inline(always)]
#[cfg(any(test, creusot))]
#[cfg_attr(
    creusot,
    ensures(match result {
        Some(reverse) => forward_rank@ < len@ && reverse@ == len@ - forward_rank@ - 1,
        None => forward_rank@ >= len@,
    })
)]
pub(crate) fn reverse_rank(len: usize, forward_rank: usize) -> Option<usize> {
    if forward_rank < len {
        Some(len - forward_rank - 1)
    } else {
        None
    }
}

#[cfg(any(test, creusot))]
#[cfg_attr(creusot, ensures(result))]
pub(crate) fn reverse_rank_is_involution(len: usize, rank: usize) -> bool {
    match reverse_rank(len, rank) {
        Some(reverse) => reverse_rank(len, reverse) == Some(rank),
        None => rank >= len,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reverse_rank_is_its_own_inverse() {
        for len in 0..64 {
            for rank in 0..80 {
                assert!(reverse_rank_is_involution(len, rank));
            }
        }
    }
}
