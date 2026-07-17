//! Executable convergence model for consensus-ranked active-sync conflicts.

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Candidate {
    pub finalized_epoch: u64,
    pub canonical_dot: u64,
}

/// Join for ambiguous concurrent SETs. A stable candidate rank makes this a
/// commutative, associative, and idempotent maximum operation.
pub fn join_set(left: Candidate, right: Candidate) -> Candidate {
    left.max(right)
}

/// Concurrent logical removal remains remove-wins and never reaches the
/// external ordering plane.
pub fn join_remove_wins(current: Option<Candidate>, remove: bool) -> Option<Candidate> {
    if remove { None } else { current }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn permutations(values: [Candidate; 3]) -> [[Candidate; 3]; 6] {
        let [a, b, c] = values;
        [
            [a, b, c],
            [a, c, b],
            [b, a, c],
            [b, c, a],
            [c, a, b],
            [c, b, a],
        ]
    }

    #[test]
    fn three_way_delivery_order_and_duplicates_converge() {
        let candidates = [
            Candidate {
                finalized_epoch: 4,
                canonical_dot: 1,
            },
            Candidate {
                finalized_epoch: 7,
                canonical_dot: 2,
            },
            Candidate {
                finalized_epoch: 7,
                canonical_dot: 3,
            },
        ];
        let expected = candidates[2];

        for order in permutations(candidates) {
            let mut state = order[0];
            for candidate in [order[1], order[1], order[2], order[0]] {
                state = join_set(state, candidate);
            }
            assert_eq!(state, expected);
        }
    }

    #[test]
    fn join_laws_hold_for_representative_rank_space() {
        let candidates = (1..=3)
            .flat_map(|epoch| {
                (1..=3).map(move |dot| Candidate {
                    finalized_epoch: epoch,
                    canonical_dot: dot,
                })
            })
            .collect::<Vec<_>>();

        for &a in &candidates {
            assert_eq!(join_set(a, a), a);
            for &b in &candidates {
                assert_eq!(join_set(a, b), join_set(b, a));
                for &c in &candidates {
                    assert_eq!(join_set(join_set(a, b), c), join_set(a, join_set(b, c)));
                }
            }
        }
    }

    #[test]
    fn concurrent_remove_wins_regardless_of_set_rank() {
        let value = Candidate {
            finalized_epoch: u64::MAX,
            canonical_dot: u64::MAX,
        };
        assert_eq!(join_remove_wins(Some(value), true), None);
        assert_eq!(join_remove_wins(Some(value), false), Some(value));
    }
}
