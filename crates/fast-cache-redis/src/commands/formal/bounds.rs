#[cfg(creusot)]
use creusot_std::macros::ensures;

#[derive(Debug, Clone, Copy)]
pub(crate) enum RedisRangeBound<T> {
    NegInfinity,
    PosInfinity,
    Finite { value: T, inclusive: bool },
}

#[cfg(not(creusot))]
impl<T: Copy + PartialOrd> RedisRangeBound<T> {
    #[inline(always)]
    pub(crate) fn contains(self, candidate: T, lower: bool) -> bool {
        match self {
            Self::NegInfinity => lower,
            Self::PosInfinity => !lower,
            Self::Finite { value, inclusive } => match (lower, inclusive) {
                (true, true) => candidate >= value,
                (true, false) => candidate > value,
                (false, true) => candidate <= value,
                (false, false) => candidate < value,
            },
        }
    }
}

#[cfg(creusot)]
impl RedisRangeBound<i64> {
    #[inline(always)]
    #[cfg_attr(
        creusot,
        ensures(result == match self {
            RedisRangeBound::NegInfinity => lower,
            RedisRangeBound::PosInfinity => !lower,
            RedisRangeBound::Finite { value, inclusive } => match (lower, inclusive) {
                (true, true) => candidate >= value,
                (true, false) => candidate > value,
                (false, true) => candidate <= value,
                (false, false) => candidate < value,
            },
        })
    )]
    pub(crate) fn contains(self, candidate: i64, lower: bool) -> bool {
        match self {
            Self::NegInfinity => lower,
            Self::PosInfinity => !lower,
            Self::Finite { value, inclusive } => match (lower, inclusive) {
                (true, true) => candidate >= value,
                (true, false) => candidate > value,
                (false, true) => candidate <= value,
                (false, false) => candidate < value,
            },
        }
    }
}

#[cfg(not(creusot))]
impl RedisRangeBound<f64> {
    #[inline(always)]
    pub(crate) fn score_parts(self) -> (f64, bool) {
        match self {
            Self::NegInfinity => (f64::NEG_INFINITY, true),
            Self::PosInfinity => (f64::INFINITY, true),
            Self::Finite { value, inclusive } => (value, inclusive),
        }
    }
}

#[cfg(any(test, creusot))]
#[cfg_attr(creusot, ensures(result))]
pub(crate) fn negative_infinity_bound_laws(candidate: i64) -> bool {
    RedisRangeBound::NegInfinity.contains(candidate, true)
        && !RedisRangeBound::NegInfinity.contains(candidate, false)
}

#[cfg(any(test, creusot))]
#[cfg_attr(creusot, ensures(result))]
pub(crate) fn positive_infinity_bound_laws(candidate: i64) -> bool {
    !RedisRangeBound::PosInfinity.contains(candidate, true)
        && RedisRangeBound::PosInfinity.contains(candidate, false)
}

#[cfg(any(test, creusot))]
#[cfg_attr(creusot, ensures(result))]
pub(crate) fn finite_inclusive_bound_laws(candidate: i64) -> bool {
    let bound = RedisRangeBound::Finite {
        value: candidate,
        inclusive: true,
    };
    bound.contains(candidate, true) && bound.contains(candidate, false)
}

#[cfg(any(test, creusot))]
#[cfg_attr(creusot, ensures(result))]
pub(crate) fn finite_exclusive_bound_laws(candidate: i64) -> bool {
    let bound = RedisRangeBound::Finite {
        value: candidate,
        inclusive: false,
    };
    !bound.contains(candidate, true) && !bound.contains(candidate, false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn range_bound_laws_hold_for_representative_inputs() {
        for candidate in -128..128 {
            assert!(negative_infinity_bound_laws(candidate));
            assert!(positive_infinity_bound_laws(candidate));
            assert!(finite_inclusive_bound_laws(candidate));
            assert!(finite_exclusive_bound_laws(candidate));
        }
    }
}
