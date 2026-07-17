use std::fmt;

pub type Result<T> = std::result::Result<T, SimEnvError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SimEnvError {
    InvalidRate {
        label: &'static str,
        value: u32,
        max: u32,
    },
    InvalidNode {
        node: usize,
        node_count: usize,
    },
    InvalidNodeCount,
    PlanNodeCountMismatch {
        plan_node_count: usize,
        cluster_node_count: usize,
    },
    TimeOverflow,
    App(String),
}

impl fmt::Display for SimEnvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRate { label, value, max } => {
                write!(f, "{label} must be <= {max}; got {value}")
            }
            Self::InvalidNode { node, node_count } => {
                write!(f, "node index {node} out of range for {node_count} nodes")
            }
            Self::InvalidNodeCount => write!(f, "hermetic cluster must contain at least one node"),
            Self::PlanNodeCountMismatch {
                plan_node_count,
                cluster_node_count,
            } => write!(
                f,
                "plan node count {plan_node_count} does not match cluster node count {cluster_node_count}"
            ),
            Self::TimeOverflow => write!(f, "simulated time overflow"),
            Self::App(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for SimEnvError {}
