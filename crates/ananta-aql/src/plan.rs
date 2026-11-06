use crate::{Identifier, Projection, Query, Statement, Expr};

/// A small, stable logical representation of AQL v0.1 SELECT queries.
/// The node order is Scan -> optional Filter -> Project -> optional Limit.
#[derive(Debug, Clone, PartialEq)]
pub struct LogicalPlan {
    pub root: LogicalPlanNode,
}

#[derive(Debug, Clone, PartialEq)]
pub enum LogicalPlanNode {
    Scan { table: Identifier },
    Filter { predicate: Expr, input: Box<LogicalPlanNode> },
    Project { projection: Projection, input: Box<LogicalPlanNode> },
    Limit { rows: u64, input: Box<LogicalPlanNode> },
}

pub fn logical_plan(query: &Query) -> LogicalPlan {
    let Statement::Select(select) = &query.statement;
    let mut node = LogicalPlanNode::Scan { table: select.source.clone() };
    if let Some(predicate) = &select.selection {
        node = LogicalPlanNode::Filter {
            predicate: predicate.clone(),
            input: Box::new(node),
        };
    }
    node = LogicalPlanNode::Project {
        projection: select.projection.clone(),
        input: Box::new(node),
    };
    if let Some(rows) = select.limit {
        node = LogicalPlanNode::Limit { rows, input: Box::new(node) };
    }
    LogicalPlan { root: node }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plan_orders_scan_filter_project_and_limit() {
        let query = crate::parse(
            "SELECT device FROM telemetry WHERE reading >= 10 LIMIT 5"
        ).unwrap();
        let plan = logical_plan(&query);
        let LogicalPlanNode::Limit { rows: 5, input } = plan.root else {
            panic!("LIMIT must be the outermost logical operator");
        };
        let LogicalPlanNode::Project { input, .. } = *input else {
            panic!("projection must follow filtering");
        };
        let LogicalPlanNode::Filter { input, .. } = *input else {
            panic!("filter must follow scan");
        };
        assert!(matches!(*input, LogicalPlanNode::Scan { .. }));
    }

    #[test]
    fn plan_without_filter_or_limit_is_scan_then_project() {
        let query = crate::parse("SELECT * FROM telemetry").unwrap();
        let plan = logical_plan(&query);
        assert!(matches!(plan.root, LogicalPlanNode::Project { input, .. }
            if matches!(*input, LogicalPlanNode::Scan { .. })));
    }
}
