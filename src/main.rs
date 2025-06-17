mod physical_plan;
mod data_type;
mod data_source;

enum BinaryOp{
    Eq,
    NotEq,
    Gt,
    And,
    Or,
    Add,
    Subtract,
}

enum AggregateFunction {
    Sum,
    Count,
    Avg,
    Min,
    Max,
}
// 表示一个"未来的"值
enum LogicalExpr{
    Column(String),  // 列引用
    LiteralString(String),
    LiteralFloat(f64),
    Binary{l: Box<LogicalExpr>, r: Box<LogicalExpr>, op: BinaryOp},
    Aggregate(AggregateFunction),
}
/// 逻辑计划树的节点, 概念上表示一张"未来的"表
pub enum LogicalPlan {
    /// 从数据源扫描数据
    Scan {
        path: String,
        schema: Schema, // 使用 Arc 来共享 Schema 的所有权
        projection: Option<Vec<String>>,
    },

    /// 过滤操作 (WHERE)
    Filter {
        input: Box<LogicalPlan>,
        predicate: LogicalExpr,
    },

    /// 投影操作 (SELECT)
    Projection {
        input: Box<LogicalPlan>,
        expr: Vec<LogicalExpr>,
        schema: Arc<Schema>, // 这个节点的输出 Schema
    },

    /// 聚合操作 (GROUP BY)
    Aggregate {
        input: Box<LogicalPlan>,
        group_expr: Vec<LogicalExpr>,
        aggr_expr: Vec<LogicalExpr>, // 这里应该是 AggregateExpr 类型
        schema: Arc<Schema>,
    },

    /// 连接操作 (JOIN)
    Join {
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
        on: Vec<(String, String)>, // 简化版的 on 条件
        join_type: JoinType,
        schema: Arc<Schema>,
    },
}

pub enum JoinType {
    Inner,
    Left,
    Right,
}
fn main() {
    println!("Hello, world!");
}
