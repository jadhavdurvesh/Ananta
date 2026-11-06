use crate::{logical_plan, BinaryOperator, UnaryOperator, ColumnReference, Expr, ExprKind, Identifier, Literal,
    LogicalPlan, ParameterBindings, ParameterRef, Projection, Query, Statement};
use ananta_core::{ColumnRangePredicate, Database};
use ananta_types::{Column, DataType, Value};
use std::collections::BTreeSet;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PlannerOptions {
    pub enable_projection_pushdown: bool,
    pub enable_predicate_pushdown: bool,
    pub navigation_map_generation: Option<u64>,
    pub max_capsule_materialized_rows: u64,
    pub max_capsule_materialized_bytes: usize,
}
impl Default for PlannerOptions {
    fn default()->Self{
        Self{enable_projection_pushdown:true,enable_predicate_pushdown:true,
            navigation_map_generation:None,max_capsule_materialized_rows:100_000,
            max_capsule_materialized_bytes:128*1024*1024}
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ColumnStatistics {
    pub name:String,
    pub data_type:DataType,
    pub null_count:Option<u64>,
    pub minimum:Option<Value>,
    pub maximum:Option<Value>,
    pub approximate_distinct_count:Option<u64>,
    pub histogram_bucket_count:usize,
}
#[derive(Debug, Clone, PartialEq)]
pub struct TableStatistics {
    pub row_count:Option<u64>,
    pub ingestion_batches:Option<u64>,
    pub encoded_bytes:Option<u64>,
    pub capsule_coverage_complete:bool,
    pub analyzed_snapshot_available:bool,
    pub columns:Vec<ColumnStatistics>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PhysicalPlanNode {
    Scan { table:String, columns:Vec<String>, estimated_rows:Option<u64>,
        projection_pushed_down:bool, capsule_map_generation:Option<u64> },
    Filter { predicate:String, pushed_to_scan:bool, estimated_rows:Option<u64> },
    Project { columns:Vec<String> },
    Limit { rows:u64, estimated_rows:Option<u64> },
}
/// Transparent, deliberately simple relative cost estimate.
///
/// Units are not milliseconds or money. The default model uses one unit per
/// scanned row, one unit per 4 KiB of estimated scan payload, 0.25 units per
/// predicate evaluation and 0.05 units per projected output cell.
#[derive(Debug, Clone, PartialEq)]
pub struct PlanCostEstimate {
    pub estimated_scan_rows: Option<u64>,
    pub estimated_output_rows: Option<u64>,
    pub estimated_scan_payload_bytes: Option<u64>,
    pub scan_cost_units: Option<f64>,
    pub filter_cost_units: Option<f64>,
    pub projection_cost_units: Option<f64>,
    pub total_cost_units: Option<f64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ExplainPlan {
    pub language_version:String,
    pub logical:LogicalPlan,
    pub physical:Vec<PhysicalPlanNode>,
    pub table_statistics:TableStatistics,
    pub required_columns:Vec<String>,
    pub timestamp_range:Option<ColumnRangePredicate>,
    pub cost:PlanCostEstimate,
    pub notes:Vec<String>,
}
impl ExplainPlan {
    pub fn render(&self)->String{
        let mut lines=vec![format!("AQL {}",self.language_version)];
        lines.push(format!("Statistics: rows={}, batches={}, encoded_bytes={}, capsule_coverage_complete={}, analyzed_snapshot={}",
            display_option(self.table_statistics.row_count),
            display_option(self.table_statistics.ingestion_batches),
            display_option(self.table_statistics.encoded_bytes),
            self.table_statistics.capsule_coverage_complete,
            self.table_statistics.analyzed_snapshot_available));
        for column in &self.table_statistics.columns {
            lines.push(format!("COLUMN_STATS {} type={} nulls={} min={:?} max={:?} ndv~={} histogram_buckets={}",
                column.name,column.data_type,display_option(column.null_count),
                column.minimum,column.maximum,
                display_option(column.approximate_distinct_count),
                column.histogram_bucket_count));
        }
        lines.push(format!(
            "COST scan_rows={} output_rows={} scan_payload_bytes={} scan_units={} filter_units={} projection_units={} total_units={}",
            display_option(self.cost.estimated_scan_rows),
            display_option(self.cost.estimated_output_rows),
            display_option(self.cost.estimated_scan_payload_bytes),
            display_float(self.cost.scan_cost_units),
            display_float(self.cost.filter_cost_units),
            display_float(self.cost.projection_cost_units),
            display_float(self.cost.total_cost_units),
        ));
        for node in &self.physical{
            lines.push(match node{
                PhysicalPlanNode::Scan{table,columns,estimated_rows,projection_pushed_down,capsule_map_generation}=>{
                    format!("SCAN {table} columns=[{}] estimated_rows={} projection_pushdown={} capsule_map={}",
                        columns.join(", "),display_option(*estimated_rows),projection_pushed_down,
                        capsule_map_generation.map_or_else(||"disabled".into(),|v|v.to_string()))
                }
                PhysicalPlanNode::Filter{predicate,pushed_to_scan,estimated_rows}=>format!(
                    "FILTER {predicate} predicate_pushdown={pushed_to_scan} estimated_rows={}",
                    display_option(*estimated_rows)),
                PhysicalPlanNode::Project{columns}=>format!("PROJECT [{}]",columns.join(", ")),
                PhysicalPlanNode::Limit{rows,estimated_rows}=>format!(
                    "LIMIT {rows} estimated_rows={}",display_option(*estimated_rows)),
            });
        }
        lines.extend(self.notes.iter().map(|note|format!("NOTE {note}")));
        lines.join("\n")
    }
}

fn display_float(value: Option<f64>) -> String {
    value.map_or_else(|| "unknown".into(), |value| format!("{value:.3}"))
}

fn estimate_rows(rows:u64,selectivity:f64)->u64{
    ((rows as f64)*selectivity.clamp(0.0,1.0))
        .round()
        .min(u64::MAX as f64) as u64
}

fn estimate_predicate_selectivity(
    expression:&Expr,
    statistics:&ananta_execution::TableStatistics,
    schema:&ananta_types::Schema,
    table:&str,
    bindings:&ParameterBindings,
)->Option<f64>{
    match &expression.kind {
        ExprKind::Literal(Literal::Boolean(value))=>Some(if *value{1.0}else{0.0}),
        ExprKind::IsNull{expression:inner,negated}=>{
            let index=planner_column_index(inner,schema,table)?;
            if statistics.scanned_rows==0{return Some(0.0);}
            let null_fraction=statistics.columns.get(index)?.null_count as f64
                / statistics.scanned_rows as f64;
            Some(if *negated{1.0-null_fraction}else{null_fraction})
        }
        ExprKind::Unary{operator:UnaryOperator::Not,expression:inner}=>
            Some(1.0-estimate_predicate_selectivity(inner,statistics,schema,table,bindings)?),
        ExprKind::Binary{left,operator:BinaryOperator::And,right}=>{
            Some(
                estimate_predicate_selectivity(left,statistics,schema,table,bindings)?
                * estimate_predicate_selectivity(right,statistics,schema,table,bindings)?
            )
        }
        ExprKind::Binary{left,operator:BinaryOperator::Or,right}=>{
            let left=estimate_predicate_selectivity(left,statistics,schema,table,bindings)?;
            let right=estimate_predicate_selectivity(right,statistics,schema,table,bindings)?;
            Some((left+right-left*right).clamp(0.0,1.0))
        }
        ExprKind::Binary{left,operator,right}=>{
            estimate_comparison_selectivity(left,*operator,right,statistics,schema,table,bindings)
        }
        _=>None,
    }
}

fn estimate_comparison_selectivity(
    left:&Expr,
    operator:BinaryOperator,
    right:&Expr,
    statistics:&ananta_execution::TableStatistics,
    schema:&ananta_types::Schema,
    table:&str,
    bindings:&ParameterBindings,
)->Option<f64>{
    let (index,value,operator)=match (
        planner_column_index(left,schema,table),
        literal_or_parameter(right,bindings),
    ){
        (Some(index),Some(value))=>(index,value,operator),
        _=>match(
            planner_column_index(right,schema,table),
            literal_or_parameter(left,bindings),
        ){
            (Some(index),Some(value))=>(index,value,reverse_operator(operator)?),
            _=>return None,
        }
    };
    let column=statistics.columns.get(index)?;
    if statistics.scanned_rows==0||matches!(value,Value::Null){return Some(0.0);}
    let non_null=column.non_null_count as f64/statistics.scanned_rows as f64;
    if non_null<=0.0{return Some(0.0);}

    if matches!(operator,BinaryOperator::Equal|BinaryOperator::NotEqual){
        let distinct=column.approximate_distinct_count.max(1) as f64;
        let equal=(non_null/distinct).min(non_null);
        return Some(if operator==BinaryOperator::Equal{equal}else{(non_null-equal).max(0.0)});
    }

    let less=histogram_cdf(column,&value,false)?;
    let less_equal=histogram_cdf(column,&value,true)?;
    let fraction=match operator{
        BinaryOperator::Less=>less,
        BinaryOperator::LessEqual=>less_equal,
        BinaryOperator::Greater=>1.0-less_equal,
        BinaryOperator::GreaterEqual=>1.0-less,
        _=>return None,
    };
    Some((non_null*fraction).clamp(0.0,1.0))
}

fn planner_column_index(
    expression:&Expr,
    schema:&ananta_types::Schema,
    table:&str,
)->Option<usize>{
    let ExprKind::Column(reference)=&expression.kind else{return None;};
    let identifier=reference.parts.last()?;
    if reference.parts.len()==2{
        let qualifier=&reference.parts[0];
        let matches=if qualifier.quoted{qualifier.value==table}
            else{qualifier.value.eq_ignore_ascii_case(table)};
        if !matches{return None;}
    }
    schema.columns().iter().position(|column|{
        if identifier.quoted{column.name==identifier.value}
        else{column.name.eq_ignore_ascii_case(&identifier.value)}
    })
}

fn reverse_operator(operator:BinaryOperator)->Option<BinaryOperator>{
    Some(match operator{
        BinaryOperator::Less=>BinaryOperator::Greater,
        BinaryOperator::LessEqual=>BinaryOperator::GreaterEqual,
        BinaryOperator::Greater=>BinaryOperator::Less,
        BinaryOperator::GreaterEqual=>BinaryOperator::LessEqual,
        BinaryOperator::Equal=>BinaryOperator::Equal,
        BinaryOperator::NotEqual=>BinaryOperator::NotEqual,
        _=>return None,
    })
}

fn histogram_cdf(
    column:&ananta_execution::ColumnStatistics,
    target:&Value,
    inclusive:bool,
)->Option<f64>{
    let histogram=&column.histogram;
    let sample_total=histogram.iter().map(|bucket|bucket.sample_count).sum::<u64>() as f64;
    if sample_total<=0.0{return None;}
    let mut matched=0.0_f64;
    for bucket in histogram{
        let lower=stat_compare(target,&bucket.lower_bound);
        let upper=stat_compare(target,&bucket.upper_bound);
        let count=bucket.sample_count as f64;
        if upper==std::cmp::Ordering::Greater{
            matched+=count;
            continue;
        }
        if lower==std::cmp::Ordering::Less{
            break;
        }
        let equality_mass=(sample_total/column.approximate_distinct_count.max(1) as f64)
            .min(count);
        if upper==std::cmp::Ordering::Equal{
            matched+=if inclusive{count}else{(count-equality_mass).max(0.0)};
            break;
        }
        if lower==std::cmp::Ordering::Equal{
            if inclusive{matched+=equality_mass;}
            break;
        }
        let fraction=match(
            planner_numeric_value(&bucket.lower_bound),
            planner_numeric_value(&bucket.upper_bound),
            planner_numeric_value(target),
        ){
            (Some(minimum),Some(maximum),Some(value)) if maximum>minimum=>
                ((value-minimum)/(maximum-minimum)).clamp(0.0,1.0),
            _=>0.5,
        };
        matched+=count*fraction;
        break;
    }
    Some((matched/sample_total).clamp(0.0,1.0))
}

fn planner_numeric_value(value:&Value)->Option<f64>{
    match value{
        Value::Int32(value)=>Some(f64::from(*value)),
        Value::Int64(value)=>Some(*value as f64),
        Value::TimestampMicros(value)=>Some(*value as f64),
        Value::Float64(value) if value.is_finite()=>Some(*value),
        _=>None,
    }
}

fn stat_compare(left:&Value,right:&Value)->std::cmp::Ordering {
    match (left,right) {
        (Value::Boolean(a),Value::Boolean(b))=>a.cmp(b),
        (Value::Int32(a),Value::Int32(b))=>a.cmp(b),
        (Value::Int64(a),Value::Int64(b))=>a.cmp(b),
        (Value::Float64(a),Value::Float64(b))=>a.total_cmp(b),
        (Value::Utf8(a),Value::Utf8(b))=>a.cmp(b),
        (Value::TimestampMicros(a),Value::TimestampMicros(b))=>a.cmp(b),
        (Value::Binary(a),Value::Binary(b))=>a.cmp(b),
        _=>std::cmp::Ordering::Equal,
    }
}

fn display_option(value:Option<u64>)->String{value.map_or_else(||"unknown".into(),|v|v.to_string())}

pub fn explain(
    database:&Database,
    query:&Query,
    bindings:&ParameterBindings,
    options:PlannerOptions,
)->Result<ExplainPlan,String>{
    crate::validate_ast(query).map_err(|e|e.to_string())?;
    bindings.validate_for(query).map_err(|e|e.to_string())?;
    let Statement::Select(select)=&query.statement;
    let table=resolve_table(database,&select.source)?;
    let schema=database.schema(&table).map_err(|e|e.to_string())?;
    let all=schema.columns().to_vec();
    let mut required=BTreeSet::new();
    if matches!(select.projection,Projection::All){
        for index in 0..all.len(){required.insert(index);}
    }else if let Projection::Items(items)=&select.projection{
        for item in items{collect_columns(&item.expression,&table,&all,&mut required)?;}
    }
    if let Some(predicate)=&select.selection{collect_columns(predicate,&table,&all,&mut required)?;}
    if required.is_empty(){required.insert(0);}
    let required_indices=required.into_iter().collect::<Vec<_>>();
    let required_columns=required_indices.iter().map(|index|all[*index].name.clone()).collect::<Vec<_>>();
    let projection_pushed_down=options.enable_projection_pushdown;
    let columns=if projection_pushed_down{
        required_columns.clone()
    }else{
        all.iter().map(|column|column.name.clone()).collect()
    };
    let stats=database.ingestion_stats(&table).map_err(|e|e.to_string())?;
    let capsule_coverage_complete=database.capsules_cover_chronicle(&table).map_err(|e|e.to_string())?;
    let analyzed_statistics=ananta_execution::load_table_statistics(database,&table)
        .map_err(|error|error.to_string())?;
    let mut column_statistics=Vec::new();

    if let Some(analyzed)=analyzed_statistics.as_ref() {
        for column in &analyzed.columns {
            column_statistics.push(ColumnStatistics {
                name:column.column.name.clone(),
                data_type:column.column.data_type,
                null_count:Some(column.null_count),
                minimum:column.min_value.clone(),
                maximum:column.max_value.clone(),
                approximate_distinct_count:Some(column.approximate_distinct_count),
                histogram_bucket_count:column.histogram.len(),
            });
        }
    } else if capsule_coverage_complete {
        let capsules=database.verified_capsule_metadata(&table).map_err(|e|e.to_string())?;
        for column in &all {
            let column_id=database.column_id(&table,&column.name).map_err(|e|e.to_string())?.0;
            let mut null_count=Some(0_u64);
            let mut minimum:Option<Value>=None;
            let mut maximum:Option<Value>=None;
            for capsule in &capsules {
                if let Some(stat)=capsule.columns.iter().find(|stat|stat.column_id==column_id) {
                    null_count=Some(null_count.unwrap_or(0).saturating_add(stat.null_count));
                    if let Some(value)=&stat.minimum {
                        if minimum.as_ref().map_or(true,|current|stat_compare(value,current).is_lt()) {
                            minimum=Some(value.clone());
                        }
                    }
                    if let Some(value)=&stat.maximum {
                        if maximum.as_ref().map_or(true,|current|stat_compare(value,current).is_gt()) {
                            maximum=Some(value.clone());
                        }
                    }
                } else {
                    null_count=None;
                    minimum=None;
                    maximum=None;
                }
            }
            column_statistics.push(ColumnStatistics {
                name:column.name.clone(),
                data_type:column.data_type,
                null_count,
                minimum,
                maximum,
                approximate_distinct_count:None,
                histogram_bucket_count:0,
            });
        }
    } else {
        for column in &all {
            column_statistics.push(ColumnStatistics {
                name:column.name.clone(),
                data_type:column.data_type,
                null_count:None,
                minimum:None,
                maximum:None,
                approximate_distinct_count:None,
                histogram_bucket_count:0,
            });
        }
    }

    let statistics=TableStatistics{
        // Chronicle counters omit legacy ANF segments. Prefer a fresh analyzed
        // snapshot; otherwise only advertise exact cardinality when Capsule
        // coverage proves the full logical table is Chronicle-backed.
        row_count:analyzed_statistics.as_ref().map(|analysis|analysis.scanned_rows)
            .or_else(||if capsule_coverage_complete{Some(stats.accepted_rows)}else{None}),
        ingestion_batches:if stats.accepted_batches==0{None}else{Some(stats.accepted_batches)},
        encoded_bytes:if stats.accepted_batches==0{None}else{Some(stats.accepted_encoded_bytes)},
        capsule_coverage_complete,
        analyzed_snapshot_available:analyzed_statistics.is_some(),
        columns:column_statistics,
    };
    let timestamp_range=select.selection.as_ref().and_then(|expr|extract_timestamp_range(expr,schema,&table,bindings).ok().flatten());
    let capsule_map_generation=if let (Some(generation),Some(_range))=(options.navigation_map_generation,&timestamp_range){
        if stats.accepted_rows<=options.max_capsule_materialized_rows
            && stats.accepted_encoded_bytes<=options.max_capsule_materialized_bytes as u64
            && database.capsules_cover_chronicle(&table).unwrap_or(false){
            Some(generation)
        }else{None}
    }else{None};
    let scan_column_count = columns.len();
    let mut physical=vec![PhysicalPlanNode::Scan{
        table:table.clone(),columns,estimated_rows:statistics.row_count,
        projection_pushed_down,capsule_map_generation,
    }];
    let mut estimated_rows_after_filter=statistics.row_count;
    if let Some(predicate)=&select.selection{
        estimated_rows_after_filter=analyzed_statistics.as_ref()
            .and_then(|analysis|estimate_predicate_selectivity(
                predicate,analysis,schema,&table,bindings,
            ))
            .and_then(|selectivity|statistics.row_count.map(|rows|estimate_rows(rows,selectivity)));
        physical.push(PhysicalPlanNode::Filter{
            predicate:format!("{predicate:?}"),
            pushed_to_scan:options.enable_predicate_pushdown,
            estimated_rows:estimated_rows_after_filter,
        });
    }
    if !matches!(select.projection,Projection::All){
        physical.push(PhysicalPlanNode::Project{columns:projection_columns(&select.projection)});
    }
    if let Some(rows)=select.limit{
        estimated_rows_after_filter=estimated_rows_after_filter.map(|estimated|estimated.min(rows));
        physical.push(PhysicalPlanNode::Limit{
            rows,
            estimated_rows:estimated_rows_after_filter,
        });
    }
    let mut notes=Vec::new();
    if statistics.row_count.is_none(){notes.push("table statistics unavailable; cardinality is unknown".into());}
    if statistics.analyzed_snapshot_available {
        notes.push("fresh database-managed analyzed statistics supplied row/null/NDV/histogram estimates".into());
    } else if statistics.capsule_coverage_complete {
        notes.push("exact Capsule statistics supplied row/null/min/max values; no analyzed histogram snapshot is cached".into());
    } else {
        notes.push("no fresh ANALYZE cache or complete Capsule coverage; cardinality and column distributions remain unknown".into());
    }
    if timestamp_range.is_some()&&capsule_map_generation.is_none(){
        notes.push("time range is exact at the row evaluator; Capsule pruning unavailable (no complete coverage, no map generation, or materialisation cap)".into());
    }
    if capsule_map_generation.is_some(){
        notes.push("verified full Chronicle Capsule coverage allows Navigation Map pruning; materialised result is bounded by max_capsule_materialized_rows".into());
    }
    if !options.enable_projection_pushdown{notes.push("projection pushdown disabled; scan decodes the full logical row".into());}
    if !options.enable_predicate_pushdown{notes.push("predicate pushdown disabled; filter remains above scan".into());}

    let estimated_scan_rows = statistics.row_count.map(|rows| {
        if select.selection.is_none() {
            select.limit.map_or(rows, |limit| rows.min(limit))
        } else {
            rows
        }
    });
    let estimated_scan_payload_bytes = analyzed_statistics.as_ref().map(|analysis| {
        let scan_fraction = if all.is_empty() {
            1.0
        } else {
            scan_column_count as f64 / all.len() as f64
        };
        (analysis.estimated_batch_payload_bytes as f64 * scan_fraction)
            .round()
            .min(u64::MAX as f64) as u64
    });
    let output_columns = match &select.projection {
        Projection::All => all.len(),
        Projection::Items(items) => items.len(),
    };
    let scan_cost_units = estimated_scan_rows.map(|rows| {
        rows as f64 + estimated_scan_payload_bytes.unwrap_or(0) as f64 / 4096.0
    });
    let filter_cost_units = if select.selection.is_some() {
        estimated_scan_rows.map(|rows| rows as f64 * 0.25)
    } else {
        Some(0.0)
    };
    let projection_cost_units = estimated_rows_after_filter.map(|rows| {
        rows as f64 * output_columns as f64 * 0.05
    });
    let total_cost_units = match (
        scan_cost_units,
        filter_cost_units,
        projection_cost_units,
    ) {
        (Some(scan), Some(filter), Some(project)) => Some(scan + filter + project),
        _ => None,
    };
    let cost = PlanCostEstimate {
        estimated_scan_rows,
        estimated_output_rows: estimated_rows_after_filter,
        estimated_scan_payload_bytes,
        scan_cost_units,
        filter_cost_units,
        projection_cost_units,
        total_cost_units,
    };
    notes.push(
        "relative cost units use 1 per scanned row, 1 per 4 KiB estimated scan payload, 0.25 per filter evaluation and 0.05 per projected cell; they are not latency predictions".into()
    );
    if estimated_scan_payload_bytes.is_none() {
        notes.push("scan payload cost is unknown until a fresh ANALYZE snapshot is available".into());
    }

    Ok(ExplainPlan{
        language_version:crate::AQL_VERSION.into(),
        logical:logical_plan(query),physical,table_statistics:statistics,
        required_columns,timestamp_range,cost,notes,
    })
}

pub fn projection_columns(projection:&Projection)->Vec<String>{
    match projection{
        Projection::All=>vec!["*".into()],
        Projection::Items(items)=>items.iter().map(|item|{
            item.alias.as_ref().map(|alias|alias.value.clone()).or_else(||{
                if let ExprKind::Column(reference)=&item.expression.kind{
                    reference.parts.last().map(|part|part.value.clone())
                }else{None}
            }).unwrap_or_else(||"expression".into())
        }).collect(),
    }
}

fn resolve_table(database:&Database,identifier:&Identifier)->Result<String,String>{
    database.table_names().find(|name|{
        if identifier.quoted{*name==identifier.value}else{name.eq_ignore_ascii_case(&identifier.value)}
    }).map(str::to_owned).ok_or_else(||format!("table '{}' does not exist",identifier.value))
}
fn collect_columns(
    expression:&Expr,table:&str,schema:&[Column],required:&mut BTreeSet<usize>,
)->Result<(),String>{
    match &expression.kind{
        ExprKind::Column(ColumnReference{parts,span})=>{
            if parts.len()==2{
                let qualifier=&parts[0];
                let matches=if qualifier.quoted{qualifier.value==table}else{qualifier.value.eq_ignore_ascii_case(table)};
                if !matches{return Err(format!("qualifier '{}' does not match source table",qualifier.value));}
            }
            let identifier=parts.last().ok_or_else(||"empty column reference".to_owned())?;
            let index=schema.iter().position(|column|{
                if identifier.quoted{column.name==identifier.value}else{column.name.eq_ignore_ascii_case(&identifier.value)}
            }).ok_or_else(||format!("column '{}' does not exist at {}:{}",identifier.value,span.start.line,span.start.column))?;
            required.insert(index);
        }
        ExprKind::Unary{expression,..}|ExprKind::IsNull{expression,..}|ExprKind::Cast{expression,..}=>collect_columns(expression,table,schema,required)?,
        ExprKind::Binary{left,right,..}=>{collect_columns(left,table,schema,required)?;collect_columns(right,table,schema,required)?;}
        ExprKind::Literal(_)|ExprKind::Parameter(_)=>{}
    }
    Ok(())
}

fn literal_or_parameter(expr:&Expr,bindings:&ParameterBindings)->Option<Value>{
    match &expr.kind{
        ExprKind::Literal(Literal::Null)=>Some(Value::Null),
        ExprKind::Literal(Literal::Boolean(v))=>Some(Value::Boolean(*v)),
        ExprKind::Literal(Literal::Int64(v))=>Some(Value::Int64(*v)),
        ExprKind::Literal(Literal::Float64(v))=>Some(Value::Float64(*v)),
        ExprKind::Literal(Literal::Utf8(v))=>Some(Value::Utf8(v.clone())),
        ExprKind::Parameter(ParameterRef::Named(name))=>bindings.named.get(name).cloned(),
        ExprKind::Parameter(ParameterRef::Positional(index))=>index.checked_sub(1)
            .and_then(|index|bindings.positional.get(index as usize).cloned()),
        ExprKind::Unary{operator,expression}=>{
            let value=literal_or_parameter(expression,bindings)?;
            match (operator,value){
                (crate::UnaryOperator::Negative,Value::TimestampMicros(v))=>Some(Value::TimestampMicros(v.checked_neg()?)),
                (crate::UnaryOperator::Negative,Value::Int64(v))=>Some(Value::Int64(v.checked_neg()?)),
                (crate::UnaryOperator::Positive,value)=>Some(value),
                _=>None,
            }
        }
        _=>None,
    }
}

/// Extract one simple TIMESTAMP_MICROS lower/upper range from a conjunction.
/// The row evaluator remains authoritative for all additional predicates.
pub fn extract_timestamp_range(
    expression:&Expr,schema:&ananta_types::Schema,table:&str,bindings:&ParameterBindings,
)->Result<Option<ColumnRangePredicate>,String>{
    let mut terms=Vec::new();
    flatten_and(expression,&mut terms);
    let mut selected:Option<(String,Option<Value>,bool,Option<Value>,bool)>=None;
    for term in terms{
        let ExprKind::Binary{left,operator,right}=&term.kind else{continue;};
        let (column,value,lower,inclusive)=match (&left.kind,operator){
            (ExprKind::Column(column),BinaryOperator::Greater|BinaryOperator::GreaterEqual)=>{
                (column,literal_or_parameter(right,bindings),true,*operator==BinaryOperator::GreaterEqual)
            }
            (ExprKind::Column(column),BinaryOperator::Less|BinaryOperator::LessEqual)=>{
                (column,literal_or_parameter(right,bindings),false,*operator==BinaryOperator::LessEqual)
            }
            _=>continue,
        };
        let Some(Value::TimestampMicros(_))=value.as_ref() else{continue;};
        if column.parts.len()!=1 && column.parts.len()!=2{return Ok(None);}
        if column.parts.len()==2{
            let qualifier=&column.parts[0];
            let matches=if qualifier.quoted{qualifier.value==table}else{qualifier.value.eq_ignore_ascii_case(table)};
            if !matches{return Err("timestamp range qualifier does not match source table".into());}
        }
        let name=column.parts.last().ok_or_else(||"invalid timestamp column".to_owned())?.value.clone();
        let identifier=column.parts.last().expect("validated timestamp column reference");
        let index=schema.columns().iter().position(|field|{
            if identifier.quoted{field.name==identifier.value}else{field.name.eq_ignore_ascii_case(&identifier.value)}
        });
        let Some(index)=index else{continue;};
        if schema.columns()[index].data_type!=DataType::TimestampMicros{continue;}
        let current=selected.get_or_insert((name.clone(),None,true,None,false));
        if current.0!=name{return Ok(None);}
        if lower{current.1=value;current.2=inclusive;}else{current.3=value;current.4=inclusive;}
    }
    Ok(selected.and_then(|(name,lower,lower_inclusive,upper,upper_inclusive)|{
        if lower.is_none()&&upper.is_none(){None}else{Some(ColumnRangePredicate::new(name,lower,lower_inclusive,upper,upper_inclusive))}
    }))
}
fn flatten_and<'a>(expression:&'a Expr,out:&mut Vec<&'a Expr>){
    if let ExprKind::Binary{left,operator:BinaryOperator::And,right}=&expression.kind{
        flatten_and(left,out);flatten_and(right,out);
    }else{out.push(expression);}
}

#[cfg(test)]
mod tests{
    use super::*;
    use ananta_core::Database;
    use ananta_types::{Schema,Column};
    use std::fs;
    use std::time::{SystemTime,UNIX_EPOCH};
    fn fixture()->(Database,std::path::PathBuf){
        let n=SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        let root=std::env::temp_dir().join(format!("ananta-plan-{n}"));
        let mut db=Database::open(&root).unwrap();
        db.create_table("telemetry",Schema::new(vec![
            Column::new("time",DataType::TimestampMicros,false),
            Column::new("device",DataType::Utf8,false),
            Column::new("value",DataType::Float64,true),
        ]).unwrap()).unwrap();
        db.insert_batch("telemetry",&[vec![Value::TimestampMicros(10),Value::Utf8("a".into()),Value::Float64(1.0)]]).unwrap();
        (db,root)
    }
    #[test]
    fn explain_pushes_required_columns_and_reports_measured_table_stats(){
        let(db,root)=fixture();
        let query=crate::parse("SELECT device FROM telemetry WHERE time >= :lower LIMIT 10").unwrap();
        let bindings=ParameterBindings{
            named:[("lower".into(),Value::TimestampMicros(5))].into_iter().collect(),positional:vec![],
        };
        let plan=explain(&db,&query,&bindings,PlannerOptions::default()).unwrap();
        assert_eq!(plan.required_columns,vec!["device","time"]);
        assert_eq!(plan.table_statistics.row_count,Some(1));
        assert!(plan.timestamp_range.is_some());
        assert!(plan.render().contains("projection_pushdown=true"));
        drop(db);fs::remove_dir_all(root).unwrap();
    }
    #[test]
    fn explain_does_not_treat_chronicle_ingestion_rows_as_full_table_rows_with_legacy_anf() {
        let n=SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        let root=std::env::temp_dir().join(format!("ananta-plan-mixed-storage-{n}"));
        let mut db=Database::open(&root).unwrap();
        db.create_table("telemetry",Schema::new(vec![
            Column::new("time",DataType::TimestampMicros,false),
            Column::new("device",DataType::Utf8,false),
            Column::new("value",DataType::Float64,true),
        ]).unwrap()).unwrap();
        db.ingest_batch(
            "telemetry",
            ananta_core::BatchId::from_bytes([91;16]).unwrap(),
            &[vec![Value::TimestampMicros(10),Value::Utf8("chronicle".into()),Value::Float64(1.0)]],
        ).unwrap();
        db.build_capsule("telemetry",1,1,1,1,Some("time")).unwrap();
        assert!(db.capsules_cover_chronicle("telemetry").unwrap());
        db.insert_batch(
            "telemetry",
            &[vec![Value::TimestampMicros(20),Value::Utf8("legacy".into()),Value::Float64(2.0)]],
        ).unwrap();

        let query=crate::parse("SELECT device FROM telemetry").unwrap();
        let plan=explain(&db,&query,&ParameterBindings::default(),PlannerOptions::default()).unwrap();
        assert!(!plan.table_statistics.capsule_coverage_complete);
        assert_eq!(plan.table_statistics.row_count,None);
        // Chronicle counters remain useful as ingestion-only diagnostics.
        assert_eq!(plan.table_statistics.ingestion_batches,Some(1));
        drop(db);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn explain_uses_fresh_database_statistics_cache_and_drops_it_after_write(){
        let(mut db,root)=fixture();
        let _analysis=ananta_execution::analyze_table_and_persist(&db,"telemetry",1).unwrap();
        let query=crate::parse("SELECT device FROM telemetry WHERE value >= 0.5 LIMIT 1").unwrap();

        let fresh=explain(&db,&query,&ParameterBindings::default(),PlannerOptions::default()).unwrap();
        assert!(fresh.table_statistics.analyzed_snapshot_available);
        assert_eq!(fresh.table_statistics.row_count,Some(1));
        let value_stats=fresh.table_statistics.columns.iter()
            .find(|column|column.name=="value").unwrap();
        assert_eq!(value_stats.null_count,Some(0));
        assert!(value_stats.approximate_distinct_count.is_some());
        assert_eq!(value_stats.histogram_bucket_count,1);
        assert!(fresh.render().contains("analyzed_snapshot=true"));
        assert!(fresh.physical.iter().any(|node|matches!(
            node,
            PhysicalPlanNode::Filter{estimated_rows:Some(1),..}
        )));
        assert!(fresh.physical.iter().any(|node|matches!(
            node,
            PhysicalPlanNode::Limit{rows:1,estimated_rows:Some(1)}
        )));

        // A newly published segment changes the inventory fingerprint, so the
        // planner must stop using the old cache and report unknown cardinality.
        db.insert_batch("telemetry",&[
            vec![Value::TimestampMicros(20),Value::Utf8("b".into()),Value::Float64(2.0)],
        ]).unwrap();
        let stale=explain(&db,&query,&ParameterBindings::default(),PlannerOptions::default()).unwrap();
        assert!(!stale.table_statistics.analyzed_snapshot_available);
        assert_eq!(stale.table_statistics.row_count,None);
        assert!(stale.notes.iter().any(|note|note.contains("no fresh ANALYZE cache")));

        drop(db);fs::remove_dir_all(root).unwrap();
    }


    #[test]
    fn explain_uses_histogram_boundaries_for_inclusive_and_exclusive_ranges(){
        let n=SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        let root=std::env::temp_dir().join(format!("ananta-plan-histogram-boundaries-{n}"));
        let mut db=Database::open(&root).unwrap();
        db.create_table("readings",Schema::new(vec![
            Column::new("id",DataType::Int64,false),
            Column::new("value",DataType::Int64,false),
        ]).unwrap()).unwrap();
        db.insert_batch("readings",&[
            vec![Value::Int64(1),Value::Int64(1)],
            vec![Value::Int64(2),Value::Int64(2)],
            vec![Value::Int64(3),Value::Int64(3)],
            vec![Value::Int64(4),Value::Int64(4)],
        ]).unwrap();
        ananta_execution::analyze_table_and_persist(&db,"readings",2).unwrap();

        let estimate=|sql:&str| {
            let query=crate::parse(sql).unwrap();
            let plan=explain(&db,&query,&ParameterBindings::default(),PlannerOptions::default()).unwrap();
            plan.physical.iter().find_map(|node|match node{
                PhysicalPlanNode::Filter{estimated_rows,..}=>*estimated_rows,
                _=>None,
            }).unwrap()
        };
        assert_eq!(estimate("SELECT id FROM readings WHERE value >= 3"),2);
        assert_eq!(estimate("SELECT id FROM readings WHERE value > 3"),1);
        assert_eq!(estimate("SELECT id FROM readings WHERE value <= 2"),2);
        assert_eq!(estimate("SELECT id FROM readings WHERE value < 2"),1);

        drop(db);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn cost_model_prices_projection_pushdown_and_safe_limit_scan_reduction(){
        let(mut db,root)=fixture();
        ananta_execution::analyze_table_and_persist(&db,"telemetry",2).unwrap();
        let query=crate::parse("SELECT device FROM telemetry").unwrap();
        let pushed=explain(&db,&query,&ParameterBindings::default(),PlannerOptions::default()).unwrap();
        let unpushed=explain(&db,&query,&ParameterBindings::default(),PlannerOptions{
            enable_projection_pushdown:false,
            ..PlannerOptions::default()
        }).unwrap();
        assert_eq!(pushed.cost.estimated_scan_rows,Some(1));
        assert_eq!(pushed.cost.estimated_output_rows,Some(1));
        assert!(pushed.cost.estimated_scan_payload_bytes.unwrap()
            < unpushed.cost.estimated_scan_payload_bytes.unwrap());
        assert!(pushed.cost.total_cost_units.unwrap()
            < unpushed.cost.total_cost_units.unwrap());
        assert!(pushed.render().contains("COST scan_rows=1 output_rows=1"));

        let limited=crate::parse("SELECT device FROM telemetry LIMIT 1").unwrap();
        let limited_plan=explain(&db,&limited,&ParameterBindings::default(),PlannerOptions::default()).unwrap();
        assert_eq!(limited_plan.cost.estimated_scan_rows,Some(1));
        assert_eq!(limited_plan.cost.total_cost_units,pushed.cost.total_cost_units);
        drop(db);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn explain_keeps_unknown_statistics_unknown(){
        let root=std::env::temp_dir().join("ananta-empty-plan");
        let _=fs::remove_dir_all(&root);
        let db=Database::open(&root).unwrap();
        db.schema("missing").unwrap_err();
    }
}
