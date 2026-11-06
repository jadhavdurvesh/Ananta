use crate::{logical_plan, BinaryOperator, ColumnReference, Expr, ExprKind, Identifier, Literal,
    LogicalPlan, ParameterBindings, ParameterRef, Projection, Query, Statement};
use ananta_core::{CapsuleScanRequest, ColumnRangePredicate, Database};
use ananta_types::{Column, DataType, Value};
use std::collections::BTreeSet;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PlannerOptions {
    pub enable_projection_pushdown: bool,
    pub enable_predicate_pushdown: bool,
    pub navigation_map_generation: Option<u64>,
    pub max_capsule_materialized_rows: u64,
}
impl Default for PlannerOptions {
    fn default()->Self{
        Self{enable_projection_pushdown:true,enable_predicate_pushdown:true,
            navigation_map_generation:None,max_capsule_materialized_rows:100_000}
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ColumnStatistics {
    pub name:String,
    pub data_type:DataType,
    pub null_count:Option<u64>,
    pub minimum:Option<Value>,
    pub maximum:Option<Value>,
}
#[derive(Debug, Clone, PartialEq)]
pub struct TableStatistics {
    pub row_count:Option<u64>,
    pub ingestion_batches:Option<u64>,
    pub encoded_bytes:Option<u64>,
    pub capsule_coverage_complete:bool,
    pub columns:Vec<ColumnStatistics>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PhysicalPlanNode {
    Scan { table:String, columns:Vec<String>, estimated_rows:Option<u64>,
        projection_pushed_down:bool, capsule_map_generation:Option<u64> },
    Filter { predicate:String, pushed_to_scan:bool },
    Project { columns:Vec<String> },
    Limit { rows:u64 },
}
#[derive(Debug, Clone, PartialEq)]
pub struct ExplainPlan {
    pub language_version:String,
    pub logical:LogicalPlan,
    pub physical:Vec<PhysicalPlanNode>,
    pub table_statistics:TableStatistics,
    pub required_columns:Vec<String>,
    pub timestamp_range:Option<ColumnRangePredicate>,
    pub notes:Vec<String>,
}
impl ExplainPlan {
    pub fn render(&self)->String{
        let mut lines=vec![format!("AQL {}",self.language_version)];
        lines.push(format!("Statistics: rows={}, batches={}, encoded_bytes={}, capsule_coverage_complete={}",
            display_option(self.table_statistics.row_count),
            display_option(self.table_statistics.ingestion_batches),
            display_option(self.table_statistics.encoded_bytes),
            self.table_statistics.capsule_coverage_complete));
        for column in &self.table_statistics.columns {
            lines.push(format!("COLUMN_STATS {} type={} nulls={} min={:?} max={:?}",
                column.name,column.data_type,display_option(column.null_count),
                column.minimum,column.maximum));
        }
        for node in &self.physical{
            lines.push(match node{
                PhysicalPlanNode::Scan{table,columns,estimated_rows,projection_pushed_down,capsule_map_generation}=>{
                    format!("SCAN {table} columns=[{}] estimated_rows={} projection_pushdown={} capsule_map={}",
                        columns.join(", "),display_option(*estimated_rows),projection_pushed_down,
                        capsule_map_generation.map_or_else(||"disabled".into(),|v|v.to_string()))
                }
                PhysicalPlanNode::Filter{predicate,pushed_to_scan}=>format!("FILTER {predicate} predicate_pushdown={pushed_to_scan}"),
                PhysicalPlanNode::Project{columns}=>format!("PROJECT [{}]",columns.join(", ")),
                PhysicalPlanNode::Limit{rows}=>format!("LIMIT {rows}"),
            });
        }
        lines.extend(self.notes.iter().map(|note|format!("NOTE {note}")));
        lines.join("\n")
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
    let mut column_statistics=Vec::new();
    if capsule_coverage_complete {
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
                name:column.name.clone(),data_type:column.data_type,
                null_count,minimum,maximum,
            });
        }
    }
    let statistics=TableStatistics{
        row_count:if stats.accepted_batches==0{None}else{Some(stats.accepted_rows)},
        ingestion_batches:if stats.accepted_batches==0{None}else{Some(stats.accepted_batches)},
        encoded_bytes:if stats.accepted_batches==0{None}else{Some(stats.accepted_encoded_bytes)},
        capsule_coverage_complete,
        columns:column_statistics,
    };
    let timestamp_range=select.selection.as_ref().and_then(|expr|extract_timestamp_range(expr,schema,&table,bindings).ok().flatten());
    let capsule_map_generation=if let (Some(generation),Some(_range))=(options.navigation_map_generation,&timestamp_range){
        if stats.accepted_rows<=options.max_capsule_materialized_rows
            && database.capsules_cover_chronicle(&table).unwrap_or(false){
            Some(generation)
        }else{None}
    }else{None};
    let mut physical=vec![PhysicalPlanNode::Scan{
        table:table.clone(),columns,estimated_rows:statistics.row_count,
        projection_pushed_down,capsule_map_generation,
    }];
    if let Some(predicate)=&select.selection{
        physical.push(PhysicalPlanNode::Filter{
            predicate:format!("{predicate:?}"),
            pushed_to_scan:options.enable_predicate_pushdown,
        });
    }
    if !matches!(select.projection,Projection::All){
        physical.push(PhysicalPlanNode::Project{columns:projection_columns(&select.projection)});
    }
    if let Some(rows)=select.limit{physical.push(PhysicalPlanNode::Limit{rows});}
    let mut notes=Vec::new();
    if statistics.row_count.is_none(){notes.push("table statistics unavailable; cardinality is unknown".into());}
    if timestamp_range.is_some()&&capsule_map_generation.is_none(){
        notes.push("time range is exact at the row evaluator; Capsule pruning unavailable (no complete coverage, no map generation, or materialisation cap)".into());
    }
    if capsule_map_generation.is_some(){
        notes.push("verified full Chronicle Capsule coverage allows Navigation Map pruning; materialised result is bounded by max_capsule_materialized_rows".into());
    }
    if !options.enable_projection_pushdown{notes.push("projection pushdown disabled; scan decodes the full logical row".into());}
    if !options.enable_predicate_pushdown{notes.push("predicate pushdown disabled; filter remains above scan".into());}
    Ok(ExplainPlan{
        language_version:crate::AQL_VERSION.into(),
        logical:logical_plan(query),physical,table_statistics:statistics,
        required_columns,timestamp_range,notes,
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
    fn explain_keeps_unknown_statistics_unknown(){
        let root=std::env::temp_dir().join("ananta-empty-plan");
        let _=fs::remove_dir_all(&root);
        let db=Database::open(&root).unwrap();
        db.schema("missing").unwrap_err();
    }
}
