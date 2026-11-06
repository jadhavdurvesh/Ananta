use crate::{
    AqlErrorCode, AqlExecutionError, BinaryOperator, ColumnReference, Expr, ExprKind,
    Identifier, Literal, ParameterBindings, ParameterRef, Projection, Query, Statement,
    UnaryOperator, VectorExpr,
};
use crate::{
    ExecutionContext, ExecutionError, ExecutionPipeline, FilterOperator, LimitOperator,
    PhysicalOperator, ProjectionOperator, ResourceLimits, ScanOperator,
};
use ananta_core::Database;
use ananta_types::{Column, DataType, Schema, Value};
use std::collections::BTreeSet;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

pub fn execute_vectorized(
    database:&Database,
    query:&Query,
    bindings:&ParameterBindings,
    batch_rows:usize,
    limits:ResourceLimits,
    cancellation:Arc<AtomicBool>,
)->Result<ExecutionPipeline,AqlExecutionError>{
    crate::validate_ast(query).map_err(parse_error)?;
    bindings.validate_for(query).map_err(parse_error)?;
    let Statement::Select(select)=&query.statement;
    let table=resolve_table(database,&select.source)?;
    let schema=database.schema(&table).map_err(storage_error)?.clone();

    let mut required=BTreeSet::new();
    match &select.projection{
        Projection::All=>for index in 0..schema.len(){required.insert(index);},
        Projection::Items(items)=>{
            for item in items{
                match &item.expression.kind{
                    ExprKind::Column(reference)=>{required.insert(resolve_column(reference,&schema,&table)?);}
                    _=>return Err(query_error(AqlErrorCode::InvalidQuery,
                        "vectorized M5 projection currently accepts column references; use execute for scalar projection expressions",
                        item.expression.span)),
                }
            }
        }
    }
    if let Some(predicate)=&select.selection{
        collect_required(predicate,&schema,&table,&mut required)?;
    }
    // A literal-only predicate/projection still needs one physical column to preserve row counts.
    if required.is_empty(){required.insert(0);}
    let required_indices=required.into_iter().collect::<Vec<_>>();
    let required_names=required_indices.iter().map(|index|schema.columns()[*index].name.as_str()).collect::<Vec<_>>();
    let mut column_map=vec![None;schema.len()];
    for (slot,index) in required_indices.iter().enumerate(){column_map[*index]=Some(slot);}

    let scan=ScanOperator::new(database,&table,&required_names,batch_rows)
        .map_err(vector_error)?;
    let mut root:Box<dyn PhysicalOperator>=Box::new(scan);
    if let Some(predicate)=&select.selection{
        let expression=compile_vector_expr(predicate,&schema,&table,&column_map,bindings)?;
        if infer_type(predicate,&schema,&table,bindings)?.is_some_and(|kind|kind!=DataType::Boolean){
            return Err(query_error(AqlErrorCode::TypeMismatch,"WHERE expression must be BOOLEAN",predicate.span));
        }
        root=Box::new(FilterOperator::new(root,expression));
    }

    if let Projection::Items(items)=&select.projection{
        let mut indices=Vec::with_capacity(items.len());
        let mut fields=Vec::with_capacity(items.len());
        for item in items{
            let ExprKind::Column(reference)=&item.expression.kind else{
                return Err(query_error(AqlErrorCode::InvalidQuery,
                    "vectorized M5 projection currently accepts column references",item.expression.span));
            };
            let source_index=resolve_column(reference,&schema,&table)?;
            let slot=column_map[source_index].ok_or_else(||query_error(
                AqlErrorCode::InvalidQuery,"projected column was not included in scan projection",reference.span))?;
            let source=&schema.columns()[source_index];
            let name=item.alias.as_ref().map_or_else(||source.name.clone(),|alias|alias.value.clone());
            indices.push(slot);
            fields.push(Column::new(name,source.data_type,source.nullable));
        }
        root=Box::new(ProjectionOperator::new_with_fields(root,indices,fields).map_err(vector_error)?);
    }
    if let Some(limit)=select.limit{
        let limit=usize::try_from(limit).map_err(|_|query_error(
            AqlErrorCode::InvalidQuery,"LIMIT exceeds platform row-count range",select.span))?;
        root=Box::new(LimitOperator::new(root,limit));
    }
    let mut context=ExecutionContext::new(limits).map_err(vector_error)?;
    context.cancellation=cancellation;
    Ok(ExecutionPipeline::new(root,context))
}

pub fn execute_vectorized_source(
    database:&Database,
    source:&str,
    bindings:&ParameterBindings,
    batch_rows:usize,
    limits:ResourceLimits,
    cancellation:Arc<AtomicBool>,
)->Result<ExecutionPipeline,AqlExecutionError>{
    let query=crate::parse(source).map_err(parse_error)?;
    execute_vectorized(database,&query,bindings,batch_rows,limits,cancellation)
}

fn resolve_table(database:&Database,identifier:&Identifier)->Result<String,AqlExecutionError>{
    database.table_names().find(|name|{
        if identifier.quoted{*name==identifier.value}else{name.eq_ignore_ascii_case(&identifier.value)}
    }).map(str::to_owned).ok_or_else(||query_error(
        AqlErrorCode::TableNotFound,format!("table '{}' does not exist",identifier.value),identifier.span))
}
fn resolve_column(reference:&ColumnReference,schema:&Schema,table:&str)->Result<usize,AqlExecutionError>{
    if reference.parts.len()==2{
        let qualifier=&reference.parts[0];
        let matches=if qualifier.quoted{qualifier.value==table}else{qualifier.value.eq_ignore_ascii_case(table)};
        if !matches{return Err(query_error(AqlErrorCode::ColumnNotFound,
            format!("qualifier '{}' does not match source table",qualifier.value),qualifier.span));}
    }
    let identifier=reference.parts.last().expect("parser validates non-empty column references");
    schema.columns().iter().position(|column|{
        if identifier.quoted{column.name==identifier.value}else{column.name.eq_ignore_ascii_case(&identifier.value)}
    }).ok_or_else(||query_error(AqlErrorCode::ColumnNotFound,
        format!("column '{}' does not exist",identifier.value),identifier.span))
}
fn collect_required(
    expression:&Expr,schema:&Schema,table:&str,required:&mut BTreeSet<usize>,
)->Result<(),AqlExecutionError>{
    match &expression.kind{
        ExprKind::Column(reference)=>{required.insert(resolve_column(reference,schema,table)?);}
        ExprKind::Unary{expression,..}|ExprKind::IsNull{expression,..}|ExprKind::Cast{expression,..}=>{
            collect_required(expression,schema,table,required)?;
        }
        ExprKind::Binary{left,right,..}=>{
            collect_required(left,schema,table,required)?;
            collect_required(right,schema,table,required)?;
        }
        ExprKind::Literal(_)|ExprKind::Parameter(_)=>{}
    }
    Ok(())
}
fn value_for_literal_or_parameter(
    expression:&Expr,bindings:&ParameterBindings,
)->Result<Value,AqlExecutionError>{
    match &expression.kind{
        ExprKind::Literal(Literal::Null)=>Ok(Value::Null),
        ExprKind::Literal(Literal::Boolean(value))=>Ok(Value::Boolean(*value)),
        ExprKind::Literal(Literal::Int64(value))=>Ok(Value::Int64(*value)),
        ExprKind::Literal(Literal::Float64(value))=>Ok(Value::Float64(*value)),
        ExprKind::Literal(Literal::Utf8(value))=>Ok(Value::Utf8(value.clone())),
        ExprKind::Parameter(ParameterRef::Named(name))=>bindings.named.get(name).cloned()
            .ok_or_else(||query_error(AqlErrorCode::InvalidParameter,format!("missing binding for :{name}"),expression.span)),
        ExprKind::Parameter(ParameterRef::Positional(index))=>index.checked_sub(1)
            .and_then(|index|bindings.positional.get(index as usize).cloned())
            .ok_or_else(||query_error(AqlErrorCode::InvalidParameter,"missing positional parameter binding",expression.span)),
        _=>Err(query_error(AqlErrorCode::InvalidQuery,"expression is not a literal/parameter",expression.span)),
    }
}
fn infer_type(
    expression:&Expr,schema:&Schema,table:&str,bindings:&ParameterBindings,
)->Result<Option<DataType>,AqlExecutionError>{
    match &expression.kind{
        ExprKind::Column(reference)=>Ok(Some(schema.columns()[resolve_column(reference,schema,table)?].data_type)),
        ExprKind::Literal(Literal::Null)=>Ok(None),
        ExprKind::Literal(Literal::Boolean(_))=>Ok(Some(DataType::Boolean)),
        ExprKind::Literal(Literal::Int64(_))=>Ok(Some(DataType::Int64)),
        ExprKind::Literal(Literal::Float64(_))=>Ok(Some(DataType::Float64)),
        ExprKind::Literal(Literal::Utf8(_))=>Ok(Some(DataType::Utf8)),
        ExprKind::Parameter(parameter)=>{
            let value=match parameter{
                ParameterRef::Named(name)=>bindings.named.get(name),
                ParameterRef::Positional(index)=>index.checked_sub(1).and_then(|i|bindings.positional.get(i as usize)),
            }.ok_or_else(||query_error(AqlErrorCode::InvalidParameter,"missing parameter binding",expression.span))?;
            Ok(value.data_type())
        }
        ExprKind::IsNull{..}=>Ok(Some(DataType::Boolean)),
        ExprKind::Unary{operator,expression:inner}=>{
            let kind=infer_type(inner,schema,table,bindings)?;
            if *operator==UnaryOperator::Not{
                if kind.is_some_and(|kind|kind!=DataType::Boolean){return Err(query_error(AqlErrorCode::TypeMismatch,"NOT requires BOOLEAN",expression.span));}
                Ok(Some(DataType::Boolean))
            }else if kind.is_some_and(|kind|!matches!(kind,DataType::Int32|DataType::Int64|DataType::Float64)){
                Err(query_error(AqlErrorCode::TypeMismatch,"unary sign requires numeric input",expression.span))
            }else{Ok(kind)}
        }
        ExprKind::Binary{left,operator,right}=>{
            let a=infer_type(left,schema,table,bindings)?;
            let b=infer_type(right,schema,table,bindings)?;
            if matches!(operator,BinaryOperator::And|BinaryOperator::Or){
                if a.is_some_and(|v|v!=DataType::Boolean)||b.is_some_and(|v|v!=DataType::Boolean){
                    return Err(query_error(AqlErrorCode::TypeMismatch,"AND/OR requires BOOLEAN",expression.span));
                }
                Ok(Some(DataType::Boolean))
            }else if matches!(operator,BinaryOperator::Equal|BinaryOperator::NotEqual|BinaryOperator::Less
                |BinaryOperator::LessEqual|BinaryOperator::Greater|BinaryOperator::GreaterEqual){
                if let (Some(a),Some(b))=(a,b){if a!=b{return Err(query_error(AqlErrorCode::TypeMismatch,
                    "comparison operands must have identical types",expression.span));}}
                Ok(Some(DataType::Boolean))
            }else{
                Err(query_error(AqlErrorCode::InvalidQuery,"vectorized operator path currently supports boolean/comparison predicates only",expression.span))
            }
        }
        ExprKind::Cast{..}=>Err(query_error(AqlErrorCode::InvalidQuery,
            "CAST is available in the scalar AQL executor; vectorized CAST is planned for the next operator increment",expression.span)),
    }
}
fn compile_vector_expr(
    expression:&Expr,schema:&Schema,table:&str,column_map:&[Option<usize>],bindings:&ParameterBindings,
)->Result<VectorExpr,AqlExecutionError>{
    match &expression.kind{
        ExprKind::Column(reference)=>{
            let index=resolve_column(reference,schema,table)?;
            Ok(VectorExpr::Column(column_map[index].ok_or_else(||query_error(
                AqlErrorCode::InvalidQuery,"predicate column missing from scan projection",reference.span))?))
        }
        ExprKind::Literal(_)|ExprKind::Parameter(_)=>Ok(VectorExpr::Literal(value_for_literal_or_parameter(expression,bindings)?)),
        ExprKind::IsNull{expression:inner,negated}=>{
            let inner=Box::new(compile_vector_expr(inner,schema,table,column_map,bindings)?);
            Ok(if *negated{VectorExpr::IsNotNull(inner)}else{VectorExpr::IsNull(inner)})
        }
        ExprKind::Unary{operator:UnaryOperator::Not,expression:inner}=>Ok(VectorExpr::Not(Box::new(
            compile_vector_expr(inner,schema,table,column_map,bindings)?))),
        ExprKind::Unary{..}=>Err(query_error(AqlErrorCode::InvalidQuery,
            "vectorized predicates do not support unary numeric expressions yet",expression.span)),
        ExprKind::Binary{left,operator,right}=>{
            let left=Box::new(compile_vector_expr(left,schema,table,column_map,bindings)?);
            let right=Box::new(compile_vector_expr(right,schema,table,column_map,bindings)?);
            Ok(match operator{
                BinaryOperator::And=>VectorExpr::And(left,right),
                BinaryOperator::Or=>VectorExpr::Or(left,right),
                BinaryOperator::Equal=>VectorExpr::Eq(left,right),
                BinaryOperator::NotEqual=>VectorExpr::NotEq(left,right),
                BinaryOperator::Less=>VectorExpr::Lt(left,right),
                BinaryOperator::LessEqual=>VectorExpr::LtEq(left,right),
                BinaryOperator::Greater=>VectorExpr::Gt(left,right),
                BinaryOperator::GreaterEqual=>VectorExpr::GtEq(left,right),
                _=>return Err(query_error(AqlErrorCode::InvalidQuery,
                    "vectorized predicates currently support comparisons and boolean operators only",expression.span)),
            })
        }
        ExprKind::Cast{..}=>Err(query_error(AqlErrorCode::InvalidQuery,
            "vectorized CAST is not implemented yet",expression.span)),
    }
}
fn query_error(code:AqlErrorCode,message:impl Into<String>,span:crate::Span)->AqlExecutionError{
    AqlExecutionError{code,message:message.into(),span}
}
fn parse_error(error:crate::AqlError)->AqlExecutionError{
    query_error(AqlErrorCode::InvalidQuery,error.message,error.span)
}
fn storage_error(error:ananta_core::DbError)->AqlExecutionError{
    let code=match &error{
        ananta_core::DbError::TableNotFound(_)=>AqlErrorCode::TableNotFound,
        ananta_core::DbError::ColumnNotFound(_)=>AqlErrorCode::ColumnNotFound,
        _=>AqlErrorCode::Storage,
    };
    query_error(code,error.to_string(),crate::Span::default())
}
fn vector_error(error:ExecutionError)->AqlExecutionError{
    let code=match &error{
        ExecutionError::Cancelled=>AqlErrorCode::Cancelled,
        ExecutionError::TypeMismatch{..}|ExecutionError::NullNotAllowed(_)=>AqlErrorCode::TypeMismatch,
        ExecutionError::Arithmetic(_)=>AqlErrorCode::Arithmetic,
        ExecutionError::Storage(_)=>AqlErrorCode::Storage,
        _=>AqlErrorCode::InvalidQuery,
    };
    query_error(code,error.to_string(),crate::Span::default())
}

#[cfg(test)]
mod tests{
    use super::*;
    use ananta_types::Schema;
    use std::fs;
    use std::time::{SystemTime,UNIX_EPOCH};

    #[test]
    fn vectorized_aql_uses_projection_pushdown_filter_and_limit(){
        let n=SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        let root=std::env::temp_dir().join(format!("ananta-vectorized-aql-{n}"));
        let mut db=Database::open(&root).unwrap();
        db.create_table("telemetry",Schema::new(vec![
            Column::new("time",DataType::TimestampMicros,false),
            Column::new("device",DataType::Utf8,false),
            Column::new("reading",DataType::Float64,true),
            Column::new("active",DataType::Boolean,true),
        ]).unwrap()).unwrap();
        let rows=vec![
            vec![Value::TimestampMicros(1),Value::Utf8("a".into()),Value::Float64(10.0),Value::Boolean(true)],
            vec![Value::TimestampMicros(2),Value::Utf8("b".into()),Value::Float64(30.0),Value::Null],
            vec![Value::TimestampMicros(3),Value::Utf8("c".into()),Value::Float64(40.0),Value::Boolean(true)],
            vec![Value::TimestampMicros(4),Value::Utf8("d".into()),Value::Float64(50.0),Value::Boolean(false)],
        ];
        db.insert_batch("telemetry",&rows).unwrap();
        let query=crate::parse("SELECT device AS sensor FROM telemetry WHERE reading >= :min AND active = TRUE LIMIT 1").unwrap();
        let bindings=ParameterBindings{
            named:[("min".into(),Value::Float64(20.0))].into_iter().collect(),positional:vec![],
        };
        let pipeline=execute_vectorized(&db,&query,&bindings,2,ResourceLimits::default(),Arc::new(AtomicBool::new(false))).unwrap();
        assert_eq!(pipeline.schema()[0].name,"sensor");
        let output=pipeline.collect::<Result<Vec<_>,_>>().unwrap();
        assert_eq!(output.iter().flat_map(|batch|batch.rows()).collect::<Vec<_>>(),
            vec![vec![Value::Utf8("c".into())]]);
        drop(db);fs::remove_dir_all(root).unwrap();
    }
}
