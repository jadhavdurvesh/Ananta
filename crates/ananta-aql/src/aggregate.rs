use crate::vector::{ExecutionContext, ExecutionError, ResourceLimits, VectorBatch};
use ananta_types::{Column, DataType, Value};
use std::cmp::Ordering;
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateKind { CountAll, Count, Sum, Min, Max, Avg }

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AggregateSpec {
    pub kind: AggregateKind,
    /// None is valid only for COUNT(*).
    pub column: Option<usize>,
    pub output_name: String,
}
impl AggregateSpec {
    pub fn count_all(name: impl Into<String>) -> Self {
        Self { kind: AggregateKind::CountAll, column: None, output_name: name.into() }
    }
    pub fn new(kind: AggregateKind, column: usize, name: impl Into<String>) -> Self {
        Self { kind, column: Some(column), output_name: name.into() }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct AggregateResult {
    pub schema: Vec<Column>,
    pub columns: Vec<Vec<Value>>,
    pub row_count: usize,
}
impl AggregateResult {
    pub fn rows(&self) -> impl Iterator<Item=Vec<Value>> + '_ {
        (0..self.row_count).map(|row| self.columns.iter().map(|column|column[row].clone()).collect())
    }
    pub fn estimated_bytes(&self) -> usize {
        self.columns.iter().flatten().fold(0usize,|total,value|total.saturating_add(value_bytes(value)))
    }
}

#[derive(Debug, Clone, PartialEq)]
struct GroupKey(Vec<Value>);
impl Eq for GroupKey {}
impl PartialOrd for GroupKey {
    fn partial_cmp(&self, other:&Self)->Option<Ordering>{Some(self.cmp(other))}
}
impl Ord for GroupKey {
    fn cmp(&self,other:&Self)->Ordering{
        for (a,b) in self.0.iter().zip(&other.0){
            let order=key_value_cmp(a,b);
            if order!=Ordering::Equal{return order;}
        }
        self.0.len().cmp(&other.0.len())
    }
}

#[derive(Debug, Clone)]
enum State {
    Count(u64),
    SumI32(i64),
    SumI64(i64),
    SumF64 { sum:f64, compensation:f64 },
    Average { sum:f64, compensation:f64, count:u64 },
    Extremum(Option<Value>),
}

impl State {
    fn new(spec:&AggregateSpec,input_schema:&[Column])->Result<Self,ExecutionError>{
        match spec.kind{
            AggregateKind::CountAll|AggregateKind::Count=>Ok(Self::Count(0)),
            AggregateKind::Sum=>{
                let index=spec.column.ok_or(ExecutionError::InvalidExpression("SUM requires a column"))?;
                match input_schema.get(index).ok_or(ExecutionError::InvalidExpression("aggregate column index out of range"))?.data_type{
                    DataType::Int32=>Ok(Self::SumI32(0)),
                    DataType::Int64=>Ok(Self::SumI64(0)),
                    DataType::Float64=>Ok(Self::SumF64{sum:0.0,compensation:0.0}),
                    _=>Err(ExecutionError::InvalidExpression("SUM requires a numeric column")),
                }
            }
            AggregateKind::Avg=>{
                let index=spec.column.ok_or(ExecutionError::InvalidExpression("AVG requires a column"))?;
                let kind=input_schema.get(index).ok_or(ExecutionError::InvalidExpression("aggregate column index out of range"))?.data_type;
                if !matches!(kind,DataType::Int32|DataType::Int64|DataType::Float64){
                    return Err(ExecutionError::InvalidExpression("AVG requires a numeric column"));
                }
                Ok(Self::Average{sum:0.0,compensation:0.0,count:0})
            }
            AggregateKind::Min|AggregateKind::Max=>{
                let index=spec.column.ok_or(ExecutionError::InvalidExpression("MIN/MAX requires a column"))?;
                if index>=input_schema.len(){return Err(ExecutionError::InvalidExpression("aggregate column index out of range"));}
                Ok(Self::Extremum(None))
            }
        }
    }
    fn update(&mut self,kind:AggregateKind,value:Option<&Value>)->Result<(),ExecutionError>{
        match (self,kind){
            (State::Count(count),AggregateKind::CountAll)=>*count=count.checked_add(1).ok_or(ExecutionError::Arithmetic("COUNT overflow"))?,
            (State::Count(count),AggregateKind::Count)=>{
                if value.is_some_and(|v|!matches!(v,Value::Null)){
                    *count=count.checked_add(1).ok_or(ExecutionError::Arithmetic("COUNT overflow"))?;
                }
            }
            (State::SumI32(sum),AggregateKind::Sum)=>{
                if let Some(Value::Int32(v))=value.filter(|v|!matches!(v,Value::Null)){
                    *sum=sum.checked_add(i64::from(*v)).ok_or(ExecutionError::Arithmetic("INT32 SUM overflow"))?;
                    if i32::try_from(*sum).is_err(){return Err(ExecutionError::Arithmetic("INT32 SUM overflow"));}
                }
            }
            (State::SumI64(sum),AggregateKind::Sum)=>{
                if let Some(Value::Int64(v))=value.filter(|v|!matches!(v,Value::Null)){
                    *sum=sum.checked_add(*v).ok_or(ExecutionError::Arithmetic("INT64 SUM overflow"))?;
                }
            }
            (State::SumF64{sum,compensation},AggregateKind::Sum)=>{
                if let Some(Value::Float64(v))=value.filter(|v|!matches!(v,Value::Null)){
                    if !v.is_finite(){return Err(ExecutionError::Arithmetic("SUM received a non-finite FLOAT64"));}
                    kahan_add(sum,compensation,*v)?;
                }
            }
            (State::Average{sum,compensation,count},AggregateKind::Avg)=>{
                if let Some(value)=value.filter(|v|!matches!(v,Value::Null)){
                    let number=as_f64(value)?;
                    if !number.is_finite(){return Err(ExecutionError::Arithmetic("AVG received a non-finite FLOAT64"));}
                    kahan_add(sum,compensation,number)?;
                    *count=count.checked_add(1).ok_or(ExecutionError::Arithmetic("AVG count overflow"))?;
                }
            }
            (State::Extremum(current),AggregateKind::Min|AggregateKind::Max)=>{
                if let Some(value)=value.filter(|v|!matches!(v,Value::Null)){
                    if let Value::Float64(v)=value {
                        if !v.is_finite(){return Err(ExecutionError::Arithmetic("MIN/MAX received a non-finite FLOAT64"));}
                    }
                    let replace=match current{
                        None=>true,
                        Some(old)=>{let order=value_cmp(value,old)?;
                            if kind==AggregateKind::Min{order.is_lt()}else{order.is_gt()}}
                    };
                    if replace{*current=Some(value.clone());}
                }
            }
            _=>return Err(ExecutionError::InvalidExpression("aggregate state/type mismatch")),
        }
        Ok(())
    }
    fn finish(&self,kind:AggregateKind)->Result<Value,ExecutionError>{
        match (self,kind){
            (State::Count(value),_)=>Ok(Value::Int64(i64::try_from(*value).map_err(|_|ExecutionError::Arithmetic("COUNT exceeds INT64"))?)),
            (State::SumI32(value),AggregateKind::Sum)=>Ok(Value::Int32(i32::try_from(*value).map_err(|_|ExecutionError::Arithmetic("INT32 SUM overflow"))?)),
            (State::SumI64(value),AggregateKind::Sum)=>Ok(Value::Int64(*value)),
            (State::SumF64{sum,..},AggregateKind::Sum)=>Ok(Value::Float64(*sum)),
            (State::Average{sum,count,..},AggregateKind::Avg)=>{
                if *count==0{Ok(Value::Null)}else{
                    let result=*sum/(*count as f64);
                    if !result.is_finite(){Err(ExecutionError::Arithmetic("AVG result is non-finite"))}else{Ok(Value::Float64(result))}
                }
            }
            (State::Extremum(value),AggregateKind::Min|AggregateKind::Max)=>Ok(value.clone().unwrap_or(Value::Null)),
            _=>Err(ExecutionError::InvalidExpression("aggregate finalisation/type mismatch")),
        }
    }
}

fn kahan_add(sum:&mut f64,compensation:&mut f64,value:f64)->Result<(),ExecutionError>{
    let adjusted=value-*compensation;
    let next=*sum+adjusted;
    *compensation=(next-*sum)-adjusted;
    *sum=next;
    if !sum.is_finite(){return Err(ExecutionError::Arithmetic("floating aggregate overflow"));}
    Ok(())
}
fn as_f64(value:&Value)->Result<f64,ExecutionError>{
    match value{
        Value::Int32(v)=>Ok(f64::from(*v)),
        Value::Int64(v)=>{
            let f=*v as f64;
            if f as i128 != *v as i128{return Err(ExecutionError::Arithmetic("INT64 is not exactly representable as FLOAT64 for AVG"));}
            Ok(f)
        }
        Value::Float64(v)=>Ok(*v),
        _=>Err(ExecutionError::InvalidExpression("expected numeric aggregate input")),
    }
}
fn key_value_cmp(a:&Value,b:&Value)->Ordering{
    match (a,b){
        (Value::Null,Value::Null)=>Ordering::Equal,
        (Value::Null,_)=>Ordering::Less,
        (_,Value::Null)=>Ordering::Greater,
        (Value::Boolean(x),Value::Boolean(y))=>x.cmp(y),
        (Value::Int32(x),Value::Int32(y))=>x.cmp(y),
        (Value::Int64(x),Value::Int64(y))=>x.cmp(y),
        (Value::Float64(x),Value::Float64(y))=>x.total_cmp(y),
        (Value::Utf8(x),Value::Utf8(y))=>x.cmp(y),
        (Value::TimestampMicros(x),Value::TimestampMicros(y))=>x.cmp(y),
        (Value::Binary(x),Value::Binary(y))=>x.cmp(y),
        _=>format!("{a:?}").cmp(&format!("{b:?}")),
    }
}
fn value_cmp(a:&Value,b:&Value)->Result<Ordering,ExecutionError>{
    if a.data_type()!=b.data_type(){return Err(ExecutionError::TypeMismatch{
        expected:b.data_type().unwrap_or(DataType::Int64),actual:a.data_type().unwrap_or(DataType::Int64)
    });}
    Ok(key_value_cmp(a,b))
}
fn value_bytes(value:&Value)->usize{
    match value{Value::Utf8(v)=>v.len(),Value::Binary(v)=>v.len(),_=>16}
}
fn result_schema(input:&[Column],group_by:&[usize],specs:&[AggregateSpec])->Result<Vec<Column>,ExecutionError>{
    let mut schema=Vec::with_capacity(group_by.len()+specs.len());
    for index in group_by{
        schema.push(input.get(*index).cloned().ok_or(ExecutionError::InvalidExpression("group key index out of range"))?);
    }
    for spec in specs{
        let (kind,nullable)=match spec.kind{
            AggregateKind::Count|AggregateKind::CountAll=>(DataType::Int64,false),
            AggregateKind::Avg=>(DataType::Float64,true),
            AggregateKind::Sum|AggregateKind::Min|AggregateKind::Max=>{
                let column=input.get(spec.column.ok_or(ExecutionError::InvalidExpression("aggregate requires input column"))?)
                    .ok_or(ExecutionError::InvalidExpression("aggregate column index out of range"))?;
                (column.data_type,true)
            }
        };
        schema.push(Column::new(spec.output_name.clone(),kind,nullable));
    }
    Ok(schema)
}
fn make_states(specs:&[AggregateSpec],input:&[Column])->Result<Vec<State>,ExecutionError>{
    specs.iter().map(|spec|State::new(spec,input)).collect()
}

/// Aggregate an arbitrary stream of owned vector batches. Group output order is
/// deterministic by typed key. Empty ungrouped input still returns one row.
pub fn aggregate_batches<I>(
    batches:I,
    input_schema:&[Column],
    group_by:&[usize],
    specs:&[AggregateSpec],
    context:&ExecutionContext,
)->Result<AggregateResult,ExecutionError>
where I:IntoIterator<Item=Result<VectorBatch,ExecutionError>>{
    context.limits.validate()?;
    if specs.is_empty(){return Err(ExecutionError::InvalidExpression("at least one aggregate is required"));}
    let output_schema=result_schema(input_schema,group_by,specs)?;
    let mut groups:BTreeMap<GroupKey,Vec<State>>=BTreeMap::new();
    let mut state_bytes=0usize;
    for batch_result in batches{
        context.check_cancelled()?;
        let batch=batch_result?;
        context.limits.check_batch(&batch)?;
        if batch.schema!=input_schema{return Err(ExecutionError::InvalidBatch("aggregate input schema changed between batches".into()));}
        for row in 0..batch.row_count{
            if row%context.limits.max_work_unit_rows==0{context.check_cancelled()?;}
            let key=GroupKey(group_by.iter().map(|index|batch.columns.get(*index)
                .and_then(|column|column.value(row)).cloned()
                .ok_or_else(||ExecutionError::InvalidBatch("group key index out of range".into())))
                .collect::<Result<Vec<_>,_>>()?);
            if let Some(Value::Float64(value))=key.0.iter().find(|v|matches!(v,Value::Float64(_))){
                if !value.is_finite(){return Err(ExecutionError::InvalidExpression("NaN/infinite group keys are not supported"));}
            }
            let is_new=!groups.contains_key(&key);
            if is_new{
                if groups.len()>=context.limits.max_groups{return Err(ExecutionError::ResourceLimit("aggregate group count exceeded"));}
                let extra=key.0.iter().map(value_bytes).sum::<usize>()
                    .saturating_add(specs.len().saturating_mul(64));
                state_bytes=state_bytes.saturating_add(extra);
                if state_bytes>context.limits.max_aggregate_bytes{return Err(ExecutionError::ResourceLimit("aggregate state memory budget exceeded"));}
                groups.insert(key.clone(),make_states(specs,input_schema)?);
            }
            let states=groups.get_mut(&key).expect("group inserted");
            for (state,spec) in states.iter_mut().zip(specs){
                let value=spec.column.and_then(|index|batch.columns.get(index)).and_then(|column|column.value(row));
                state.update(spec.kind,value)?;
            }
        }
    }
    if group_by.is_empty() && groups.is_empty(){
        groups.insert(GroupKey(Vec::new()),make_states(specs,input_schema)?);
    }
    let row_count=groups.len();
    let mut columns=vec![Vec::with_capacity(row_count);output_schema.len()];
    for (key,states) in groups{
        for (index,value) in key.0.into_iter().enumerate(){columns[index].push(value);}
        for (index,(state,spec)) in states.iter().zip(specs).enumerate(){
            columns[group_by.len()+index].push(state.finish(spec.kind)?);
        }
    }
    let estimated=columns.iter().flatten().fold(0usize,|total,value|total.saturating_add(value_bytes(value)));
    if estimated>context.limits.max_aggregate_bytes{return Err(ExecutionError::ResourceLimit("aggregate output memory budget exceeded"));}
    Ok(AggregateResult{schema:output_schema,columns,row_count})
}

#[cfg(test)]
mod tests{
    use super::*;
    use crate::vector::Vector;

    fn batch(rows:&[[Value;3]])->VectorBatch{
        let schema=vec![
            Column::new("group",DataType::Utf8,true),
            Column::new("amount",DataType::Int64,true),
            Column::new("float",DataType::Float64,true),
        ];
        let columns=(0..3).map(|index|{
            Vector::new(schema[index].clone(),rows.iter().map(|row|row[index].clone()).collect()).unwrap()
        }).collect();
        VectorBatch::try_new(columns,rows.len()).unwrap()
    }
    #[test]
    fn grouped_count_sum_min_max_avg_ignore_nulls_and_order_groups(){
        let input=batch(&[
            [Value::Utf8("b".into()),Value::Int64(5),Value::Float64(2.0)],
            [Value::Utf8("a".into()),Value::Null,Value::Float64(4.0)],
            [Value::Utf8("b".into()),Value::Int64(7),Value::Null],
            [Value::Utf8("a".into()),Value::Int64(3),Value::Float64(8.0)],
        ]);
        let specs=vec![
            AggregateSpec::count_all("rows"),
            AggregateSpec::new(AggregateKind::Count,1,"nonnull"),
            AggregateSpec::new(AggregateKind::Sum,1,"total"),
            AggregateSpec::new(AggregateKind::Min,1,"minimum"),
            AggregateSpec::new(AggregateKind::Max,1,"maximum"),
            AggregateSpec::new(AggregateKind::Avg,2,"average"),
        ];
        let context=ExecutionContext::default();
        let result=aggregate_batches(vec![Ok(input.clone())],&input.schema,&[0],&specs,&context).unwrap();
        let rows=result.rows().collect::<Vec<_>>();
        assert_eq!(rows,vec![
            vec![Value::Utf8("a".into()),Value::Int64(2),Value::Int64(1),Value::Int64(3),Value::Int64(3),Value::Int64(3),Value::Float64(6.0)],
            vec![Value::Utf8("b".into()),Value::Int64(2),Value::Int64(2),Value::Int64(12),Value::Int64(5),Value::Int64(7),Value::Float64(2.0)],
        ]);
    }
    #[test]
    fn empty_ungrouped_aggregate_returns_count_zero_and_null_sum_avg(){
        let input=batch(&[]);
        let specs=vec![
            AggregateSpec::count_all("count"),
            AggregateSpec::new(AggregateKind::Sum,1,"sum"),
            AggregateSpec::new(AggregateKind::Avg,2,"avg"),
        ];
        let result=aggregate_batches(Vec::<Result<VectorBatch,ExecutionError>>::new(),
            &input.schema,&[],&specs,&ExecutionContext::default()).unwrap();
        assert_eq!(result.rows().next().unwrap(),vec![Value::Int64(0),Value::Null,Value::Null]);
    }
    #[test]
    fn group_and_memory_limits_fail_without_dropping_rows(){
        let input=batch(&[
            [Value::Utf8("a".into()),Value::Int64(1),Value::Float64(1.0)],
            [Value::Utf8("b".into()),Value::Int64(2),Value::Float64(2.0)],
        ]);
        let mut limits=ResourceLimits::default();limits.max_groups=1;
        let context=ExecutionContext::new(limits).unwrap();
        let error=aggregate_batches(vec![Ok(input.clone())],&input.schema,&[0],
            &[AggregateSpec::count_all("n")],&context).unwrap_err();
        assert!(matches!(error,ExecutionError::ResourceLimit(_)));
    }
}
