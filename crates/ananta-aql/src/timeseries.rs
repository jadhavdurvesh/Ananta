use crate::aggregate::{aggregate_batches, AggregateResult, AggregateSpec};
use crate::vector::{ExecutionContext, ExecutionError, Vector, VectorBatch};
use ananta_types::{Column, DataType, Value};
use std::cmp::Ordering;
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimeRange {
    pub lower_inclusive: Option<i64>,
    pub upper_exclusive: Option<i64>,
}
impl TimeRange {
    pub fn new(lower_inclusive:Option<i64>,upper_exclusive:Option<i64>)->Result<Self,ExecutionError>{
        if let (Some(lower),Some(upper))=(lower_inclusive,upper_exclusive){
            if lower>upper{return Err(ExecutionError::InvalidExpression("time range lower bound exceeds upper bound"));}
        }
        Ok(Self{lower_inclusive,upper_exclusive})
    }
    pub fn contains(&self,timestamp:i64)->bool{
        self.lower_inclusive.map_or(true,|lower|timestamp>=lower)
            && self.upper_exclusive.map_or(true,|upper|timestamp<upper)
    }
}

/// Fixed-width UTC bucket start aligned to origin. Euclidean division is used
/// deliberately so timestamps before 1970 align toward negative infinity.
pub fn time_bucket_start(timestamp_micros:i64,width_micros:i64,origin_micros:i64)->Result<i64,ExecutionError>{
    if width_micros<=0{return Err(ExecutionError::InvalidExpression("time bucket width must be positive"));}
    let delta=timestamp_micros.checked_sub(origin_micros)
        .ok_or(ExecutionError::Arithmetic("timestamp-origin overflow"))?;
    let bucket=delta.div_euclid(width_micros);
    let offset=bucket.checked_mul(width_micros)
        .ok_or(ExecutionError::Arithmetic("time bucket multiplication overflow"))?;
    origin_micros.checked_add(offset)
        .ok_or(ExecutionError::Arithmetic("time bucket start overflow"))
}

pub fn filter_time_range(
    batch:&VectorBatch,
    timestamp_column:usize,
    range:TimeRange,
)->Result<VectorBatch,ExecutionError>{
    let column=batch.columns.get(timestamp_column)
        .ok_or(ExecutionError::InvalidExpression("timestamp column index out of range"))?;
    if column.field.data_type!=DataType::TimestampMicros{
        return Err(ExecutionError::TypeMismatch{expected:DataType::TimestampMicros,actual:column.field.data_type});
    }
    let mut selected=Vec::with_capacity(batch.row_count);
    for row in 0..batch.row_count{
        match column.value(row){
            Some(Value::TimestampMicros(value)) if range.contains(*value)=>selected.push(row),
            Some(Value::Null)=>{},
            Some(Value::TimestampMicros(_))=>{},
            _=>return Err(ExecutionError::InvalidExpression("invalid timestamp vector value")),
        }
    }
    batch.take_rows(&selected)
}

/// Add a bucket-start column, then reuse the standard deterministic grouped
/// aggregate implementation. Empty intervals are not synthesized.
pub fn bucketed_aggregate<I>(
    batches:I,
    input_schema:&[Column],
    timestamp_column:usize,
    width_micros:i64,
    origin_micros:i64,
    group_columns:&[usize],
    aggregates:&[AggregateSpec],
    context:&ExecutionContext,
)->Result<AggregateResult,ExecutionError>
where I:IntoIterator<Item=Result<VectorBatch,ExecutionError>>{
    let timestamp=input_schema.get(timestamp_column)
        .ok_or(ExecutionError::InvalidExpression("timestamp column index out of range"))?;
    if timestamp.data_type!=DataType::TimestampMicros{
        return Err(ExecutionError::TypeMismatch{expected:DataType::TimestampMicros,actual:timestamp.data_type});
    }
    if width_micros<=0{return Err(ExecutionError::InvalidExpression("time bucket width must be positive"));}
    let mut schema=input_schema.to_vec();
    schema.push(Column::new("_aql_bucket_start",DataType::TimestampMicros,false));
    let bucket_index=input_schema.len();
    let mut transformed=Vec::new();
    for batch in batches{
        context.check_cancelled()?;
        let batch=batch?;
        let time=&batch.columns[timestamp_column];
        let mut buckets=Vec::with_capacity(batch.row_count);
        for value in time.values.iter(){
            match value{
                Value::TimestampMicros(timestamp)=>buckets.push(Value::TimestampMicros(
                    time_bucket_start(*timestamp,width_micros,origin_micros)?)),
                Value::Null=>return Err(ExecutionError::InvalidExpression("time bucketing does not accept NULL timestamps")),
                _=>return Err(ExecutionError::InvalidExpression("invalid timestamp value")),
            }
        }
        let mut columns=batch.columns.clone();
        columns.push(Vector::new(schema[bucket_index].clone(),buckets)?);
        transformed.push(Ok(VectorBatch::try_new(columns,batch.row_count)?));
    }
    let mut groups=group_columns.to_vec();
    groups.push(bucket_index);
    aggregate_batches(transformed.into_iter(),&schema,&groups,aggregates,context)
}

#[derive(Debug,Clone)]
struct SeriesKey(Vec<Value>);
impl PartialEq for SeriesKey{fn eq(&self,other:&Self)->bool{self.cmp(other)==Ordering::Equal}}
impl Eq for SeriesKey{}
impl PartialOrd for SeriesKey{fn partial_cmp(&self,other:&Self)->Option<Ordering>{Some(self.cmp(other))}}
impl Ord for SeriesKey{
    fn cmp(&self,other:&Self)->Ordering{
        for (left,right) in self.0.iter().zip(&other.0){
            let order=key_compare(left,right);
            if order!=Ordering::Equal{return order;}
        }
        self.0.len().cmp(&other.0.len())
    }
}
fn key_compare(a:&Value,b:&Value)->Ordering{
    match (a,b){
        (Value::Null,Value::Null)=>Ordering::Equal,(Value::Null,_)=>Ordering::Less,(_,Value::Null)=>Ordering::Greater,
        (Value::Boolean(x),Value::Boolean(y))=>x.cmp(y),
        (Value::Int32(x),Value::Int32(y))=>x.cmp(y),(Value::Int64(x),Value::Int64(y))=>x.cmp(y),
        (Value::Float64(x),Value::Float64(y))=>x.total_cmp(y),
        (Value::Utf8(x),Value::Utf8(y))=>x.cmp(y),
        (Value::TimestampMicros(x),Value::TimestampMicros(y))=>x.cmp(y),
        (Value::Binary(x),Value::Binary(y))=>x.cmp(y),
        _=>format!("{a:?}").cmp(&format!("{b:?}")),
    }
}

/// Select the maximum event-time row per series. Equal timestamps choose the
/// later input row. Older late arrivals never replace a newer event.
pub fn latest_per_series<I>(
    batches:I,
    input_schema:&[Column],
    series_columns:&[usize],
    timestamp_column:usize,
    context:&ExecutionContext,
)->Result<VectorBatch,ExecutionError>
where I:IntoIterator<Item=Result<VectorBatch,ExecutionError>>{
    let timestamp=input_schema.get(timestamp_column)
        .ok_or(ExecutionError::InvalidExpression("timestamp column index out of range"))?;
    if timestamp.data_type!=DataType::TimestampMicros{
        return Err(ExecutionError::TypeMismatch{expected:DataType::TimestampMicros,actual:timestamp.data_type});
    }
    if series_columns.iter().any(|index|*index>=input_schema.len()){
        return Err(ExecutionError::InvalidExpression("series key column index out of range"));
    }
    let mut latest:BTreeMap<SeriesKey,(i64,u64,Vec<Value>)>=BTreeMap::new();
    let mut ordinal=0_u64;
    let mut retained_bytes=0usize;
    for batch in batches{
        context.check_cancelled()?;
        let batch=batch?;
        context.limits.check_batch(&batch)?;
        if batch.schema!=input_schema{return Err(ExecutionError::InvalidBatch("latest-per-series input schema changed".into()));}
        for row in 0..batch.row_count{
            if row%context.limits.max_work_unit_rows==0{context.check_cancelled()?;}
            let timestamp=match batch.columns[timestamp_column].value(row){
                Some(Value::TimestampMicros(value))=>*value,
                Some(Value::Null)=>{ordinal=ordinal.saturating_add(1);continue;}
                _=>return Err(ExecutionError::InvalidExpression("invalid event timestamp value")),
            };
            let values=batch.columns.iter().map(|column|column.values[row].clone()).collect::<Vec<_>>();
            let key=SeriesKey(series_columns.iter().map(|index|values[*index].clone()).collect());
            let replace=latest.get(&key).map_or(true,|(old_timestamp,_,_)|timestamp>=*old_timestamp);
            if replace{
                if !latest.contains_key(&key){
                    if latest.len()>=context.limits.max_groups{return Err(ExecutionError::ResourceLimit("latest-per-series group limit exceeded"));}
                    retained_bytes=retained_bytes.saturating_add(values.iter().map(value_bytes).sum::<usize>());
                    if retained_bytes>context.limits.max_aggregate_bytes{return Err(ExecutionError::ResourceLimit("latest-per-series memory limit exceeded"));}
                }
                latest.insert(key,(timestamp,ordinal,values));
            }
            ordinal=ordinal.saturating_add(1);
        }
    }
    let rows=latest.into_values().map(|(_,_,row)|row).collect::<Vec<_>>();
    let mut columns=Vec::with_capacity(input_schema.len());
    for (index,field) in input_schema.iter().enumerate(){
        columns.push(Vector::new(field.clone(),rows.iter().map(|row|row[index].clone()).collect())?);
    }
    VectorBatch::try_new(columns,rows.len())
}
fn value_bytes(value:&Value)->usize{
    match value{Value::Utf8(v)=>v.len(),Value::Binary(v)=>v.len(),_=>16}
}

#[cfg(test)]
mod tests{
    use super::*;
    use crate::aggregate::{AggregateKind,AggregateSpec};
    use crate::vector::{ResourceLimits,Vector};

    fn input()->VectorBatch{
        let schema=vec![
            Column::new("series",DataType::Utf8,false),
            Column::new("time",DataType::TimestampMicros,false),
            Column::new("value",DataType::Float64,true),
        ];
        let rows=vec![
            vec![Value::Utf8("a".into()),Value::TimestampMicros(1),Value::Float64(2.0)],
            vec![Value::Utf8("b".into()),Value::TimestampMicros(11),Value::Float64(4.0)],
            vec![Value::Utf8("a".into()),Value::TimestampMicros(9),Value::Float64(6.0)],
            vec![Value::Utf8("a".into()),Value::TimestampMicros(3),Value::Float64(1.0)],
            vec![Value::Utf8("b".into()),Value::TimestampMicros(11),Value::Float64(8.0)],
        ];
        let columns=(0..3).map(|i|Vector::new(schema[i].clone(),rows.iter().map(|row|row[i].clone()).collect()).unwrap()).collect();
        VectorBatch::try_new(columns,rows.len()).unwrap()
    }
    #[test]
    fn buckets_align_pre_epoch_and_respect_boundaries(){
        assert_eq!(time_bucket_start(-1,10,0).unwrap(),-10);
        assert_eq!(time_bucket_start(-10,10,0).unwrap(),-10);
        assert_eq!(time_bucket_start(10,10,0).unwrap(),10);
        assert_eq!(time_bucket_start(9,10,3).unwrap(),3);
        assert!(time_bucket_start(1,0,0).is_err());
        assert!(time_bucket_start(i64::MAX,2,0).is_err());
    }
    #[test]
    fn range_is_lower_inclusive_upper_exclusive_and_null_safe(){
        let batch=input();
        let range=TimeRange::new(Some(3),Some(11)).unwrap();
        let filtered=filter_time_range(&batch,1,range).unwrap();
        assert_eq!(filtered.row_count,2);
        assert!(TimeRange::new(Some(11),Some(3)).is_err());
    }
    #[test]
    fn bucketed_aggregate_uses_utc_bucket_start_as_group_key(){
        let batch=input();
        let result=bucketed_aggregate(vec![Ok(batch.clone())],&batch.schema,1,10,0,&[0],
            &[AggregateSpec::new(AggregateKind::Count,2,"samples")],&ExecutionContext::default()).unwrap();
        let rows=result.rows().collect::<Vec<_>>();
        assert_eq!(rows,vec![
            vec![Value::Utf8("a".into()),Value::TimestampMicros(0),Value::Int64(3)],
            vec![Value::Utf8("b".into()),Value::TimestampMicros(10),Value::Int64(2)],
        ]);
    }
    #[test]
    fn latest_per_series_ignores_late_arrivals_and_uses_later_tie(){
        let batch=input();
        let latest=latest_per_series(vec![Ok(batch.clone())],&batch.schema,&[0],1,&ExecutionContext::default()).unwrap();
        let rows=latest.rows().collect::<Vec<_>>();
        assert_eq!(rows,vec![
            vec![Value::Utf8("a".into()),Value::TimestampMicros(9),Value::Float64(6.0)],
            vec![Value::Utf8("b".into()),Value::TimestampMicros(11),Value::Float64(8.0)],
        ]);
    }
    #[test]
    fn latest_per_series_enforces_cardinality_limit(){
        let batch=input();
        let mut limits=ResourceLimits::default();limits.max_groups=1;
        let context=ExecutionContext::new(limits).unwrap();
        assert!(matches!(latest_per_series(vec![Ok(batch.clone())],&batch.schema,&[0],1,&context),
            Err(ExecutionError::ResourceLimit(_))));
    }
}
