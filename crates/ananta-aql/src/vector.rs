use ananta_core::{CapsuleScanDiagnostics, CapsuleScanRequest, ColumnRangePredicate, Database, DbError, RecordBatch, ScanBatchIter};
use ananta_types::{Column, DataType, Value};
use std::cmp::Ordering;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex};

pub const DEFAULT_MAX_BATCH_ROWS: usize = 65_536;
pub const DEFAULT_MAX_BATCH_CELLS: usize = 1_000_000;
pub const DEFAULT_MAX_BATCH_BYTES: usize = 64 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq)]
pub struct Vector {
    pub field: Column,
    pub values: Arc<[Value]>,
}
impl Vector {
    pub fn new(field: Column, values: Vec<Value>) -> Result<Self, ExecutionError> {
        for value in &values {
            if let Some(actual) = value.data_type() {
                if actual != field.data_type {
                    return Err(ExecutionError::TypeMismatch {
                        expected: field.data_type, actual,
                    });
                }
            } else if !field.nullable {
                return Err(ExecutionError::NullNotAllowed(field.name.clone()));
            }
        }
        Ok(Self { field, values: Arc::from(values) })
    }
    pub fn len(&self) -> usize { self.values.len() }
    pub fn is_empty(&self) -> bool { self.values.is_empty() }
    pub fn value(&self, row: usize) -> Option<&Value> { self.values.get(row) }
    pub fn estimated_bytes(&self) -> usize {
        self.values.iter().fold(self.values.len().saturating_mul(16), |total, value| {
            total.saturating_add(match value {
                Value::Utf8(value) => value.len(),
                Value::Binary(value) => value.len(),
                _ => 0,
            })
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct VectorBatch {
    pub schema: Vec<Column>,
    pub columns: Vec<Vector>,
    pub row_count: usize,
}
impl VectorBatch {
    pub fn try_new(columns: Vec<Vector>, row_count: usize) -> Result<Self, ExecutionError> {
        if columns.is_empty() { return Err(ExecutionError::InvalidBatch("zero-column batch".into())); }
        if columns.iter().any(|column| column.len() != row_count) {
            return Err(ExecutionError::InvalidBatch("vector lengths differ from row_count".into()));
        }
        let cells = row_count.checked_mul(columns.len()).ok_or(ExecutionError::ResourceLimit("batch cell count overflow"))?;
        if row_count > DEFAULT_MAX_BATCH_ROWS || cells > DEFAULT_MAX_BATCH_CELLS {
            return Err(ExecutionError::ResourceLimit("batch rows/cells exceed default bound"));
        }
        let schema = columns.iter().map(|column| column.field.clone()).collect();
        Ok(Self { schema, columns, row_count })
    }
    pub fn empty(schema: Vec<Column>) -> Self {
        let columns = schema.iter().cloned().map(|field| Vector {
            field, values: Arc::from(Vec::<Value>::new()),
        }).collect();
        Self { schema: columns.iter().map(|column| column.field.clone()).collect(), columns, row_count: 0 }
    }
    pub fn estimated_bytes(&self) -> usize {
        self.columns.iter().fold(0_usize, |total, column| total.saturating_add(column.estimated_bytes()))
    }
    pub fn select_columns(&self, indices: &[usize]) -> Result<Self, ExecutionError> {
        if indices.is_empty() { return Err(ExecutionError::InvalidBatch("projection cannot be empty".into())); }
        let mut columns = Vec::with_capacity(indices.len());
        for index in indices {
            columns.push(self.columns.get(*index).cloned()
                .ok_or_else(|| ExecutionError::InvalidBatch(format!("projection index {index} is out of range")))?);
        }
        Self::try_new(columns, self.row_count)
    }
    pub fn select_columns_with_fields(
        &self,
        indices: &[usize],
        fields: &[Column],
    ) -> Result<Self, ExecutionError> {
        if indices.is_empty() || indices.len()!=fields.len() {
            return Err(ExecutionError::InvalidBatch("projection indices and output fields must be non-empty and equal length".into()));
        }
        let mut columns=Vec::with_capacity(indices.len());
        for (index,field) in indices.iter().zip(fields) {
            let source=self.columns.get(*index)
                .ok_or_else(||ExecutionError::InvalidBatch(format!("projection index {index} is out of range")))?;
            if source.field.data_type!=field.data_type {
                return Err(ExecutionError::TypeMismatch{expected:field.data_type,actual:source.field.data_type});
            }
            columns.push(Vector{field:field.clone(),values:Arc::clone(&source.values)});
        }
        Self::try_new(columns,self.row_count)
    }

    pub fn take_rows(&self, indices: &[usize]) -> Result<Self, ExecutionError> {
        let mut columns = Vec::with_capacity(self.columns.len());
        for column in &self.columns {
            let mut values = Vec::with_capacity(indices.len());
            for index in indices {
                values.push(column.values.get(*index).cloned()
                    .ok_or_else(|| ExecutionError::InvalidBatch(format!("row index {index} is out of range")))?);
            }
            columns.push(Vector::new(column.field.clone(), values)?);
        }
        Self::try_new(columns, indices.len())
    }
    pub fn from_record_batch(batch: RecordBatch) -> Result<Self, ExecutionError> {
        let columns = batch.columns.into_iter().map(|column| Vector::new(column.column, column.values))
            .collect::<Result<Vec<_>, _>>()?;
        Self::try_new(columns, batch.row_count)
    }
    pub fn rows(&self) -> impl Iterator<Item = Vec<Value>> + '_ {
        (0..self.row_count).map(|row| self.columns.iter().map(|column| column.values[row].clone()).collect())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResourceLimits {
    pub max_batch_rows: usize,
    pub max_batch_cells: usize,
    pub max_batch_bytes: usize,
    pub max_work_unit_rows: usize,
    pub max_groups: usize,
    pub max_aggregate_bytes: usize,
}
impl Default for ResourceLimits {
    fn default() -> Self {
        Self {
            max_batch_rows: DEFAULT_MAX_BATCH_ROWS,
            max_batch_cells: DEFAULT_MAX_BATCH_CELLS,
            max_batch_bytes: DEFAULT_MAX_BATCH_BYTES,
            max_work_unit_rows: 512,
            max_groups: 1_000_000,
            max_aggregate_bytes: 256 * 1024 * 1024,
        }
    }
}
impl ResourceLimits {
    pub fn validate(&self) -> Result<(), ExecutionError> {
        if self.max_batch_rows == 0 || self.max_batch_rows > DEFAULT_MAX_BATCH_ROWS
            || self.max_batch_cells == 0 || self.max_batch_cells > DEFAULT_MAX_BATCH_CELLS
            || self.max_batch_bytes == 0 || self.max_work_unit_rows == 0
            || self.max_groups == 0 || self.max_aggregate_bytes == 0
        {
            return Err(ExecutionError::InvalidLimits);
        }
        Ok(())
    }
    pub fn check_batch(&self, batch: &VectorBatch) -> Result<(), ExecutionError> {
        self.validate()?;
        let cells = batch.row_count.checked_mul(batch.columns.len())
            .ok_or(ExecutionError::ResourceLimit("batch cell count overflow"))?;
        if batch.row_count > self.max_batch_rows || cells > self.max_batch_cells {
            return Err(ExecutionError::ResourceLimit("batch row/cell budget exceeded"));
        }
        if batch.estimated_bytes() > self.max_batch_bytes {
            return Err(ExecutionError::ResourceLimit("batch byte budget exceeded"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecutionError {
    InvalidBatch(String),
    InvalidLimits,
    ResourceLimit(&'static str),
    Cancelled,
    TypeMismatch { expected: DataType, actual: DataType },
    NullNotAllowed(String),
    InvalidExpression(&'static str),
    Arithmetic(&'static str),
    Storage(String),
}
impl std::fmt::Display for ExecutionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidBatch(message) => write!(f, "invalid vector batch: {message}"),
            Self::InvalidLimits => f.write_str("execution resource limits are invalid"),
            Self::ResourceLimit(message) => write!(f, "execution resource limit: {message}"),
            Self::Cancelled => f.write_str("execution cancelled"),
            Self::TypeMismatch { expected, actual } => write!(f, "expected {expected}, received {actual}"),
            Self::NullNotAllowed(column) => write!(f, "NULL is not allowed in column '{column}'"),
            Self::InvalidExpression(message) => write!(f, "invalid vector expression: {message}"),
            Self::Arithmetic(message) => write!(f, "arithmetic error: {message}"),
            Self::Storage(message) => write!(f, "storage error: {message}"),
        }
    }
}
impl std::error::Error for ExecutionError {}
impl From<DbError> for ExecutionError {
    fn from(value: DbError) -> Self { Self::Storage(value.to_string()) }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ExecutionProfile {
    pub input_batches: u64,
    pub output_batches: u64,
    pub input_rows: u64,
    pub output_rows: u64,
    pub filtered_rows: u64,
    pub decoded_value_bytes_estimate: u64,
    pub output_value_bytes_estimate: u64,
    pub elapsed_micros: u64,
    pub capsules_considered: u64,
    pub capsules_skipped: u64,
    pub capsules_read: u64,
    pub capsule_bytes_verified: u64,
    pub capsule_payload_bytes_decoded: u64,
}
#[derive(Clone)]
pub struct ExecutionContext {
    pub limits: ResourceLimits,
    pub cancellation: Arc<AtomicBool>,
    profile: Arc<Mutex<ExecutionProfile>>,
    started: std::time::Instant,
}
impl Default for ExecutionContext {
    fn default() -> Self {
        Self { limits: ResourceLimits::default(), cancellation: Arc::new(AtomicBool::new(false)),
            profile: Arc::new(Mutex::new(ExecutionProfile::default())), started: std::time::Instant::now() }
    }
}
impl ExecutionContext {
    pub fn new(limits: ResourceLimits) -> Result<Self, ExecutionError> {
        limits.validate()?;
        Ok(Self { limits, ..Self::default() })
    }
    pub fn cancel(&self) { self.cancellation.store(true, AtomicOrdering::Release); }
    pub fn check_cancelled(&self) -> Result<(), ExecutionError> {
        if self.cancellation.load(AtomicOrdering::Acquire) { Err(ExecutionError::Cancelled) } else { Ok(()) }
    }
    pub fn profile(&self) -> ExecutionProfile {
        let mut profile=self.profile.lock().map(|p| p.clone()).unwrap_or_default();
        profile.elapsed_micros=u64::try_from(self.started.elapsed().as_micros()).unwrap_or(u64::MAX);
        profile
    }
    fn record_input(&self, batch: &VectorBatch) {
        if let Ok(mut profile) = self.profile.lock() {
            profile.input_batches = profile.input_batches.saturating_add(1);
            profile.input_rows = profile.input_rows.saturating_add(batch.row_count as u64);
            profile.decoded_value_bytes_estimate = profile.decoded_value_bytes_estimate
                .saturating_add(batch.estimated_bytes() as u64);
        }
    }
    fn record_output(&self, batch: &VectorBatch) {
        if let Ok(mut profile) = self.profile.lock() {
            profile.output_batches = profile.output_batches.saturating_add(1);
            profile.output_rows = profile.output_rows.saturating_add(batch.row_count as u64);
            profile.output_value_bytes_estimate = profile.output_value_bytes_estimate
                .saturating_add(batch.estimated_bytes() as u64);
        }
    }
    fn record_filtered(&self, rows: usize) {
        if let Ok(mut profile) = self.profile.lock() {
            profile.filtered_rows = profile.filtered_rows.saturating_add(rows as u64);
        }
    }
    fn record_capsule_scan(&self, diagnostics:&CapsuleScanDiagnostics) {
        if let Ok(mut profile)=self.profile.lock() {
            profile.capsules_considered=profile.capsules_considered.saturating_add(diagnostics.capsules_considered);
            profile.capsules_skipped=profile.capsules_skipped.saturating_add(diagnostics.capsules_skipped);
            profile.capsules_read=profile.capsules_read.saturating_add(diagnostics.capsules_read);
            profile.capsule_bytes_verified=profile.capsule_bytes_verified.saturating_add(diagnostics.bytes_verified);
            profile.capsule_payload_bytes_decoded=profile.capsule_payload_bytes_decoded.saturating_add(diagnostics.payload_bytes_decoded);
        }
    }
}

pub trait PhysicalOperator: Send {
    fn schema(&self) -> &[Column];
    fn next_batch(&mut self, context: &ExecutionContext) -> Result<Option<VectorBatch>, ExecutionError>;
}

pub struct ScanOperator {
    input: ScanBatchIter,
    schema: Vec<Column>,
    terminal: bool,
}
impl ScanOperator {
    pub fn new(
        database: &Database,
        table: &str,
        columns: &[&str],
        batch_rows: usize,
    ) -> Result<Self, ExecutionError> {
        if columns.is_empty() || batch_rows == 0 {
            return Err(ExecutionError::InvalidLimits);
        }
        let schema = database.schema(table)?.columns().to_vec();
        let projected = columns.iter().map(|name| {
            schema.iter().find(|column| column.name == **name).cloned()
                .ok_or_else(|| ExecutionError::Storage(format!("column not found: {name}")))
        }).collect::<Result<Vec<_>, _>>()?;
        let input = database.scan_columns_batches(table, columns, batch_rows)?;
        Ok(Self { input, schema: projected, terminal: false })
    }
}
impl PhysicalOperator for ScanOperator {
    fn schema(&self) -> &[Column] { &self.schema }
    fn next_batch(&mut self, context: &ExecutionContext) -> Result<Option<VectorBatch>, ExecutionError> {
        if self.terminal { return Ok(None); }
        context.check_cancelled()?;
        match self.input.next() {
            Some(Ok(batch)) => {
                let batch = VectorBatch::from_record_batch(batch)?;
                context.limits.check_batch(&batch)?;
                context.record_input(&batch);
                Ok(Some(batch))
            }
            Some(Err(error)) => {
                self.terminal = true;
                Err(error.into())
            }
            None => { self.terminal = true; Ok(None) }
        }
    }
}


/// Bounded-output adapter for a complete, validated Capsule inventory. The
/// underlying Capsule scan materialises its selected result, so both the
/// accepted source-row count and the estimated selected bytes are checked
/// before the result is split into independently owned output batches.
pub struct CapsuleScanOperator {
    batches:VecDeque<VectorBatch>,
    schema:Vec<Column>,
    diagnostics:CapsuleScanDiagnostics,
    terminal:bool,
}
impl CapsuleScanOperator {
    pub fn new(
        database:&Database,
        table:&str,
        columns:&[&str],
        range:ColumnRangePredicate,
        navigation_map_generation:u64,
        batch_rows:usize,
        max_materialized_rows:u64,
        max_materialized_bytes:usize,
    )->Result<Self,ExecutionError>{
        if columns.is_empty()||batch_rows==0||batch_rows>DEFAULT_MAX_BATCH_ROWS{
            return Err(ExecutionError::InvalidLimits);
        }
        let ingestion=database.ingestion_stats(table)?;
        if ingestion.accepted_rows>max_materialized_rows{
            return Err(ExecutionError::ResourceLimit("Capsule result exceeds materialisation row cap"));
        }
        if !database.capsules_cover_chronicle(table)?{
            return Err(ExecutionError::ResourceLimit("Capsule inventory does not cover the complete Chronicle table"));
        }
        let schema=database.schema(table)?.columns().to_vec();
        let output_schema=columns.iter().map(|name|schema.iter().find(|column|column.name==**name)
            .cloned().ok_or_else(||ExecutionError::Storage(format!("Capsule projection column not found: {name}"))))
            .collect::<Result<Vec<_>,_>>()?;
        let request=CapsuleScanRequest{ranges:vec![range],projection:Some(columns.iter().map(|name|(*name).to_owned()).collect())};
        let scanned=database.scan_capsules(table,&request,Some(navigation_map_generation))?;
        if scanned.rows.len() as u64>max_materialized_rows{
            return Err(ExecutionError::ResourceLimit("Capsule result exceeds materialisation row cap"));
        }
        let estimated=scanned.rows.iter().flatten().fold(0usize,|total,value|{
            total.saturating_add(match value{Value::Utf8(v)=>v.len(),Value::Binary(v)=>v.len(),_=>16})
        });
        if estimated>max_materialized_bytes{
            return Err(ExecutionError::ResourceLimit("Capsule result exceeds materialisation byte cap"));
        }
        let mut batches=VecDeque::new();
        for chunk in scanned.rows.chunks(batch_rows){
            let mut vectors=Vec::with_capacity(output_schema.len());
            for (column_index,field) in output_schema.iter().enumerate(){
                let values=chunk.iter().map(|row|row.get(column_index).cloned()
                    .ok_or_else(||ExecutionError::InvalidBatch("Capsule row width differs from projection".into())))
                    .collect::<Result<Vec<_>,_>>()?;
                vectors.push(Vector::new(field.clone(),values)?);
            }
            batches.push_back(VectorBatch::try_new(vectors,chunk.len())?);
        }
        Ok(Self{batches,schema:output_schema,diagnostics:scanned.diagnostics,terminal:false})
    }
    pub fn diagnostics(&self)->&CapsuleScanDiagnostics{&self.diagnostics}
}
impl PhysicalOperator for CapsuleScanOperator {
    fn schema(&self)->&[Column]{&self.schema}
    fn next_batch(&mut self,context:&ExecutionContext)->Result<Option<VectorBatch>,ExecutionError>{
        if self.terminal{return Ok(None);}
        context.check_cancelled()?;
        if let Some(batch)=self.batches.pop_front(){
            context.limits.check_batch(&batch)?;
            context.record_input(&batch);
            Ok(Some(batch))
        }else{
            self.terminal=true;
            context.record_capsule_scan(&self.diagnostics);
            Ok(None)
        }
    }
}

pub struct ProjectionOperator {
    input: Box<dyn PhysicalOperator>,
    indices: Vec<usize>,
    schema: Vec<Column>,
    terminal: bool,
}
impl ProjectionOperator {
    pub fn new(input: Box<dyn PhysicalOperator>, indices: Vec<usize>) -> Result<Self, ExecutionError> {
        let schema = indices.iter().map(|index| input.schema().get(*index).cloned()
            .ok_or_else(||ExecutionError::InvalidBatch("invalid projection index".into())))
            .collect::<Result<Vec<_>,_>>()?;
        Self::new_with_fields(input,indices,schema)
    }
    pub fn new_with_fields(
        input:Box<dyn PhysicalOperator>,
        indices:Vec<usize>,
        schema:Vec<Column>,
    )->Result<Self,ExecutionError>{
        if indices.is_empty()||indices.len()!=schema.len()
            ||indices.iter().any(|index|*index>=input.schema().len()){
            return Err(ExecutionError::InvalidBatch("invalid projection indices/fields".into()));
        }
        Ok(Self{input,indices,schema,terminal:false})
    }
}
impl PhysicalOperator for ProjectionOperator {
    fn schema(&self) -> &[Column] { &self.schema }
    fn next_batch(&mut self, context: &ExecutionContext) -> Result<Option<VectorBatch>, ExecutionError> {
        if self.terminal { return Ok(None); }
        match self.input.next_batch(context) {
            Ok(Some(batch)) => {
                let output = batch.select_columns_with_fields(&self.indices,&self.schema)?;
                Ok(Some(output))
            }
            Ok(None) => { self.terminal = true; Ok(None) }
            Err(error) => { self.terminal = true; Err(error) }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum VectorExpr {
    Column(usize),
    Literal(Value),
    IsNull(Box<VectorExpr>),
    IsNotNull(Box<VectorExpr>),
    Not(Box<VectorExpr>),
    And(Box<VectorExpr>, Box<VectorExpr>),
    Or(Box<VectorExpr>, Box<VectorExpr>),
    Eq(Box<VectorExpr>, Box<VectorExpr>),
    NotEq(Box<VectorExpr>, Box<VectorExpr>),
    Lt(Box<VectorExpr>, Box<VectorExpr>),
    LtEq(Box<VectorExpr>, Box<VectorExpr>),
    Gt(Box<VectorExpr>, Box<VectorExpr>),
    GtEq(Box<VectorExpr>, Box<VectorExpr>),
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Truth { True, False, Unknown }
fn truth(value: &Value) -> Result<Truth, ExecutionError> {
    match value {
        Value::Boolean(true) => Ok(Truth::True),
        Value::Boolean(false) => Ok(Truth::False),
        Value::Null => Ok(Truth::Unknown),
        _ => Err(ExecutionError::InvalidExpression("predicate must evaluate to BOOLEAN")),
    }
}
fn truth_value(value: Truth) -> Value {
    match value { Truth::True => Value::Boolean(true), Truth::False => Value::Boolean(false), Truth::Unknown => Value::Null }
}
fn eval(expr: &VectorExpr, batch: &VectorBatch, row: usize) -> Result<Value, ExecutionError> {
    match expr {
        VectorExpr::Column(index) => batch.columns.get(*index).and_then(|col| col.value(row)).cloned()
            .ok_or_else(|| ExecutionError::InvalidBatch("expression column index out of range".into())),
        VectorExpr::Literal(value) => Ok(value.clone()),
        VectorExpr::IsNull(expr) => Ok(Value::Boolean(matches!(eval(expr,batch,row)?, Value::Null))),
        VectorExpr::IsNotNull(expr) => Ok(Value::Boolean(!matches!(eval(expr,batch,row)?, Value::Null))),
        VectorExpr::Not(expr) => Ok(match truth(&eval(expr,batch,row)?)? {
            Truth::True => Value::Boolean(false), Truth::False => Value::Boolean(true), Truth::Unknown => Value::Null,
        }),
        VectorExpr::And(left,right) => {
            let a=truth(&eval(left,batch,row)?)?; let b=truth(&eval(right,batch,row)?)?;
            Ok(truth_value(match (a,b) {
                (Truth::False,_)|(_,Truth::False)=>Truth::False,
                (Truth::True,Truth::True)=>Truth::True,_=>Truth::Unknown,
            }))
        }
        VectorExpr::Or(left,right) => {
            let a=truth(&eval(left,batch,row)?)?; let b=truth(&eval(right,batch,row)?)?;
            Ok(truth_value(match (a,b) {
                (Truth::True,_)|(_,Truth::True)=>Truth::True,
                (Truth::False,Truth::False)=>Truth::False,_=>Truth::Unknown,
            }))
        }
        comparison => {
            let (left,right,op)=match comparison {
                VectorExpr::Eq(a,b)=>(a,b,0), VectorExpr::NotEq(a,b)=>(a,b,1),
                VectorExpr::Lt(a,b)=>(a,b,2), VectorExpr::LtEq(a,b)=>(a,b,3),
                VectorExpr::Gt(a,b)=>(a,b,4), VectorExpr::GtEq(a,b)=>(a,b,5),
                _=>unreachable!(),
            };
            let a=eval(left,batch,row)?; let b=eval(right,batch,row)?;
            if matches!(a,Value::Null)||matches!(b,Value::Null){return Ok(Value::Null);}
            let order=compare(&a,&b)?;
            let result=match op {0=>order==Ordering::Equal,1=>order!=Ordering::Equal,2=>order==Ordering::Less,
                3=>order!=Ordering::Greater,4=>order==Ordering::Greater,5=>order!=Ordering::Less,_=>false};
            Ok(Value::Boolean(result))
        }
    }
}
fn compare(a:&Value,b:&Value)->Result<Ordering,ExecutionError>{
    match (a,b) {
        (Value::Boolean(x),Value::Boolean(y))=>Ok(x.cmp(y)),
        (Value::Int32(x),Value::Int32(y))=>Ok(x.cmp(y)),
        (Value::Int64(x),Value::Int64(y))=>Ok(x.cmp(y)),
        (Value::Float64(x),Value::Float64(y))=>Ok(x.total_cmp(y)),
        (Value::Utf8(x),Value::Utf8(y))=>Ok(x.cmp(y)),
        (Value::TimestampMicros(x),Value::TimestampMicros(y))=>Ok(x.cmp(y)),
        (Value::Binary(x),Value::Binary(y))=>Ok(x.cmp(y)),
        _=>Err(ExecutionError::InvalidExpression("comparison operands have different types")),
    }
}

pub struct FilterOperator {
    input: Box<dyn PhysicalOperator>,
    predicate: VectorExpr,
    schema: Vec<Column>,
    terminal: bool,
}
impl FilterOperator {
    pub fn new(input: Box<dyn PhysicalOperator>, predicate: VectorExpr) -> Self {
        let schema=input.schema().to_vec();
        Self{input,predicate,schema,terminal:false}
    }
}
impl PhysicalOperator for FilterOperator {
    fn schema(&self)->&[Column]{&self.schema}
    fn next_batch(&mut self,context:&ExecutionContext)->Result<Option<VectorBatch>,ExecutionError>{
        if self.terminal{return Ok(None);}
        loop{
            context.check_cancelled()?;
            let Some(batch)=self.input.next_batch(context)? else{self.terminal=true;return Ok(None);};
            let mut selection=Vec::with_capacity(batch.row_count);
            for row in 0..batch.row_count{
                if row % context.limits.max_work_unit_rows == 0 {context.check_cancelled()?;}
                if matches!(truth(&eval(&self.predicate,&batch,row)?)?,Truth::True){selection.push(row);}
            }
            context.record_filtered(batch.row_count.saturating_sub(selection.len()));
            if selection.is_empty(){continue;}
            let output=batch.take_rows(&selection)?;
            context.limits.check_batch(&output)?;
            return Ok(Some(output));
        }
    }
}

pub struct LimitOperator {
    input: Box<dyn PhysicalOperator>,
    remaining: usize,
    schema: Vec<Column>,
    terminal: bool,
}
impl LimitOperator {
    pub fn new(input:Box<dyn PhysicalOperator>,limit:usize)->Self{
        let schema=input.schema().to_vec();
        Self{input,remaining:limit,schema,terminal:limit==0}
    }
}
impl PhysicalOperator for LimitOperator {
    fn schema(&self)->&[Column]{&self.schema}
    fn next_batch(&mut self,context:&ExecutionContext)->Result<Option<VectorBatch>,ExecutionError>{
        if self.terminal||self.remaining==0{self.terminal=true;return Ok(None);}
        context.check_cancelled()?;
        let Some(batch)=self.input.next_batch(context)? else{self.terminal=true;return Ok(None);};
        if batch.row_count<=self.remaining{self.remaining-=batch.row_count;return Ok(Some(batch));}
        let indices=(0..self.remaining).collect::<Vec<_>>();
        let output=batch.take_rows(&indices)?;
        self.remaining=0;self.terminal=true;
        Ok(Some(output))
    }
}

pub struct ExecutionPipeline { root: Box<dyn PhysicalOperator>, context: ExecutionContext, terminal: bool }
impl ExecutionPipeline {
    pub fn new(root:Box<dyn PhysicalOperator>,context:ExecutionContext)->Self{Self{root,context,terminal:false}}
    pub fn schema(&self)->&[Column]{self.root.schema()}
    pub fn profile(&self)->ExecutionProfile{self.context.profile()}
    pub fn cancel(&self){self.context.cancel();}
}
impl Iterator for ExecutionPipeline {
    type Item=Result<VectorBatch,ExecutionError>;
    fn next(&mut self)->Option<Self::Item>{
        if self.terminal{return None;}
        match self.root.next_batch(&self.context){
            Ok(Some(batch))=>{self.context.record_output(&batch);Some(Ok(batch))},
            Ok(None)=>{self.terminal=true;None},
            Err(error)=>{self.terminal=true;Some(Err(error))},
        }
    }
}

#[cfg(test)]
mod tests{
    use super::*;
    use ananta_types::{Column,DataType,Schema};
    use std::fs;
    use std::time::{SystemTime,UNIX_EPOCH};

    fn fixture()->(Database,std::path::PathBuf){
        let n=SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        let root=std::env::temp_dir().join(format!("ananta-vector-{n}"));
        let mut db=Database::open(&root).unwrap();
        db.create_table("t",Schema::new(vec![
            Column::new("id",DataType::Int64,false),
            Column::new("v",DataType::Int64,true),
            Column::new("enabled",DataType::Boolean,true),
        ]).unwrap()).unwrap();
        db.insert_batch("t",&[
            vec![Value::Int64(1),Value::Int64(10),Value::Boolean(true)],
            vec![Value::Int64(2),Value::Null,Value::Null],
            vec![Value::Int64(3),Value::Int64(30),Value::Boolean(false)],
            vec![Value::Int64(4),Value::Int64(40),Value::Boolean(true)],
        ]).unwrap();
        (db,root)
    }

    #[test]
    fn vector_batches_validate_owned_lengths_and_share_projection_buffers(){
        let field=Column::new("x",DataType::Int64,false);
        let vector=Vector::new(field,vec![Value::Int64(1),Value::Int64(2)]).unwrap();
        let batch=VectorBatch::try_new(vec![vector.clone()],2).unwrap();
        let projected=batch.select_columns(&[0]).unwrap();
        assert!(Arc::ptr_eq(&batch.columns[0].values,&projected.columns[0].values));
        assert!(VectorBatch::try_new(vec![vector],3).is_err());
    }

    #[test]
    fn scan_filter_projection_and_limit_are_bounded(){
        let(db,root)=fixture();
        let context=ExecutionContext::default();
        let scan=ScanOperator::new(&db,"t",&["id","v","enabled"],2).unwrap();
        let filter=FilterOperator::new(Box::new(scan),VectorExpr::And(
            Box::new(VectorExpr::IsNotNull(Box::new(VectorExpr::Column(1)))),
            Box::new(VectorExpr::Eq(Box::new(VectorExpr::Column(2)),Box::new(VectorExpr::Literal(Value::Boolean(true)))))
        ));
        let project=ProjectionOperator::new(Box::new(filter),vec![0,1]).unwrap();
        let limit=LimitOperator::new(Box::new(project),1);
        let mut pipeline=ExecutionPipeline::new(Box::new(limit),context);
        let mut rows=Vec::new();
        for batch in pipeline.by_ref(){rows.extend(batch.unwrap().rows());}
        assert_eq!(rows,vec![vec![Value::Int64(1),Value::Int64(10)]]);
        drop(db);fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn resource_limits_and_cancellation_fail_closed(){
        let(db,root)=fixture();
        let mut limits=ResourceLimits::default();limits.max_batch_bytes=1;
        let context=ExecutionContext::new(limits).unwrap();
        let scan=ScanOperator::new(&db,"t",&["id"],2).unwrap();
        let mut pipeline=ExecutionPipeline::new(Box::new(scan),context);
        assert!(matches!(pipeline.next().unwrap(),Err(ExecutionError::ResourceLimit(_))));
        let context=ExecutionContext::default();context.cancel();
        let scan=ScanOperator::new(&db,"t",&["id"],2).unwrap();
        let mut pipeline=ExecutionPipeline::new(Box::new(scan),context);
        assert_eq!(pipeline.next().unwrap().unwrap_err(),ExecutionError::Cancelled);
        drop(db);fs::remove_dir_all(root).unwrap();
    }
}
