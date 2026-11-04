use std::collections::HashSet;
use std::fmt;

/// Logical data types supported by the first native storage prototype.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum DataType {
    Boolean = 1,
    Int32 = 2,
    Int64 = 3,
    Float64 = 4,
    Utf8 = 5,
    TimestampMicros = 6,
    Binary = 7,
}

impl DataType {
    pub fn from_code(code: u8) -> Option<Self> {
        match code {
            1 => Some(Self::Boolean),
            2 => Some(Self::Int32),
            3 => Some(Self::Int64),
            4 => Some(Self::Float64),
            5 => Some(Self::Utf8),
            6 => Some(Self::TimestampMicros),
            7 => Some(Self::Binary),
            _ => None,
        }
    }
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::Boolean => "BOOLEAN",
            Self::Int32 => "INT32",
            Self::Int64 => "INT64",
            Self::Float64 => "FLOAT64",
            Self::Utf8 => "UTF8",
            Self::TimestampMicros => "TIMESTAMP_MICROS",
            Self::Binary => "BINARY",
        };
        f.write_str(name)
    }
}

/// A typed value. Null is represented explicitly and checked against column nullability.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Boolean(bool),
    Int32(i32),
    Int64(i64),
    Float64(f64),
    Utf8(String),
    TimestampMicros(i64),
    Binary(Vec<u8>),
}

impl Value {
    pub fn data_type(&self) -> Option<DataType> {
        match self {
            Self::Null => None,
            Self::Boolean(_) => Some(DataType::Boolean),
            Self::Int32(_) => Some(DataType::Int32),
            Self::Int64(_) => Some(DataType::Int64),
            Self::Float64(_) => Some(DataType::Float64),
            Self::Utf8(_) => Some(DataType::Utf8),
            Self::TimestampMicros(_) => Some(DataType::TimestampMicros),
            Self::Binary(_) => Some(DataType::Binary),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

impl Column {
    pub fn new(name: impl Into<String>, data_type: DataType, nullable: bool) -> Self {
        Self { name: name.into(), data_type, nullable }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Schema {
    columns: Vec<Column>,
}

impl Schema {
    pub fn new(columns: Vec<Column>) -> Result<Self, TypeError> {
        if columns.is_empty() {
            return Err(TypeError::InvalidSchema("a table must contain at least one column".into()));
        }
        let mut names = HashSet::with_capacity(columns.len());
        for column in &columns {
            if column.name.trim().is_empty() {
                return Err(TypeError::InvalidSchema("column names cannot be empty".into()));
            }
            if !names.insert(column.name.to_lowercase()) {
                return Err(TypeError::InvalidSchema(format!("duplicate column name: {}", column.name)));
            }
        }
        Ok(Self { columns })
    }

    pub fn columns(&self) -> &[Column] { &self.columns }
    pub fn len(&self) -> usize { self.columns.len() }
    pub fn is_empty(&self) -> bool { self.columns.is_empty() }

    pub fn validate_row(&self, row: &[Value]) -> Result<(), TypeError> {
        if row.len() != self.columns.len() {
            return Err(TypeError::RowWidth { expected: self.columns.len(), actual: row.len() });
        }
        for (column, value) in self.columns.iter().zip(row) {
            match value.data_type() {
                None if !column.nullable => return Err(TypeError::NullNotAllowed(column.name.clone())),
                None => {}
                Some(actual) if actual != column.data_type => {
                    return Err(TypeError::TypeMismatch {
                        column: column.name.clone(), expected: column.data_type, actual,
                    });
                }
                Some(_) => {}
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TypeError {
    InvalidSchema(String),
    RowWidth { expected: usize, actual: usize },
    NullNotAllowed(String),
    TypeMismatch { column: String, expected: DataType, actual: DataType },
}

impl fmt::Display for TypeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidSchema(message) => write!(f, "invalid schema: {message}"),
            Self::RowWidth { expected, actual } => write!(f, "row has {actual} values; expected {expected}"),
            Self::NullNotAllowed(column) => write!(f, "column '{column}' does not allow NULL"),
            Self::TypeMismatch { column, expected, actual } =>
                write!(f, "column '{column}' expects {expected}, received {actual}"),
        }
    }
}

impl std::error::Error for TypeError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_validates_types_and_nullability() {
        let schema = Schema::new(vec![
            Column::new("id", DataType::Int64, false),
            Column::new("note", DataType::Utf8, true),
        ]).unwrap();
        assert!(schema.validate_row(&[Value::Int64(7), Value::Null]).is_ok());
        assert!(schema.validate_row(&[Value::Int32(7), Value::Null]).is_err());
        assert!(schema.validate_row(&[Value::Int64(7)]).is_err());
    }

    #[test]
    fn schema_rejects_duplicate_names_case_insensitively() {
        let result = Schema::new(vec![
            Column::new("Device", DataType::Int64, false),
            Column::new("device", DataType::Utf8, false),
        ]);
        assert!(result.is_err());
    }
}
