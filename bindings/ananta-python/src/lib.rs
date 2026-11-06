use ananta_aql::{execute_source, ParameterBindings};
use ananta_core::{BatchId, Database, IngestOutcome};
use ananta_types::{Column, DataType, Schema, Value};
use pyo3::exceptions::{PyRuntimeError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList};
use std::path::PathBuf;
use std::sync::Mutex;

fn engine_error(error: impl std::fmt::Display) -> PyErr {
    PyRuntimeError::new_err(error.to_string())
}

fn parse_type(value: &str) -> PyResult<DataType> {
    match value.to_ascii_uppercase().as_str() {
        "BOOLEAN" => Ok(DataType::Boolean),
        "INT32" => Ok(DataType::Int32),
        "INT64" => Ok(DataType::Int64),
        "FLOAT64" => Ok(DataType::Float64),
        "UTF8" | "STRING" => Ok(DataType::Utf8),
        "TIMESTAMP_MICROS" => Ok(DataType::TimestampMicros),
        "BINARY" => Ok(DataType::Binary),
        other => Err(PyValueError::new_err(format!("unsupported ANANTA type: {other}"))),
    }
}

fn py_value_to_native(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
    data_type: DataType,
) -> PyResult<Value> {
    if value.is_none() {
        return Ok(Value::Null);
    }
    match data_type {
        DataType::Boolean => value.extract::<bool>().map(Value::Boolean),
        DataType::Int32 => value.extract::<i32>().map(Value::Int32),
        DataType::Int64 => value.extract::<i64>().map(Value::Int64),
        DataType::Float64 => value.extract::<f64>().map(Value::Float64),
        DataType::Utf8 => value.extract::<String>().map(Value::Utf8),
        DataType::TimestampMicros => value.extract::<i64>().map(Value::TimestampMicros),
        DataType::Binary => value.extract::<Vec<u8>>().map(Value::Binary),
    }
    .map_err(|error| PyTypeError::new_err(format!("value does not match {data_type}: {error}")))
}

fn native_value_to_py(py: Python<'_>, value: &Value) -> PyResult<PyObject> {
    Ok(match value {
        Value::Null => py.None(),
        Value::Boolean(value) => value.into_py(py),
        Value::Int32(value) => value.into_py(py),
        Value::Int64(value) => value.into_py(py),
        Value::Float64(value) => value.into_py(py),
        Value::Utf8(value) => value.into_py(py),
        Value::TimestampMicros(value) => value.into_py(py),
        Value::Binary(value) => PyBytes::new_bound(py, value).into_py(py),
    })
}

#[pyclass(name = "AnantaDatabase")]
struct PyAnantaDatabase {
    database: Mutex<Database>,
    path: PathBuf,
}

#[pymethods]
impl PyAnantaDatabase {
    #[new]
    fn new(path: String) -> PyResult<Self> {
        let database = Database::open(&path).map_err(engine_error)?;
        Ok(Self { database: Mutex::new(database), path: PathBuf::from(path) })
    }

    #[getter]
    fn path(&self) -> String {
        self.path.display().to_string()
    }

    fn table_names(&self) -> PyResult<Vec<String>> {
        let database = self.database.lock().map_err(engine_error)?;
        Ok(database.table_names().map(str::to_owned).collect())
    }

    fn create_table(&self, name: &str, columns: Vec<(String, String, bool)>) -> PyResult<()> {
        let mut database = self.database.lock().map_err(engine_error)?;
        let native_columns = columns
            .into_iter()
            .map(|(column_name, data_type, nullable)| {
                Ok(Column::new(column_name, parse_type(&data_type)?, nullable))
            })
            .collect::<PyResult<Vec<_>>>()?;
        let schema = Schema::new(native_columns).map_err(engine_error)?;

        if database.table_names().any(|existing| existing == name) {
            let current = database.schema(name).map_err(engine_error)?;
            if current != &schema {
                return Err(PyValueError::new_err(format!(
                    "table '{name}' already exists with a different schema"
                )));
            }
            return Ok(());
        }
        database.create_table(name, schema).map_err(engine_error)
    }

    fn ingest_batch(
        &self,
        py: Python<'_>,
        table: &str,
        batch_id: &[u8],
        rows: Vec<Vec<PyObject>>,
    ) -> PyResult<PyObject> {
        let id_bytes: [u8; 16] = batch_id.try_into()
            .map_err(|_| PyValueError::new_err("BatchId must contain exactly 16 bytes"))?;
        let id = BatchId::from_bytes(id_bytes).map_err(engine_error)?;
        let mut database = self.database.lock().map_err(engine_error)?;
        let schema = database.schema(table).map_err(engine_error)?.clone();

        let mut native_rows = Vec::with_capacity(rows.len());
        for (row_index, row) in rows.iter().enumerate() {
            if row.len() != schema.len() {
                return Err(PyValueError::new_err(format!(
                    "row {row_index} has {} values; expected {}",
                    row.len(), schema.len()
                )));
            }
            let mut native_row = Vec::with_capacity(row.len());
            for (column, value) in schema.columns().iter().zip(row) {
                native_row.push(py_value_to_native(py, value.bind(py), column.data_type)?);
            }
            native_rows.push(native_row);
        }

        let receipt = database.ingest_batch(table, id, &native_rows).map_err(engine_error)?;
        let output = PyDict::new_bound(py);
        output.set_item(
            "outcome",
            match receipt.outcome {
                IngestOutcome::Accepted => "accepted",
                IngestOutcome::Duplicate => "duplicate",
            },
        )?;
        output.set_item("batch_id", receipt.batch_id.to_hex())?;
        output.set_item("table_id", receipt.table_id)?;
        output.set_item("ingestion_sequence", receipt.ingestion_sequence)?;
        output.set_item("row_count", receipt.row_count)?;
        output.set_item("encoded_bytes", receipt.encoded_bytes)?;
        Ok(output.into_py(py))
    }

    fn query(&self, py: Python<'_>, source: &str, batch_rows: usize) -> PyResult<PyObject> {
        let database = self.database.lock().map_err(engine_error)?;
        let bindings = ParameterBindings::default();
        let mut result = execute_source(&database, source, &bindings, batch_rows, None)
            .map_err(engine_error)?;
        let output = PyList::empty_bound(py);

        while let Some(batch) = result.next() {
            let batch = batch.map_err(engine_error)?;
            for row_index in 0..batch.row_count {
                let record = PyDict::new_bound(py);
                for (column_index, column) in batch.schema.iter().enumerate() {
                    let value = batch.columns[column_index].get(row_index)
                        .ok_or_else(|| PyRuntimeError::new_err("AQL result batch column length mismatch"))?;
                    record.set_item(&column.name, native_value_to_py(py, value)?)?;
                }
                output.append(record)?;
            }
        }
        Ok(output.into_py(py))
    }
}

#[pymodule]
fn ananta_native(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyAnantaDatabase>()?;
    Ok(())
}
