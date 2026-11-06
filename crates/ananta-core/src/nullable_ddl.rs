//! WAL-backed immutable schema and catalog publication for nullable-column DDL.

use super::{
    allocate_object_id, increment_catalog_revision, Database,
    DbError, ObjectId,
};
use crate::schema_snapshot::SchemaSnapshot;
use ananta_types::{Column, DataType, Schema};

impl Database {
    /// Add one nullable column without rewriting existing data segments.
    ///
    /// The current immutable snapshot is verified against the catalog first.
    /// The new schema snapshot and complete catalog snapshot are committed as
    /// one WAL transaction before either final metadata name is published.
    pub fn add_nullable_column(
        &mut self,
        table: &str,
        name: impl Into<String>,
        data_type: DataType,
    ) -> Result<ObjectId, DbError> {
        self.ensure_operational()?;
        let name = name.into();
        let current = self.catalog.tables.get(table)
            .ok_or_else(|| DbError::TableNotFound(table.to_owned()))?;

        let current_snapshot = SchemaSnapshot::read_from(
            &self.root,
            current.id,
            current.schema_revision,
        ).map_err(|_| DbError::SchemaCompatibility(
            "current schema snapshot is missing or invalid",
        ))?;

        if current_snapshot.table_id() != current.id
            || current_snapshot.revision() != current.schema_revision
            || current_snapshot.schema()? != current.schema
            || current_snapshot.column_ids().collect::<Vec<_>>() != current.column_ids
        {
            return Err(DbError::SchemaCompatibility(
                "current schema snapshot does not match the catalog definition",
            ));
        }

        let normalized_name = name.to_lowercase();
        if current.schema.columns().iter()
            .any(|column| column.name.to_lowercase() == normalized_name)
        {
            return Err(DbError::ColumnExists(name));
        }

        let mut candidate = self.catalog.clone();
        let new_column_id = allocate_object_id(&mut candidate)?;
        let definition = candidate.tables.get_mut(table)
            .ok_or_else(|| DbError::TableNotFound(table.to_owned()))?;

        definition.schema_revision = definition.schema_revision
            .checked_add(1)
            .ok_or(DbError::LengthOverflow)?;

        let mut columns = definition.schema.columns().to_vec();
        columns.push(Column::new(name, data_type, true));
        definition.schema = Schema::new(columns)?;
        definition.column_ids.push(new_column_id);
        increment_catalog_revision(&mut candidate)?;

        let revised = candidate.tables.get(table)
            .ok_or_else(|| DbError::TableNotFound(table.to_owned()))?;
        let snapshot = SchemaSnapshot::new(
            revised.id,
            revised.schema_revision,
            &revised.schema,
            &revised.column_ids,
        )?;
        self.commit_catalog_mutation(candidate, snapshot, b"add_nullable_column")?;
        Ok(new_column_id)
    }
}
