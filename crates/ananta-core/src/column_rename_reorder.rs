//! Snapshot-first schema operations that preserve stable column identities.

use super::{
    increment_catalog_revision, save_catalog, Database, DbError, ObjectId, TableDefinition,
};
use crate::schema_snapshot::SchemaSnapshot;
use ananta_types::{Column, Schema};
use std::path::Path;

fn verify_snapshot(root: &Path, definition: &TableDefinition) -> Result<(), DbError> {
    let snapshot = SchemaSnapshot::read_from(
        root,
        definition.id,
        definition.schema_revision,
    ).map_err(|_| DbError::SchemaCompatibility(
        "current schema snapshot is missing or invalid",
    ))?;

    if snapshot.table_id() != definition.id
        || snapshot.revision() != definition.schema_revision
        || snapshot.schema()? != definition.schema
        || snapshot.column_ids().collect::<Vec<ObjectId>>() != definition.column_ids
    {
        return Err(DbError::SchemaCompatibility(
            "current schema snapshot does not match the catalog definition",
        ));
    }
    Ok(())
}

impl Database {
    /// Rename a column while preserving its stable ColumnId.
    ///
    /// Historical segment layouts remain valid because resolution uses ColumnId,
    /// not the display name. An exact same-name request is a no-op.
    pub fn rename_column(
        &mut self,
        table: &str,
        old_name: &str,
        new_name: impl Into<String>,
    ) -> Result<ObjectId, DbError> {
        let new_name = new_name.into();
        let current = self.catalog.tables.get(table)
            .ok_or_else(|| DbError::TableNotFound(table.to_owned()))?;
        verify_snapshot(&self.root, current)?;

        let index = current.schema.columns().iter()
            .position(|column| column.name == old_name)
            .ok_or_else(|| DbError::ColumnNotFound(old_name.to_owned()))?;
        let column_id = current.column_ids[index];

        if old_name == new_name {
            return Ok(column_id);
        }

        let normalized_name = new_name.to_lowercase();
        if current.schema.columns().iter().enumerate()
            .any(|(other_index, column)| {
                other_index != index && column.name.to_lowercase() == normalized_name
            })
        {
            return Err(DbError::ColumnExists(new_name));
        }

        let mut columns = current.schema.columns().to_vec();
        columns[index] = Column::new(
            new_name,
            columns[index].data_type,
            columns[index].nullable,
        );
        let revised_schema = Schema::new(columns)?;

        let mut candidate = self.catalog.clone();
        let revised = candidate.tables.get_mut(table)
            .ok_or_else(|| DbError::TableNotFound(table.to_owned()))?;
        revised.schema_revision = revised.schema_revision
            .checked_add(1)
            .ok_or(DbError::LengthOverflow)?;
        revised.schema = revised_schema;
        increment_catalog_revision(&mut candidate)?;

        let revised = candidate.tables.get(table)
            .ok_or_else(|| DbError::TableNotFound(table.to_owned()))?;
        SchemaSnapshot::new(
            revised.id,
            revised.schema_revision,
            &revised.schema,
            &revised.column_ids,
        )?.write_to(&self.root)?;

        save_catalog(
            &self.root.join("catalog").join("catalog.anc"),
            &candidate,
        )?;
        self.catalog = candidate;
        Ok(column_id)
    }

    /// Reorder all columns using an exact permutation of their current names.
    ///
    /// The parallel ColumnId vector is permuted in lockstep with the schema.
    /// Historical segment values are resolved by identity into this new order.
    pub fn reorder_columns(
        &mut self,
        table: &str,
        ordered_names: &[&str],
    ) -> Result<(), DbError> {
        let current = self.catalog.tables.get(table)
            .ok_or_else(|| DbError::TableNotFound(table.to_owned()))?;
        verify_snapshot(&self.root, current)?;

        if ordered_names.len() != current.schema.len() {
            return Err(DbError::InvalidColumnOrder(format!(
                "expected {} column names, received {}",
                current.schema.len(),
                ordered_names.len(),
            )));
        }

        let mut indexes = Vec::with_capacity(ordered_names.len());
        let mut seen = vec![false; current.schema.len()];
        for name in ordered_names {
            let index = current.schema.columns().iter()
                .position(|column| column.name == *name)
                .ok_or_else(|| DbError::ColumnNotFound((*name).to_owned()))?;
            if seen[index] {
                return Err(DbError::InvalidColumnOrder(format!(
                    "column {name} appears more than once",
                )));
            }
            seen[index] = true;
            indexes.push(index);
        }

        if indexes.iter().copied().eq(0..current.schema.len()) {
            return Ok(());
        }

        let columns = indexes.iter()
            .map(|index| current.schema.columns()[*index].clone())
            .collect::<Vec<_>>();
        let column_ids = indexes.iter()
            .map(|index| current.column_ids[*index])
            .collect::<Vec<_>>();
        let revised_schema = Schema::new(columns)?;

        let mut candidate = self.catalog.clone();
        let revised = candidate.tables.get_mut(table)
            .ok_or_else(|| DbError::TableNotFound(table.to_owned()))?;
        revised.schema_revision = revised.schema_revision
            .checked_add(1)
            .ok_or(DbError::LengthOverflow)?;
        revised.schema = revised_schema;
        revised.column_ids = column_ids;
        increment_catalog_revision(&mut candidate)?;

        let revised = candidate.tables.get(table)
            .ok_or_else(|| DbError::TableNotFound(table.to_owned()))?;
        SchemaSnapshot::new(
            revised.id,
            revised.schema_revision,
            &revised.schema,
            &revised.column_ids,
        )?.write_to(&self.root)?;

        save_catalog(
            &self.root.join("catalog").join("catalog.anc"),
            &candidate,
        )?;
        self.catalog = candidate;
        Ok(())
    }
}
