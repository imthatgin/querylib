use chrono::Utc;
use neo4rs::{query, Database, Graph};
use serde::{Deserialize, Serialize};
use std::{
    fs,
    path::{Path, PathBuf},
};
use thiserror::Error;
use tracing::{error, info};

use crate::{get_single, parameterize::parameterize, QueryError};

/// Represents a file migration discovered on disk.
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct FileMigration {
    pub checksum: String,
    pub file_name: String,
    pub cypher_text: String,
}

/// Represents a migrated migration in the graph database.
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct MigrationsNode {
    pub checksum: String,
    pub file_name: String,
    pub cypher_text: String,
    pub version: u64,
    pub timestamp: chrono::DateTime<Utc>,
}

#[derive(Debug, Error)]
pub enum MigrationError {
    #[error("Migration checksum was mismatched")]
    ChecksumMismatch,

    #[error("Database error when migrating: {0}")]
    Neo4jError(#[from] neo4rs::Error),

    #[error("Query error: {0}")]
    QueryError(#[from] QueryError),
}

/// GraphMigrator is used to run migrations from .cyp or .cypher files in a directory.
/// When migrating, it creates a chain of nodes representing migrations in the database.
pub struct GraphMigrator {}

impl GraphMigrator {
    pub fn new() -> Self {
        Self {}
    }

    /// Gathers all `.cyp` migrations from the specified folder.
    pub fn gather_migrations(&self, folder_path: &Path) -> Vec<FileMigration> {
        // Collect migration files
        match fs::read_dir(folder_path) {
            Ok(entries) => entries
                .filter_map(|entry| entry.ok()) // Filter out errors
                .filter_map(|entry| self.process_file(entry.path())) // Process valid `.cyp` files
                .collect(),
            Err(_) => vec![], // Return empty vector if folder can't be read
        }
    }

    pub async fn run_migrations(
        &self,
        db: Database,
        driver: Graph,
        migrations: Vec<FileMigration>,
    ) -> Result<(), MigrationError> {
        info!("Running migrations for {} files", migrations.len());

        for (counter, migration) in migrations.iter().enumerate() {
            self.up_migration(counter as u64, db.clone(), driver.clone(), migration)
                .await?;
        }

        Ok(())
    }

    /// Checks if a migration exists already, and migrates it if it has not been migrated already.
    async fn up_migration(
        &self,
        counter: u64,
        db: Database,
        driver: Graph,
        migration: &FileMigration,
    ) -> Result<(), MigrationError> {
        let existing = self
            .get_existing_migration(db.clone(), driver.clone(), migration.file_name.as_str())
            .await?;

        if let Some(existing_migration) = existing {
            if existing_migration.checksum != migration.checksum {
                error!(
                    "[{}] CHECKSUM MISMATCH - wrong checksum ✘",
                    migration.file_name
                );
                return Err(MigrationError::ChecksumMismatch);
            }
            info!("[{}] SKIP - up to date ☇", migration.file_name);
            return Ok(());
        }

        let _ = self
            .create_migration_node(counter, db, driver, migration)
            .await?;

        info!("[{}] DONE - migrated ✓", migration.file_name);

        Ok(())
    }

    /// Actual migration in a transaction.
    async fn create_migration_node(
        &self,
        counter: u64,
        db: Database,
        driver: Graph,
        migration: &FileMigration,
    ) -> Result<Option<MigrationsNode>, MigrationError> {
        let mut tx = driver.start_txn_on(db.clone()).await?;

        let new_node = MigrationsNode {
            checksum: migration.checksum.clone(),
            file_name: migration.file_name.clone(),
            cypher_text: migration.file_name.clone(),
            version: counter + 1,
            timestamp: chrono::Utc::now(),
        };

        let parameterized_migration = parameterize(new_node);
        let migration_node_query = query(include_str!("cypher/create_migration_node.cypher"))
            .param("migrationNode", parameterized_migration)
            .param("previousVersion", counter as i64);

        tx.run(query(migration.cypher_text.as_str())).await?;
        tx.commit().await?;

        let mut tx_migration_node = driver.start_txn_on(db.clone()).await?;

        tx_migration_node.run(migration_node_query).await?;
        tx_migration_node.commit().await?;

        Ok(None)
    }

    /// Runs a query to look for a migration node with the same file name.
    async fn get_existing_migration(
        &self,
        db: Database,
        driver: Graph,
        name: &str,
    ) -> Result<Option<MigrationsNode>, MigrationError> {
        let mut tx = driver.start_txn_on(db.clone()).await?;

        let q = query("MATCH (m:DataModelMigration { file_name: $migration_file_name }) RETURN m")
            .param("migration_file_name", name);

        let mut results = tx.execute(q).await?;

        let execution_result = get_single::<MigrationsNode>(&mut tx, &mut results).await;

        match execution_result {
            Ok(migration) => Ok(migration),
            Err(err) => Err(MigrationError::QueryError(err)),
        }
    }

    /// Processes a single file and returns a `FileMigration` if it is a `.cyp` file.
    fn process_file(&self, file_path: PathBuf) -> Option<FileMigration> {
        if file_path.extension()? == "cyp" || file_path.extension()? == "cypher" {
            let file_name = file_path.file_name()?.to_string_lossy().to_string();
            let cypher_text = fs::read_to_string(&file_path).ok()?;
            let checksum = self.calculate_checksum(&cypher_text);

            Some(FileMigration {
                checksum,
                file_name,
                cypher_text,
            })
        } else {
            None
        }
    }

    /// Calculates a checksum for the given text.
    fn calculate_checksum(&self, content: &str) -> String {
        sha256::digest(content).to_string()
    }
}

impl Default for GraphMigrator {
    fn default() -> Self {
        Self::new()
    }
}
