use neo4rs::{RowStream, Txn};
use serde::de::DeserializeOwned;
use thiserror::Error;

mod migrations;
mod parameterize;

pub use migrations::GraphMigrator;
pub use parameterize::parameterize;

#[derive(Debug, Error)]
pub enum QueryError {
    #[error("Failed to execute query: {0}")]
    ExecutionError(#[from] neo4rs::Error),

    #[error("Failed to deserialize query result: {0}")]
    NeoDeError(#[from] neo4rs::DeError),

    #[error(transparent)]
    SerializationError(#[from] serde_json::Error),

    #[error("No records were found")]
    NoRecordsFound,
}

/// Safe variant of``single`. This will return an Option<T> instead of an error.
pub async fn get_single<T>(tx: &mut Txn, results: &mut RowStream) -> Result<Option<T>, QueryError>
where
    T: DeserializeOwned,
{
    let query_results = if let Some(row) = results.next(tx).await? {
        // Deserialize the result into the desired type
        let deserialized: Option<T> = row.to::<Option<T>>()?;
        Ok(deserialized)
    } else {
        Ok(None)
    };

    query_results
}

pub async fn single<T>(tx: &mut Txn, results: &mut RowStream) -> Result<T, QueryError>
where
    T: DeserializeOwned,
{
    let row = results.next(tx).await?.ok_or(QueryError::NoRecordsFound)?;
    let deserialized: T = row.to::<T>()?;

    Ok(deserialized)
}

pub async fn all<T>(tx: &mut Txn, results: &mut RowStream) -> Result<Vec<T>, QueryError>
where
    T: DeserializeOwned,
{
    let mut output = Vec::new();

    // Collect all rows
    while let Some(ref row) = results.next(&mut *tx).await? {
        match row.to::<T>() {
            Ok(entry) => {
                let deserialized: T = entry;
                output.push(deserialized);
            }
            Err(_) => (),
        };
    }

    Ok(output)
}
