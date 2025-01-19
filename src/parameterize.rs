use neo4rs::BoltType;
use serde::Serialize;

use crate::QueryError;

/// Used to convert an instance of T into a parameterizable map of properties that neo4rs can use.
pub fn parameterize<T: Serialize>(instance: T) -> BoltType {
    struct_to_hashmap(&instance).unwrap()
}

fn struct_to_hashmap<T: Serialize>(instance: &T) -> Result<BoltType, QueryError> {
    let value = serde_json::to_value(instance)?;
    let bolt_type = BoltType::try_from(value)?;

    Ok(bolt_type)
}
