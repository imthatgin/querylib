OPTIONAL MATCH (previousMigration:DataModelMigration {version: $previousVersion})
CREATE (migration:DataModelMigration $migrationNode)

WITH previousMigration, migration

WHERE previousMigration IS NOT NULL
MERGE (migration)-[:PREVIOUS_MIGRATION]->(previousMigration)

RETURN migration