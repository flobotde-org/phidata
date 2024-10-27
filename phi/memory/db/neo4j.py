from neo4j import GraphDatabase, Driver, ManagedTransaction
from typing import Optional, List, Dict, Any
from phi.memory.db import MemoryDb
from phi.memory.row import MemoryRow  # Assuming this is your data model


class Neo4jMemoryDb(MemoryDb):
    def __init__(self, uri: str, user: str, password: str):
        """
        This class provides a memory store backed by a Neo4j graph database.

        Args:
            uri (str): The URI for the Neo4j database.
            user (str): The username to connect with.
            password (str): The password to connect with.
        """
        self.driver: Driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        """Close the connection to the database."""
        self.driver.close()

    def _transaction_execute(
        self, /, tx: ManagedTransaction, query: str, parameters: Optional[Dict[str, Any]] = None
    ) -> Any:
        return tx.run(query, parameters).data()

    def _execute_write(self, query: str, parameters: Optional[Dict[str, Any]] = None) -> Any:
        with self.driver.session() as session:
            return session.write_transaction(self._transaction_execute, query, parameters)

    def _execute_read(self, query: str, parameters: Optional[Dict[str, Any]] = None) -> Any:
        with self.driver.session() as session:
            return session.read_transaction(self._transaction_execute, query, parameters)

    def create(self) -> None:
        # In Neo4j, you don't typically create tables. But you might want an index if you're working with very large graphs.
        query = """
        CREATE CONSTRAINT IF NOT EXISTS
        FOR (m:Memory)
        REQUIRE m.id IS UNIQUE
        """
        self._execute_write(query)

    def memory_exists(self, memory: MemoryRow) -> bool:
        query = """
        MATCH (m:Memory {id: $id})
        RETURN m
        """
        result = self._execute_read(query, {"id": memory.id})
        return len(result) > 0

    def read_memories(
        self, user_id: Optional[str] = None, limit: Optional[int] = None, sort: Optional[str] = None
    ) -> List[MemoryRow]:
        query = """
        MATCH (m:Memory)
        WHERE $user_id IS NULL OR m.user_id = $user_id
        RETURN m ORDER BY m.created_at {}
        LIMIT $limit
        """.format("ASC" if sort == "asc" else "DESC")
        parameters = {"user_id": user_id, "limit": limit or 100}
        results = self._execute_read(query, parameters)
        return [MemoryRow(**record["m"]) for record in results]

    def upsert_memory(self, memory: MemoryRow) -> Optional[MemoryRow]:
        query = """
        MERGE (m:Memory {id: $id})
        ON CREATE SET m.user_id = $user_id, m.memory = $memory, m.created_at = timestamp()
        ON MATCH SET m.memory = $memory, m.updated_at = timestamp()
        RETURN m
        """
        parameters = {"id": memory.id, "user_id": memory.user_id, "memory": memory.memory}
        self._execute_write(query, parameters)
        return memory

    def delete_memory(self, id: str) -> None:
        query = """
        MATCH (m:Memory {id: $id})
        DELETE m
        """
        self._execute_write(query, {"id": id})

    def drop_table(self) -> None:
        # Deleting all memories
        query = """
        MATCH (m:Memory)
        DETACH DELETE m
        """
        self._execute_write(query)

    def table_exists(self) -> bool:
        # Not quite applicable in the same way as SQL, but checking if any Memory exists
        query = """
        MATCH (m:Memory) RETURN m LIMIT 1
        """
        result = self._execute_read(query)
        return len(result) > 0

    def clear(self) -> bool:
        self.drop_table()
        return True


# Usage Example:
# db = Neo4jMemoryDb("bolt://localhost:7687", "neo4j", "password")
# db.create()
# db.upsert_memory(MemoryRow(id="1", user_id="user1", memory={'data': 'example'}))
# memories = db.read_memories()
# db.delete_memory("1")
# db.close()
