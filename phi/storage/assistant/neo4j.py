# phi/storage/assistant/neo4j.py
from neo4j import GraphDatabase, Neo4jDriver, Transaction
from typing import Optional, List
from phi.assistant.run import AssistantRun
from phi.storage.assistant.base import AssistantStorage
from phi.utils.log import logger


class Neo4jAssistantStorage(AssistantStorage):
    def __init__(self, uri: str, user: str, password: str):
        """
        Initialize Assistant storage using a Neo4j graph database.

        Args:
            uri (str): The URI for the Neo4j database.
            user (str): The username to connect with.
            password (str): The password to connect with.
        """
        self.driver: Neo4jDriver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        """Close the connection to the database."""
        self.driver.close()

    def _execute_write(self, query: str, parameters: Optional[dict] = None):
        with self.driver.session() as session:
            return session.write_transaction(self._transaction_execute, query, parameters)

    def _execute_read(self, query: str, parameters: Optional[dict] = None):
        with self.driver.session() as session:
            return session.read_transaction(self._transaction_execute, query, parameters)

    @staticmethod
    def _transaction_execute(tx: Transaction, query: str, parameters: Optional[dict] = None):
        return tx.run(query, parameters).data()

    def create(self) -> None:
        # In Neo4j, you don't typically create tables, but you can create constraints.
        query = """
        CREATE CONSTRAINT IF NOT EXISTS
        FOR (r:Run)
        REQUIRE r.run_id IS UNIQUE
        """
        self._execute_write(query)

    def read(self, run_id: str) -> Optional[AssistantRun]:
        query = """
        MATCH (r:Run {run_id: $run_id})
        RETURN r
        """
        result = self._execute_read(query, {"run_id": run_id})
        if result:
            return AssistantRun(**result[0]["r"])
        return None

    def get_all_run_ids(self, user_id: Optional[str] = None) -> List[str]:
        query = """
        MATCH (r:Run)
        WHERE $user_id IS NULL OR r.user_id = $user_id
        RETURN r.run_id
        """
        results = self._execute_read(query, {"user_id": user_id})
        return [record["r.run_id"] for record in results]

    def get_all_runs(self, user_id: Optional[str] = None) -> List[AssistantRun]:
        query = """
        MATCH (r:Run)
        WHERE $user_id IS NULL OR r.user_id = $user_id
        RETURN r
        """
        results = self._execute_read(query, {"user_id": user_id})
        return [AssistantRun(**record["r"]) for record in results]

    def upsert(self, row: AssistantRun) -> Optional[AssistantRun]:
        query = """
        MERGE (r:Run {run_id: $run_id})
        SET r.name = $name,
            r.run_name = $run_name,
            r.user_id = $user_id,
            r.llm = $llm,
            r.memory = $memory,
            r.assistant_data = $assistant_data,
            r.run_data = $run_data,
            r.user_data = $user_data,
            r.task_data = $task_data,
            r.updated_at = timestamp()
        RETURN r
        """
        parameters = {
            "run_id": row.run_id,
            "name": row.name,
            "run_name": row.run_name,
            "user_id": row.user_id,
            "llm": row.llm,
            "memory": row.memory,
            "assistant_data": row.assistant_data,
            "run_data": row.run_data,
            "user_data": row.user_data,
            "task_data": row.task_data,
        }
        result = self._execute_write(query, parameters)
        if result:
            return AssistantRun(**result[0]["r"])
        return None

    def delete(self) -> None:
        # Delete all runs
        query = """
        MATCH (r:Run)
        DETACH DELETE r
        """
        self._execute_write(query)


# Usage Example:
# db = Neo4jAssistantStorage("bolt://localhost:7687", "neo4j", "password")
# db.create()
# run = AssistantRun(run_id="1", name="Test", run_name="Test Run", user_id="user1", llm={}, memory={}, assistant_data={}, run_data={}, user_data={}, task_data={})
# db.upsert(run)
# runs = db.get_all_runs()
# db.delete()
# db.close()
