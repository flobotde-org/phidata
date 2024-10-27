# phi/storage/agent/neo4j.py
from neo4j import GraphDatabase, Neo4jDriver, Transaction
from typing import Optional, List
from phi.agent.session import AgentSession
from phi.storage.agent.base import AgentStorage
from phi.utils.log import logger


class Neo4jAgentStorage(AgentStorage):
    def __init__(self, uri: str, user: str, password: str):
        """
        Initialize Agent storage using a Neo4j graph database.

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
        FOR (s:Session)
        REQUIRE s.session_id IS UNIQUE
        """
        self._execute_write(query)

    def read(self, session_id: str, user_id: Optional[str] = None) -> Optional[AgentSession]:
        query = """
        MATCH (s:Session {session_id: $session_id})
        RETURN s
        """
        result = self._execute_read(query, {"session_id": session_id})
        if result:
            return AgentSession(**result[0]['s'])
        return None

    def get_all_session_ids(self, user_id: Optional[str] = None, agent_id: Optional[str] = None) -> List[str]:
        query = """
        MATCH (s:Session)
        WHERE ($user_id IS NULL OR s.user_id = $user_id) AND ($agent_id IS NULL OR s.agent_id = $agent_id)
        RETURN s.session_id
        """
        results = self._execute_read(query, {"user_id": user_id, "agent_id": agent_id})
        return [record['s.session_id'] for record in results]

    def get_all_sessions(self, user_id: Optional[str] = None, agent_id: Optional[str] = None) -> List[AgentSession]:
        query = """
        MATCH (s:Session)
        WHERE ($user_id IS NULL OR s.user_id = $user_id) AND ($agent_id IS NULL OR s.agent_id = $agent_id)
        RETURN s
        """
        results = self._execute_read(query, {"user_id": user_id, "agent_id": agent_id})
        return [AgentSession(**record['s']) for record in results]

    def upsert(self, session: AgentSession) -> Optional[AgentSession]:
        query = """
        MERGE (s:Session {session_id: $session_id})
        SET s.agent_id = $agent_id,
            s.user_id = $user_id,
            s.memory = $memory,
            s.agent_data = $agent_data,
            s.user_data = $user_data,
            s.session_data = $session_data,
            s.updated_at = timestamp()
        RETURN s
        """
        parameters = {
            "session_id": session.session_id,
            "agent_id": session.agent_id,
            "user_id": session.user_id,
            "memory": session.memory,
            "agent_data": session.agent_data,
            "user_data": session.user_data,
            "session_data": session.session_data
        }
        result = self._execute_write(query, parameters)
        if result:
            return AgentSession(**result[0]['s'])
        return None

    def delete_session(self, session_id: Optional[str] = None):
        if session_id is None:
            logger.warning("No session_id provided for deletion.")
            return

        query = """
        MATCH (s:Session {session_id: $session_id})
        DELETE s
        """
        self._execute_write(query, {"session_id": session_id})

    def drop(self) -> None:
        # Delete all sessions
        query = """
        MATCH (s:Session)
        DETACH DELETE s
        """
        self._execute_write(query)

    def upgrade_schema(self) -> None:
        # Placeholder for schema upgrades, if needed
        pass

# Usage Example:
# db = Neo4jAgentStorage("bolt://localhost:7687", "neo4j", "password")
# db.create()
# session = AgentSession(session_id="1", agent_id="agent1", user_id="user1", memory={}, agent_data={}, user_data={}, session_data={})
# db.upsert(session)
# sessions = db.get_all_sessions()
# db.delete_session("1")
# db.close()
