import time
from typing import Optional, List
from neo4j import GraphDatabase, Driver
from phi.workflow import WorkflowSession
from phi.storage.workflow.base import WorkflowStorage
from phi.utils.log import logger


class Neo4jWorkflowStorage(WorkflowStorage):
    def __init__(
        self,
        table_name: str,
        uri: str,
        username: str,
        password: str,
        database: str = "neo4j",
        schema_version: int = 1,
        auto_upgrade_schema: bool = False,
    ):
        """
        This class provides workflow storage using a Neo4j database.

        Args:
            table_name (str): The name of the label to store Workflow sessions.
            uri (str): The URI of the Neo4j database.
            username (str): The username for the Neo4j database.
            password (str): The password for the Neo4j database.
            database (str): The name of the Neo4j database to use. Defaults to "neo4j".
            schema_version (int): Version of the schema. Defaults to 1.
            auto_upgrade_schema (bool): Whether to automatically upgrade the schema.
        """
        # Database attributes
        self.table_name: str = table_name
        self.uri: str = uri
        self.username: str = username
        self.password: str = password
        self.database: str = database
        self._engine: Driver = GraphDatabase.driver(uri, auth=(username, password))

        # Table schema version
        self.schema_version: int = schema_version
        # Automatically upgrade schema if True
        self.auto_upgrade_schema: bool = auto_upgrade_schema

        logger.debug(f"Created Neo4jWorkflowStorage: '{self.database}.{self.table_name}'")

    def create(self) -> None:
        """
        Create the constraint for unique session_id if it doesn't exist.
        """
        with self._engine.session(database=self.database) as session:
            session.run(f"""
            CREATE CONSTRAINT IF NOT EXISTS FOR (s:{self.table_name})
            REQUIRE s.session_id IS UNIQUE
            """)

    def read(self, session_id: str, user_id: Optional[str] = None) -> Optional[WorkflowSession]:
        """
        Read a WorkflowSession from the database.

        Args:
            session_id (str): The ID of the session to read.
            user_id (Optional[str]): The ID of the user associated with the session.

        Returns:
            Optional[WorkflowSession]: The WorkflowSession object if found, None otherwise.
        """
        try:
            with self._engine.session(database=self.database) as session:
                result = session.run(
                    f"""
                MATCH (s:{self.table_name} {{session_id: $session_id}})
                WHERE $user_id IS NULL OR s.user_id = $user_id
                RETURN s
                """,
                    session_id=session_id,
                    user_id=user_id,
                )
                record = result.single()
                if record:
                    session_data = record["s"]
                    return WorkflowSession(**session_data)
        except Exception as e:
            logger.debug(f"Exception reading from database: {e}")
            logger.debug("Creating constraint for future transactions")
            self.create()
        return None

    def get_all_session_ids(self, user_id: Optional[str] = None, workflow_id: Optional[str] = None) -> List[str]:
        """
        Get all session IDs, optionally filtered by user_id and/or workflow_id.

        Args:
            user_id (Optional[str]): The ID of the user to filter by.
            workflow_id (Optional[str]): The ID of the workflow to filter by.

        Returns:
            List[str]: List of session IDs matching the criteria.
        """
        try:
            with self._engine.session(database=self.database) as session:
                query = f"""
                MATCH (s:{self.table_name})
                WHERE ($user_id IS NULL OR s.user_id = $user_id)
                  AND ($workflow_id IS NULL OR s.workflow_id = $workflow_id)
                RETURN s.session_id
                ORDER BY s.created_at DESC
                """
                result = session.run(query, user_id=user_id, workflow_id=workflow_id)
                return [record["s.session_id"] for record in result]
        except Exception as e:
            logger.debug(f"Exception reading from database: {e}")
            logger.debug("Creating constraint for future transactions")
            self.create()
        return []

    def get_all_sessions(
        self, user_id: Optional[str] = None, workflow_id: Optional[str] = None
    ) -> List[WorkflowSession]:
        """
        Get all sessions, optionally filtered by user_id and/or workflow_id.

        Args:
            user_id (Optional[str]): The ID of the user to filter by.
            workflow_id (Optional[str]): The ID of the workflow to filter by.

        Returns:
            List[WorkflowSession]: List of WorkflowSession objects matching the criteria.
        """
        try:
            with self._engine.session(database=self.database) as session:
                query = f"""
                MATCH (s:{self.table_name})
                WHERE ($user_id IS NULL OR s.user_id = $user_id)
                  AND ($workflow_id IS NULL OR s.workflow_id = $workflow_id)
                RETURN s
                ORDER BY s.created_at DESC
                """
                result = session.run(query, user_id=user_id, workflow_id=workflow_id)
                return [WorkflowSession(**record["s"]) for record in result]
        except Exception as e:
            logger.debug(f"Exception reading from database: {e}")
            logger.debug("Creating constraint for future transactions")
            self.create()
        return []

    def upsert(self, session: WorkflowSession) -> Optional[WorkflowSession]:
        """
        Insert or update a WorkflowSession in the database.

        Args:
            session (WorkflowSession): The WorkflowSession object to upsert.

        Returns:
            Optional[WorkflowSession]: The upserted WorkflowSession object.
        """
        try:
            with self._engine.session(database=self.database) as db_session:
                query = f"""
                MERGE (s:{self.table_name} {{session_id: $session_id}})
                SET s = $properties,
                    s.updated_at = $updated_at
                RETURN s
                """
                properties = session.model_dump()
                properties["created_at"] = properties.get("created_at", int(time.time()))
                properties["updated_at"] = int(time.time())

                result = db_session.run(
                    query, session_id=session.session_id, properties=properties, updated_at=int(time.time())
                )
                record = result.single()
                if record:
                    return WorkflowSession(**record["s"])
        except Exception as e:
            logger.debug(f"Exception upserting into database: {e}")
            logger.debug("Creating constraint and retrying upsert")
            self.create()
            return self.upsert(session)
        return None

    def delete_session(self, session_id: Optional[str] = None):
        """
        Delete a workflow session from the database.

        Args:
            session_id (Optional[str]): The ID of the session to delete.
        """
        if session_id is None:
            logger.warning("No session_id provided for deletion.")
            return

        try:
            with self._engine.session(database=self.database) as session:
                result = session.run(
                    f"""
                MATCH (s:{self.table_name} {{session_id: $session_id}})
                DELETE s
                RETURN count(s) as deleted_count
                """,
                    session_id=session_id,
                )
                deleted_count = result.single()["deleted_count"]
                if deleted_count == 0:
                    logger.debug(f"No session found with session_id: {session_id}")
                else:
                    logger.debug(f"Successfully deleted session with session_id: {session_id}")
        except Exception as e:
            logger.error(f"Error deleting session: {e}")

    def drop(self) -> None:
        """
        Drop all nodes with the specified label and remove the constraint.
        """
        with self._engine.session(database=self.database) as session:
            session.run(f"MATCH (s:{self.table_name}) DETACH DELETE s")
            session.run(f"DROP CONSTRAINT IF EXISTS FOR (s:{self.table_name}) REQUIRE s.session_id IS UNIQUE")

    def upgrade_schema(self) -> None:
        """
        Upgrade the schema of the workflow storage.
        This method is currently a placeholder and does not perform any actions.
        """
        pass

    def __deepcopy__(self, memo):
        """
        Create a deep copy of the Neo4jWorkflowStorage instance, handling unpickleable attributes.

        Args:
            memo (dict): A dictionary of objects already copied during the current copying pass.

        Returns:
            Neo4jWorkflowStorage: A deep-copied instance of Neo4jWorkflowStorage.
        """
        from copy import deepcopy

        cls = self.__class__
        copied_obj = cls.__new__(cls)
        memo[id(self)] = copied_obj

        for k, v in self.__dict__.items():
            if k == "_engine":
                # Reuse the driver without copying
                setattr(copied_obj, k, v)
            else:
                setattr(copied_obj, k, deepcopy(v, memo))

        return copied_obj

    def close(self):
        """
        Close the database connection.
        """
        self._engine.close()
