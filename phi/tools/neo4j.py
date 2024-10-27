# /phi/tools/neo4j.py
from neo4j import GraphDatabase, Neo4jDriver, Transaction
from typing import Optional, Dict, Any
from phi.tools import Toolkit
from phi.utils.log import logger


class Neo4jTools(Toolkit):
    """A basic tool to connect to a Neo4j database and perform read-only operations on it."""

    def __init__(
        self,
        uri: str,
        user: str,
        password: str,
        run_queries: bool = True,
        inspect_queries: bool = False,
        summarize_nodes: bool = True,
        export_nodes: bool = False,
    ):
        super().__init__(name="neo4j_tools")
        self.uri: str = uri
        self.user: str = user
        self.password: str = password
        self.driver: Neo4jDriver = GraphDatabase.driver(self.uri, auth=(self.user, self.password))

        self.register(self.show_nodes)
        self.register(self.describe_node)
        if inspect_queries:
            self.register(self.inspect_query)
        if run_queries:
            self.register(self.run_query)
        if summarize_nodes:
            self.register(self.summarize_node)
        if export_nodes:
            self.register(self.export_node_to_path)

    def close(self):
        """Close the connection to the database."""
        self.driver.close()

    def _execute_read(self, query: str, parameters: Optional[Dict[str, Any]] = None):
        with self.driver.session() as session:
            return session.read_transaction(self._transaction_execute, query, parameters)

    def _execute_write(self, query: str, parameters: Optional[Dict[str, Any]] = None):
        with self.driver.session() as session:
            return session.write_transaction(self._transaction_execute, query, parameters)

    @staticmethod
    def _transaction_execute(tx: Transaction, query: str, parameters: Optional[Dict[str, Any]] = None):
        return tx.run(query, parameters).data()

    def show_nodes(self) -> str:
        """Function to show node labels in the database

        :return: List of node labels in the database
        """
        query = "CALL db.labels()"
        nodes = self._execute_read(query)
        logger.debug(f"Node labels: {nodes}")
        return nodes

    def describe_node(self, label: str) -> str:
        """Function to describe a node label

        :param label: Node label to describe
        :return: Description of the node label
        """
        query = f"MATCH (n:{label}) RETURN keys(n) LIMIT 1"
        node_description = self._execute_read(query)
        logger.debug(f"Node description: {node_description}")
        return f"{label}\n{node_description}"

    def summarize_node(self, label: str) -> str:
        """Function to compute a summary of nodes with a specific label.

        :param label: Node label to summarize
        :return: Summary of the nodes
        """
        query = f"""
        MATCH (n:{label})
        RETURN count(n) AS count, avg(size(keys(n))) AS avg_properties
        """
        node_summary = self._execute_read(query)
        logger.debug(f"Node summary: {node_summary}")
        return node_summary

    def inspect_query(self, query: str) -> str:
        """Function to inspect a query and return the query plan.

        :param query: Query to inspect
        :return: Query plan
        """
        query = f"EXPLAIN {query}"
        explain_plan = self._execute_read(query)
        logger.debug(f"Explain plan: {explain_plan}")
        return explain_plan

    def export_node_to_path(self, label: str, path: Optional[str] = None) -> str:
        """Save nodes with a specific label in CSV format.

        :param label: Node label to export
        :param path: Path to export to
        :return: None
        """
        logger.debug(f"Exporting Nodes with label {label} as CSV to path {path}")
        if path is None:
            path = f"{label}.csv"
        else:
            path = f"{path}/{label}.csv"

        query = f"MATCH (n:{label}) RETURN n"
        nodes = self._execute_read(query)

        with open(path, "w") as file:
            for node in nodes:
                file.write(f"{node}\n")

        logger.debug(f"Exported nodes with label {label} to {path}")
        return f"Exported nodes with label {label} to {path}"

    def run_query(self, query: str) -> str:
        """Function that runs a query and returns the result.

        :param query: Cypher query to run
        :return: Result of the query
        """
        try:
            logger.info(f"Running: {query}")
            result = self._execute_read(query)
            logger.debug(f"Query result: {result}")
            return result
        except Exception as e:
            logger.error(f"Error running query: {e}")
            return str(e)


# Usage Example:
# tools = Neo4jTools("bolt://localhost:7687", "neo4j", "password")
# tools.show_nodes()
# tools.describe_node("Person")
# tools.summarize_node("Person")
# tools.export_node_to_path("Person", "/tmp")
# tools.close()
