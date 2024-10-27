# phi/vectordb/neo4j/neo4j.py
from typing import Optional, List
from neo4j import GraphDatabase, Driver
from phi.document import Document
from phi.embedder import Embedder
from phi.vectordb.base import VectorDb
from phi.vectordb.search import SearchType
from phi.vectordb.neo4j.index import VectorIndex, Neo4jNativeIndex, HNSW
from phi.vectordb.neo4j.distance import Neo4jDistance
import hashlib
import time


class Neo4jVector(VectorDb):
    def __init__(
        self,
        table_name: str,
        uri: str,
        username: str,
        password: str,
        database: str = "neo4j",
        embedder: Optional[Embedder] = None,
        search_type: SearchType = SearchType.vector,
        vector_index: VectorIndex = HNSW(),
        distance: Neo4jDistance = Neo4jDistance.cosine,  # Verwendung von Neo4jDistance
    ):
        if not uri or not username or not password:
            raise ValueError("URI, username, and password must be provided.")

        self.driver: Driver = GraphDatabase.driver(uri, auth=(username, password))
        self.database = database
        self.embedder = embedder if embedder else Embedder()
        self.table_name = table_name
        self.search_type = search_type
        self.vector_index = vector_index
        self.distance = distance  # Speicherung der Distanzmethode

    def create(self):
        with self.driver.session(database=self.database) as session:
            session.run(f"CREATE CONSTRAINT IF NOT EXISTS ON (d:{self.table_name}) ASSERT d.id IS UNIQUE")
            self.create_index()

    def create_index(self):
        if isinstance(self.vector_index, HNSW):
            index_config = self.vector_index.to_dict()
            cypher_query = f"""
            CALL gds.index.vector.create(
                '{self.table_name}_hnsw',
                '{self.table_name}',
                'embedding',
                $index_config
            )
            """
            with self.driver.session(database=self.database) as session:
                session.run(cypher_query, index_config=index_config)
        elif isinstance(self.vector_index, Neo4jNativeIndex):
            cypher_query = f"CREATE INDEX ON :{self.table_name}(embedding)"
            with self.driver.session(database=self.database) as session:
                session.run(cypher_query)

    def drop_index(self):
        if isinstance(self.vector_index, HNSW):
            cypher_query = f"""
            CALL gds.index.vector.drop('{self.table_name}_hnsw')
            """
        elif isinstance(self.vector_index, Neo4jNativeIndex):
            cypher_query = f"DROP INDEX ON :{self.table_name}(embedding)"

        with self.driver.session(database=self.database) as session:
            session.run(cypher_query)

    def drop(self):
        """Drop the entire table."""
        cypher_query = f"MATCH (d:{self.table_name}) DELETE d"
        with self.driver.session(database=self.database) as session:
            session.run(cypher_query)

        # Drop the index after deleting the documents
        self.drop_index()

    def insert(self, documents: List[Document]) -> List[str]:
        inserted_ids = []
        for doc in documents:
            content_hash = hashlib.md5(doc.content.encode()).hexdigest()
            embedding = self.embedder.embed_query(doc.content) if self.embedder else None

            cypher_query = f"""
            CREATE (d:{self.table_name} {{
                id: $id,
                content: $content,
                metadata: $metadata,
                embedding: $embedding,
                content_hash: $content_hash,
                created_at: $created_at,
                updated_at: $updated_at
            }})
            RETURN d.id AS id
            """

            with self.driver.session(database=self.database) as session:
                result = session.run(
                    cypher_query,
                    id=doc.id,
                    content=doc.content,
                    metadata=doc.metadata,
                    embedding=embedding,
                    content_hash=content_hash,
                    created_at=int(time.time()),
                    updated_at=int(time.time()),
                )

                inserted_ids.append(result.single()["id"])

        return inserted_ids

    def search(self, query: str, limit: int = 5) -> List[Document]:
        if self.search_type == SearchType.vector:
            return self._vector_search(query, limit)
        elif self.search_type == SearchType.keyword:
            return self._keyword_search(query, limit)
        elif self.search_type == SearchType.hybrid:
            return self._hybrid_search(query, limit)

    def _vector_search(self, query: str, limit: int) -> List[Document]:
        query_embedding = self.embedder.embed_query(query)

        # Verwenden der Distanzmethode aus Neo4jDistance
        distance_method = "gds.similarity." + str(self.distance.value)

        cypher_query = f"""
        CALL {distance_method}.queryHNSW(
            '{self.table_name}_hnsw',
            $query_embedding,
            {{
                topK: $limit
            }}
        ) YIELD node AS id 
        RETURN id 
        LIMIT $limit
        """

        with self.driver.session(database=self.database) as session:
            result = session.run(cypher_query, query_embedding=query_embedding, limit=limit)
            return [self._create_document_from_record(record["id"]) for record in result]

    def _keyword_search(self, query: str, limit: int) -> List[Document]:
        cypher_query = f"""
        MATCH (d:{self.table_name})
        WHERE d.content CONTAINS $query 
        RETURN d AS node 
        LIMIT $limit
        """

        with self.driver.session(database=self.database) as session:
            result = session.run(cypher_query, query=query)
            return [self._create_document_from_record(record["node"]) for record in result]

    def _hybrid_search(self, query: str, limit: int) -> List[Document]:
        vector_results = self._vector_search(query, limit)
        keyword_results = self._keyword_search(query, limit)

        combined_results = sorted(
            vector_results + keyword_results, key=lambda x: x.metadata.get("score", 0), reverse=True
        )

        return combined_results[:limit]

    def _create_document_from_record(self, node_id: str) -> Document:
        # Placeholder for creating a Document from a Neo4j node record.
        return Document(
            id=node_id,
            content="",  # Populate based on your actual data structure.
            metadata={},  # Populate based on your actual data structure.
        )

    def close(self):
        """Close the database connection."""
        if hasattr(self.driver, "close"):
            self.driver.close()
