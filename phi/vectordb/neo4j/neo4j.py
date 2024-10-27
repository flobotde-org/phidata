# phi/vectordb/neo4j/neo4j.py
from typing import Optional, List, Dict, Any, Union
from neo4j import GraphDatabase, Driver, ManagedTransaction
from phi.document import Document
from phi.embedder import Embedder
from phi.vectordb.base import VectorDb
from phi.vectordb.distance import Distance
from phi.vectordb.search import SearchType
from phi.vectordb.neo4j.index import VectorIndex, Neo4jNativeIndex, HNSW
import hashlib


class Neo4jVector(VectorDb):
    def __init__(
        self,
        uri: str,
        username: str,
        password: str,
        database: str = "neo4j",
        embedder: Optional[Embedder] = None,
        search_type: SearchType = SearchType.vector,
        vector_index: VectorIndex = HNSW(),
        distance: Distance = Distance.cosine,
        label: str = "Document",
        property: str = "embedding",
    ):
        self.driver: Driver = GraphDatabase.driver(uri, auth=(username, password))
        self.database = database
        self.embedder = embedder
        self.search_type = search_type
        self.vector_index = vector_index
        self.distance = distance
        self.label = label
        self.property = property

    def _transaction_execute(
        self, /, tx: ManagedTransaction, query: str, parameters: Optional[Dict[str, Any]] = None
    ) -> Any:
        return tx.run(query, parameters).data()

    def execute_write(self, query: str, parameters: Optional[Dict[str, Any]] = None) -> Any:
        with self.driver.session(database=self.database) as session:
            return session.write_transaction(self._transaction_execute, query, parameters)

    def execute_read(self, query: str, parameters: Optional[Dict[str, Any]] = None) -> Any:
        with self.driver.session(database=self.database) as session:
            return session.read_transaction(self._transaction_execute, query, parameters)

    def create_index(self):
        if isinstance(self.vector_index, HNSW):
            index_config = self.vector_index.model_dump()
            cypher_query = f"""
            CALL gds.index.vector.create(
                '{self.label}_{self.property}_hnsw',
                '{self.label}',
                '{self.property}',
                {index_config}
            )
            """
        elif isinstance(self.vector_index, Neo4jNativeIndex):
            cypher_query = f"CREATE INDEX ON :{self.label}({self.property})"
        else:
            raise ValueError(f"Unsupported index type: {type(self.vector_index)}")

        self.execute_write(cypher_query)

    def create(self):
        self.execute_write(f"CREATE CONSTRAINT IF NOT EXISTS ON (d:{self.label}) ASSERT d.id IS UNIQUE")
        self.create_index()

    def insert(self, documents: List[Document]) -> List[str]:
        inserted_ids = []
        for doc in documents:
            content_hash = hashlib.md5(doc.content.encode()).hexdigest()
            embedding = self.embedder.embed_query(doc.content) if self.embedder else None
            cypher_query = f"""
            CREATE (d:{self.label} {{
                id: $id,
                content: $content,
                metadata: $metadata,
                embedding: $embedding,
                content_hash: $content_hash
            }})
            RETURN d.id
            """
            result = self.execute_write(
                cypher_query,
                {
                    "id": doc.id,
                    "content": doc.content,
                    "metadata": doc.metadata,
                    "embedding": embedding,
                    "content_hash": content_hash,
                },
            )
            inserted_ids.append(result[0]["d.id"])
        return inserted_ids

    def upsert(self, documents: List[Document]) -> List[str]:
        upserted_ids = []
        for doc in documents:
            content_hash = hashlib.md5(doc.content.encode()).hexdigest()
            embedding = self.embedder.embed_query(doc.content) if self.embedder else None
            cypher_query = f"""
            MERGE (d:{self.label} {{id: $id}})
            ON CREATE SET
                d.content = $content,
                d.metadata = $metadata,
                d.embedding = $embedding,
                d.content_hash = $content_hash
            ON MATCH SET
                d.content = $content,
                d.metadata = $metadata,
                d.embedding = $embedding,
                d.content_hash = $content_hash
            RETURN d.id
            """
            result = self.execute_write(
                cypher_query,
                {
                    "id": doc.id,
                    "content": doc.content,
                    "metadata": doc.metadata,
                    "embedding": embedding,
                    "content_hash": content_hash,
                },
            )
            upserted_ids.append(result[0]["d.id"])
        return upserted_ids

    def delete(self, document_ids: List[str]) -> None:
        cypher_query = f"""
        MATCH (d:{self.label})
        WHERE d.id IN $ids
        DELETE d
        """
        self.execute_write(cypher_query, {"ids": document_ids})

    def drop_index(self):
        if isinstance(self.vector_index, HNSW):
            cypher_query = f"""
            CALL gds.index.vector.drop('{self.label}_{self.property}_hnsw')
            """
        elif isinstance(self.vector_index, Neo4jNativeIndex):
            cypher_query = f"DROP INDEX ON :{self.label}({self.property})"
        else:
            raise ValueError(f"Unsupported index type: {type(self.vector_index)}")

        self.execute_write(cypher_query)

    def drop(self):
        self.drop_index()
        self.execute_write(f"MATCH (d:{self.label}) DELETE d")

    def _vector_search(self, query: str, limit: int, filters: Optional[Dict[str, Any]] = None) -> List[Document]:
        if self.embedder is None:
            raise ValueError("Embedder is required for vector search")
        query_embedding = self.embedder.embed_query(query)

        if isinstance(self.vector_index, HNSW):
            cypher_query = f"""
            CALL gds.index.vector.queryHNSW(
                '{self.label}_{self.property}_hnsw',
                $query_embedding,
                {{
                    topK: $limit,
                    filter: {self._build_filter_conditions(filters) if filters else ''}
                }}
            ) YIELD node, score
            RETURN node, score
            """
        else:
            cypher_query = f"""
            MATCH (d:{self.label})
            WHERE {self._build_filter_conditions(filters) if filters else 'true'}
            WITH d, gds.similarity.cosine(d.{self.property}, $query_embedding) AS score
            ORDER BY score DESC
            LIMIT $limit
            RETURN d AS node, score
            """

        results = self.execute_read(cypher_query, {"query_embedding": query_embedding, "limit": limit})
        return [self._create_document_from_record(record["node"], record["score"]) for record in results]

    def _keyword_search(self, query: str, limit: int, filters: Optional[Dict[str, Any]] = None) -> List[Document]:
        cypher_query = f"""
        MATCH (d:{self.label})
        WHERE d.content CONTAINS $query {f'AND {self._build_filter_conditions(filters)}' if filters else ''}
        RETURN d AS node, 1 AS score
        LIMIT $limit
        """
        results = self.execute_read(cypher_query, {"query": query, "limit": limit})
        return [self._create_document_from_record(record["node"], record["score"]) for record in results]

    def _hybrid_search(self, query: str, limit: int, filters: Optional[Dict[str, Any]] = None) -> List[Document]:
        vector_results = self._vector_search(query, limit, filters)
        keyword_results = self._keyword_search(query, limit, filters)

        # Combine and sort results (you may want to implement a more sophisticated ranking method)
        combined_results = sorted(
            vector_results + keyword_results, key=lambda x: x.metadata.get("score", 0), reverse=True
        )
        return combined_results[:limit]

    def _create_document_from_record(self, node: Dict[str, Any], score: float) -> Document:
        return Document(
            id=node["id"],
            content=node["content"],
            metadata={**node["metadata"], "score": score} if node["metadata"] else {"score": score},
        )

    def _build_filter_conditions(self, filters: Optional[Dict[str, Any]]) -> str:
        if not filters:
            return ""
        conditions = []
        for key, value in filters.items():
            if isinstance(value, str):
                conditions.append(f"d.{key} = '{value}'")
            else:
                conditions.append(f"d.{key} = {value}")
        return " AND ".join(conditions)

    def close(self):
        self.driver.close()
