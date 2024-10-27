# phi/vectordb/neo4j/neo4j.py
from typing import List, Optional, Union, Dict, Any
from neo4j import GraphDatabase, Driver
from phi.document import Document
from phi.vectordb.base import VectorDb
from phi.embedder import Embedder
from phi.utils.log import logger


class Neo4jVectorDb(VectorDb):
    """Neo4j Vector Database Implementation"""

    def __init__(
        self,
        uri: str,
        user: str,
        password: str,
        database: str = "neo4j",
        embedder: Optional[Embedder] = None,
        search_type: SearchType = SearchType.vector,
        vector_index: Union[Neo4jNativeIndex, HNSW] = HNSW(),
        distance: Neo4jDistance = Neo4jDistance.cosine,
        schema_version: int = 1,
        auto_upgrade_schema: bool = False,
    ):
        """
        Initialize the Neo4jVectorDb instance.

        Args:
            uri (str): The URI of the Neo4j database.
            user (str): The username for authentication.
            password (str): The password for authentication.
            database (str): The name of the Neo4j database to use. Defaults to "neo4j".
            embedder (Optional[Embedder]): The embedder to use for vector operations.
            search_type (SearchType): The type of search to perform. Defaults to vector search.
            distance (Neo4jDistance): The distance metric to use. Defaults to cosine distance.
            schema_version (int): Version of the schema. Defaults to 1.
            auto_upgrade_schema (bool): Whether to automatically upgrade the schema.
        """
        self.driver: Driver = GraphDatabase.driver(uri, auth=(user, password))
        self.database: str = database
        self.embedder: Optional[Embedder] = embedder
        self.search_type: SearchType = search_type
        self.distance: Neo4jDistance = distance
        self.schema_version: int = schema_version
        self.auto_upgrade_schema: bool = auto_upgrade_schema
        vector_index: Union[Neo4jNativeIndex, HNSW] = vector_index
        self._check_gds_availability()
        logger.debug(f"Created Neo4jVectorDb: '{self.database}'")

    def create(self) -> None:
        """
        Create the constraint for unique document id if it doesn't exist.
        """
        with self.driver.session(database=self.database) as session:
            session.run("""
            CREATE CONSTRAINT ON (d:Document) ASSERT d.id IS UNIQUE;
            """)
        
        self.create_index()

    def create_index(self):
        if isinstance(self.vector_index, HNSW):
            index_config = self.vector_index.to_dict()
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

        with self.driver.session(database=self.database) as session:
            session.run(cypher_query)

    def doc_exists(self, document: Document) -> bool:
        """
        Check if a document with the given content hash exists.

        Args:
            document (Document): The document to check.

        Returns:
            bool: True if the document exists, False otherwise.
        """
        with self.driver.session(database=self.database) as session:
            result = session.run("""
            MATCH (d:Document {content_hash: $hash}) RETURN d
            """, hash=document.content_hash)
            return result.single() is not None

    def name_exists(self, name: str) -> bool:
        """
        Check if a document with the given name exists.

        Args:
            name (str): The name to check.

        Returns:
            bool: True if a document with the given name exists, False otherwise.
        """
        with self.driver.session(database=self.database) as session:
            result = session.run("""
            MATCH (d:Document {name: $name}) RETURN d
            """, name=name)
            return result.single() is not None

    def id_exists(self, id: str) -> bool:
        """
        Check if a document with the given id exists.

        Args:
            id (str): The id to check.

        Returns:
            bool: True if a document with the given id exists, False otherwise.
        """
        with self.driver.session(database=self.database) as session:
            result = session.run("""
            MATCH (d:Document {id: $id}) RETURN d
            """, id=id)
            return result.single() is not None

    def insert(self, documents: List[Document], filters: Optional[Dict[str, Any]] = None) -> None:
        """
        Insert documents into the database.

        Args:
            documents (List[Document]): The documents to insert.
            filters (Optional[Dict[str, Any]]): Optional filters for the insertion.
        """
        with self.driver.session(database=self.database) as session:
            for doc in documents:
                session.run("""
                CREATE (d:Document {
                    id: $id,
                    name: $name,
                    content: $content,
                    content_hash: $hash,
                    embedding: $embedding
                })
                """, id=doc.id, name=doc.name, content=doc.content,
                hash=doc.content_hash, embedding=doc.embedding)

    def upsert(self, documents: List[Document], filters: Optional[Dict[str, Any]] = None) -> None:
        """
        Insert or update documents in the database.

        Args:
            documents (List[Document]): The documents to upsert.
            filters (Optional[Dict[str, Any]]): Optional filters for the upsert operation.
        """
        with self.driver.session(database=self.database) as session:
            for doc in documents:
                session.run("""
                MERGE (d:Document {id: $id})
                SET d.name = $name,
                    d.content = $content,
                    d.content_hash = $hash,
                    d.embedding = $embedding
                """, id=doc.id, name=doc.name, content=doc.content,
                hash=doc.content_hash, embedding=doc.embedding)

    def search(self, query: str, limit: int = 5, filters: Optional[Dict[str, Any]] = None) -> List[Document]:
        """
        Search for documents based on the query and search type.

        Args:
            query (str): The search query.
            limit (int): The maximum number of results to return. Defaults to 5.
            filters (Optional[Dict[str, Any]]): Optional filters for the search.

        Returns:
            List[Document]: A list of matching documents.
        """
        if self.search_type == SearchType.vector:
            return self._vector_search(query, limit, filters)
        elif self.search_type == SearchType.keyword:
            return self._keyword_search(query, limit, filters)
        elif self.search_type == SearchType.hybrid:
            return self._hybrid_search(query, limit, filters)
        else:
            raise ValueError(f"Unsupported search type: {self.search_type}")

    def drop_index(self):
        if isinstance(self.vector_index, HNSW):
            cypher_query = f"""
            CALL gds.index.vector.drop('{self.label}_{self.property}_hnsw')
            """
        elif isinstance(self.vector_index, Neo4jNativeIndex):
            cypher_query = f"DROP INDEX ON :{self.label}({self.property})"
        else:
            raise ValueError(f"Unsupported index type: {type(self.vector_index)}")

        with self.driver.session(database=self.database) as session:
            session.run(cypher_query)

    def drop(self) -> None:
        """
        Drop all Document nodes, their relationships and index.
        """
        self.drop_index()
        with self.driver.session(database=self.database) as session:
            session.run("MATCH (d:Document) DETACH DELETE d")

    def exists(self) -> bool:
        """
        Check if the Document label exists in the database.

        Returns:
            bool: True if the Document label exists, False otherwise.
        """
        with self.driver.session(database=self.database) as session:
            result = session.run("CALL db.labels() YIELD label RETURN label")
            return "Document" in [record["label"] for record in result]

    def delete(self) -> bool:
        """
        Delete all Document nodes and their relationships.

        Returns:
            bool: True if the operation was successful, False otherwise.
        """
        try:
            self.drop()
            return True
        except Exception as e:
            logger.error(f"Error deleting documents: {e}")
            return False

    def close(self):
        """
        Close the database connection.
        """
        self.driver.close()

    def upgrade_schema(self) -> None:
        """
        Upgrade the schema of the vector storage.
        This method is currently a placeholder and does not perform any actions.
        """
        pass

    def _vector_search(self, query: str, limit: int, filters: Optional[Dict[str, Any]]) -> List[Document]:
        query_embedding = self.embedder.embed_query(query)
    
        if isinstance(self.vector_index, HNSW):
            cypher_query = f"""
            CALL gds.index.vector.queryHNSW(
                '{self.label}_{self.property}_hnsw',
                $query_embedding,
                {{
                    topK: $limit,
                    filter: {self._build_filter_conditions(filters)}
                }}
            ) YIELD node, score
            RETURN node, score
            """
        else:
            # Fallback auf native Suche, wenn kein HNSW-Index verwendet wird
            cypher_query = f"""
            MATCH (d:{self.label})
        WHERE {self._build_filter_conditions(filters)}
            WITH d, {self._distance_function()}(d.{self.property}, $query_embedding) AS distance
            ORDER BY distance
            LIMIT $limit
            RETURN d, distance AS score
            """
    
        with self.driver.session(database=self.database) as session:
            results = session.run(cypher_query, query_embedding=query_embedding, limit=limit)
            return [self._create_document_from_record(record['node'], score=record['score']) for record in results]

    def _keyword_search(self, query: str, limit: int, filters: Optional[Dict[str, Any]]) -> List[Document]:
        """
        Perform a keyword search.
        """
        cypher_query = f"""
        MATCH (d:Document)
        WHERE d.content CONTAINS $query AND {self._build_filter_conditions(filters)}
        RETURN d
        LIMIT $limit
        """
        
        with self.driver.session(database=self.database) as session:
            results = session.run(cypher_query, query=query, limit=limit)
            return [self._create_document_from_record(record['d']) for record in results]

    def _hybrid_search(self, query: str, limit: int, filters: Optional[Dict[str, Any]]) -> List[Document]:
        """
        Perform a hybrid search combining vector and keyword search.
        """
        if self.embedder is None:
            raise ValueError("Embedder is required for hybrid search")
        
        query_embedding = self.embedder.embed_query(query)
        
        cypher_query = f"""
        MATCH (d:Document)
        WHERE {self._build_filter_conditions(filters)}
        WITH d, 
             {self._distance_function()}(d.embedding, $query_embedding) AS vector_distance,
             CASE WHEN d.content CONTAINS $query THEN 0 ELSE 1 END AS keyword_match
        ORDER BY keyword_match, vector_distance
        LIMIT $limit
        RETURN d
        """
        
        with self.driver.session(database=self.database) as session:
            results = session.run(cypher_query, query_embedding=query_embedding, query=query, limit=limit)
            return [self._create_document_from_record(record['d']) for record in results]

    def _distance_function(self) -> str:
        """
        Return the appropriate distance function based on the selected distance metric.
        """
        if self.distance == Neo4jDistance.cosine:
            return "gds.similarity.cosine"
        elif self.distance == Neo4jDistance.euclidean:
            return "gds.similarity.euclidean"
        elif self.distance == Neo4jDistance.manhattan:
            return "gds.similarity.manhattan"
        else:
            raise ValueError(f"Unsupported distance function: {self.distance}")

    def _build_filter_conditions(self, filters: Optional[Dict[str, Any]]) -> str:
        """
        Build the filter conditions for the Cypher query.
        """
        if not filters:
            return "1=1"
        conditions = []
        for key, value in filters.items():
            conditions.append(f"d.{key} = '{value}'")
        return " AND ".join(conditions)

    def _create_document_from_record(self, record: Dict[str, Any]) -> Document:
        """
        Create a Document object from a Neo4j record.
        """
        return Document(
            id=record["id"],
            name=record["name"],
            content=record["content"],
            embedding=record["embedding"],
            metadata=record.get("metadata", {})
        )
    def _check_gds_availability(self):
        query = "CALL gds.list() YIELD name RETURN count(*) > 0 AS gds_available"
        with self.driver.session(database=self.database) as session:
            result = session.run(query)
            gds_available = result.single()["gds_available"]
            if not gds_available:
                raise RuntimeError("Graph Data Science library is not available in Neo4j. Please install it.")
