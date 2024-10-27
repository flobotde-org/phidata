# phi/vectordb/neo4j/neo4j.py
from typing import List, Optional, Dict, Any, Callable
from hashlib import md5
import logging

from phi.document import Document as PhiDocument
from phi.vectordb.base import VectorDb
from langchain_community.vectorstores.neo4j_vector import Neo4jVector
from langchain.embeddings.base import Embeddings  # Base class for embedders
from langchain_community.vectorstores.neo4j_vector import SearchType, DistanceStrategy, IndexType
from langchain_core.documents.base import Document as LangchainDocument  # Import LangChain's Document

logger = logging.getLogger(__name__)


class Neo4jVectorDb(VectorDb):
    def __init__(
        self,
        url: str,
        username: Optional[str],
        password: Optional[str],
        index_name: str,
        node_label: str,
        embedder: Embeddings,
        search_type: SearchType = SearchType.HYBRID,
        keyword_index_name: Optional[str] = None,
        database: Optional[str] = None,
        embedding_node_property: str = "embedding",
        text_node_property: str = "content",
        distance_strategy: DistanceStrategy = DistanceStrategy.COSINE,
        pre_delete_collection: bool = False,
        retrieval_query: Optional[str] = None,
        relevance_score_fn: Optional[Callable[[float], float]] = None,
        index_type: IndexType = IndexType.NODE,
    ):
        """
        Initialize a Neo4jVectorDb instance with connection and collection settings.

        :param url: URL of the Neo4j database.
        :param username: Username for Neo4j authentication.
        :param password: Password for Neo4j authentication.
        :param index_name: Name of the vector index.
        :param node_label: Label for nodes in the Neo4j graph.
        :param embedder: An instance of an embedder compatible with LangChain.
        :param search_type: Type of search to perform. HYBRID, VECTOR
        :param keyword_index_name: Name of the keyword index, if any.
        :param database: Name of the Neo4j database.
        :param embedding_node_property: Property name for embeddings in nodes.
        :param text_node_property: Property name for text in nodes.
        :param distance_strategy: Strategy for calculating distance. COSINE, JACCARD, DOT_PRODUCT, MAX_INNER_PRODUCT, EUCLIDEAN_DISTANCE
        :param pre_delete_collection: Whether to delete the collection before creating a new one.
        :param retrieval_query: Custom query for retrieval.
        :param relevance_score_fn: Function to calculate relevance scores.
        :param index_type: Type of index to use. NODE, RELATIONSHIP
        """
        # Set up Neo4j and embedding configurations
        self.url = url
        self.username = username
        self.password = password
        self.index_name = index_name
        self.node_label = node_label
        self.embedder = embedder

        # Instantiate Neo4jVector for vector operations
        self.vector_store = Neo4jVector(
            embedding=self.embedder,
            search_type=search_type,
            username=username,
            password=password,
            url=url,
            keyword_index_name=keyword_index_name,
            database=database,
            index_name=index_name,
            node_label=node_label,
            embedding_node_property=embedding_node_property,
            text_node_property=text_node_property,
            distance_strategy=distance_strategy,
            logger=logger,
            pre_delete_collection=pre_delete_collection,
            retrieval_query=retrieval_query or "",
            relevance_score_fn=relevance_score_fn,
            index_type=index_type,
        )

    def create(self) -> None:
        """Create a graph structure if it does not exist."""
        if not self.vector_store.retrieve_existing_index():
            logger.debug(f"Creating collection: {self.index_name}")
            self.vector_store.create_new_index()
        else:
            logger.debug(f"Collection already exists: {self.index_name}")

    def doc_exists(self, document: PhiDocument) -> bool:
        """
        Check if a document exists in the Neo4j collection.

        :param document: Document instance to check.
        :return: True if the document exists, False otherwise.
        """
        result = self.vector_store.similarity_search(document.content, k=1)
        return len(result) > 0

    def name_exists(self, name: str) -> bool:
        """
        Check if a document with the specified name exists.

        Note: Direct name lookup may not be supported without additional indexing.

        :param name: Name of the document to search for.
        """
        logger.warning("Direct name lookup is not implemented without metadata indexing.")
        return False

    def id_exists(self, id: str) -> bool:
        """
        Check if a document with the given ID exists in the document store.

        :param id: ID to check.
        :return: True if document exists, False otherwise.
        """
        result = self.vector_store.get_by_ids([id])
        return len(result) > 0

    def insert(self, documents: List[PhiDocument], filters: Optional[Dict[str, Any]] = None) -> None:
        """
        Insert documents into the Neo4j vector store.

        :param documents: List of PhiDocument objects to insert.
        """
        logger.debug(f"Inserting {len(documents)} documents")
        for doc in documents:
            # Embed the document using LangChain's embedder
            embedding = self.embedder.embed_query(doc.content)
            doc.embedding = embedding
            doc_id = md5(doc.content.encode()).hexdigest()
            # Create a new LangChain Document object for each entry
            new_doc = LangchainDocument(id=doc_id, page_content=doc.content, metadata=doc.meta_data)
            self.vector_store.add_documents([new_doc])

    def upsert(self, documents: List[PhiDocument], filters: Optional[Dict[str, Any]] = None) -> None:
        """
        Upsert documents into the database.

        :param documents: List of PhiDocument objects to upsert.
        """
        self.insert(documents, filters)

    def search(self, query: str, limit: int = 5, filters: Optional[Dict[str, Any]] = None) -> List[PhiDocument]:
        """
        Search for documents matching the query.

        :param query: Search query string.
        :param limit: Limit on the number of results returned.
        :return: List of PhiDocument objects matching the query.
        """
        query_embedding = self.embedder.embed_query(query)
        if query_embedding is None:
            logger.error(f"Error generating embedding for the query: {query}")
            return []

        results = self.vector_store.similarity_search_by_vector(query_embedding, k=limit)
        return [PhiDocument(id=res.id, content=res.page_content, meta_data=res.metadata) for res in results]

    def drop(self) -> None:
        """Drop the collection's index in Neo4j."""
        if self.exists():
            logger.debug(f"Dropping collection: {self.index_name}")
            self.vector_store.delete()

    def exists(self) -> bool:
        """Check if the collection index exists in the Neo4j database."""
        return self.vector_store.retrieve_existing_index() is not None

    def optimize(self) -> None:
        """Optimize the vector store (if applicable)."""
        logger.info("Optimize not implemented for Neo4j vector indexing.")

    def delete(self) -> bool:
        """Clear all entries from the current collection."""
        self.drop()
        return True
