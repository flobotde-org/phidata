# phi/vectordb/neo4j/index.py
from typing import Dict, Any
from pydantic import BaseModel

class Neo4jNativeIndex(BaseModel):
    pass

class HNSW(BaseModel):
    m: int = 16
    ef_construction: int = 200
    max_connections: int = 16
    configuration: Dict[str, Any] = {
        "maintenance_work_mem": "2GB",
    }
