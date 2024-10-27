# phi/vectordb/neo4j/distance.py
from enum import Enum

class Neo4jDistance(str, Enum):
    cosine = "cosine"
    euclidean = "euclidean"
    manhattan = "manhattan"
