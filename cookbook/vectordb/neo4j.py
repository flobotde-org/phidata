# cookbook/vectordb/neo4j.py
from phi.agent import Agent
from phi.knowledge.pdf import PDFUrlKnowledgeBase
from phi.vectordb.neo4j import Neo4jVectorDb
from langchain_community.embeddings import OllamaEmbeddings

# connect to Neo4j-Database
# start one with cookbook/run_neo4j.sh if you haven't already or connect to a free aura instance
url= "bolt://localhost:7687"
username="neo4j"
password="phi-neo4j"
embeddings = OllamaEmbeddings(model="llama3.2",)

knowledge_base = PDFUrlKnowledgeBase(
    urls=["https://phi-public.s3.amazonaws.com/recipes/ThaiRecipes.pdf"],
    vector_db=Neo4jVectorDb(
        url=url,
        username=username,
        password=password,
        embedder=embeddings,
        index_name="thaicurry",
        node_label="thai_curry_recipes"
    ),
)
knowledge_base.load(recreate=False)  # Comment out after first run

agent = Agent(knowledge_base=knowledge_base, use_tools=True, show_tool_calls=True)
agent.print_response("How to make Thai curry?", markdown=True)
