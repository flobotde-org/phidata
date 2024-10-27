# cookbook/vectordb/neo4j.py
from phi.agent import Agent
from phi.knowledge.pdf import PDFUrlKnowledgeBase
from phi.vectordb.neo4j import Neo4jVector

# connect to Neo4j-Database
# start one with cookbook/run_neo4j.sh if you haven't already or connect to a free aura instance
db_url = "bolt://neo4j:phi-neo4j@localhost:7687"

knowledge_base = PDFUrlKnowledgeBase(
    urls=["https://phi-public.s3.amazonaws.com/recipes/ThaiRecipes.pdf"],
    vector_db=Neo4jVector(table_name="recipes", db_url=db_url),
)
knowledge_base.load(recreate=False)  # Comment out after first run

agent = Agent(knowledge_base=knowledge_base, use_tools=True, show_tool_calls=True)
agent.print_response("How to make Thai curry?", markdown=True)
