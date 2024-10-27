# cookbook/integrations/neo4j/agent.py
from phi.agent import Agent
from phi.storage.agent.neo4j import Neo4jAgentStorage
from phi.knowledge.pdf import PDFUrlKnowledgeBase
from phi.vectordb.neo4j import Neo4jVector

# connect to Neo4j-Database
# start one with cookbook/run_neo4j.sh if you haven't already or connect to a free aura instance
db_url = "bolt://neo4j:phi-neo4j@localhost:7687"

agent = Agent(
    storage=Neo4jAgentStorage(table_name="recipe_agent", db_url=db_url),
    knowledge_base=PDFUrlKnowledgeBase(
        urls=["https://phi-public.s3.amazonaws.com/recipes/ThaiRecipes.pdf"],
        vector_db=Neo4jVector(collection="recipe_documents", db_url=db_url),
    ),
    # Show tool calls in the response
    show_tool_calls=True,
    # Enable the agent to search the knowledge base
    search_knowledge=True,
    # Enable the agent to read the chat history
    read_chat_history=True,
)
# Comment out after first run
agent.knowledge_base.load(recreate=False)  # type: ignore

agent.print_response("How do I make pad thai?", markdown=True)
