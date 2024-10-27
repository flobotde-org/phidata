import typer
from rich.prompt import Prompt
from typing import Optional

from phi.agent import Agent
from phi.knowledge.pdf import PDFUrlKnowledgeBase
from phi.vectordb.neo4j import Neo4jVectorDb
from langchain_ollama.embeddings import OllamaEmbeddings

url = "bolt://localhost:7687"
username = "neo4j"
password = "phi-neo4j"
embeddings = OllamaEmbeddings(
    model="llama3.2",
)


knowledge_base = PDFUrlKnowledgeBase(
    urls=["https://phi-public.s3.amazonaws.com/recipes/ThaiRecipes.pdf"],
    vector_db=Neo4jVectorDb(
        url=url,
        username=username,
        password=password,
        embedder=embeddings,
        node_label="thai-curry-recipe",
        index_name="thai-curry-recipe-pdf",
    ),
)

# Comment out after first run
knowledge_base.load(recreate=False)


def pdf_agent(user: str = "user"):
    run_id: Optional[str] = None

    agent = Agent(
        run_id=run_id,
        user_id=user,
        knowledge_base=knowledge_base,
        use_tools=True,
        show_tool_calls=True,
        debug_mode=True,
    )
    if run_id is None:
        run_id = agent.run_id
        print(f"Started Run: {run_id}\n")
    else:
        print(f"Continuing Run: {run_id}\n")

    while True:
        message = Prompt.ask(f"[bold] :sunglasses: {user} [/bold]")
        if message in ("exit", "bye"):
            break
        agent.print_response(message)


if __name__ == "__main__":
    typer.run(pdf_agent)
