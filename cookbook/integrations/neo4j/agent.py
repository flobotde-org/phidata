# cookbook/integrations/neo4j/agent.py
from phi.agent import Agent
from phi.knowledge.langchain import LangChainKnowledgeBase
from phi.document.reader.pdf import PDFUrlReader
from langchain.document_loaders import TextLoader
from langchain.text_splitter import CharacterTextSplitter
from langchain_community.vectorstores.neo4j_vector import Neo4jVector
from langchain_community.embeddings import OllamaEmbeddings

# connect to Neo4j-Database
# start one with cookbook/run_neo4j.sh if you haven't already or connect to a free aura instance
url= "bolt://neo4j:phi-neo4j@localhost:7687"
username="neo4j"
password="phi-neo4j"
embeddings = OllamaEmbeddings(model="llama3.2",)

## Prepare the document
# load pdf from url wirh phi
reader = PDFUrlReader()
document_orig = reader.read("https://phi-public.s3.amazonaws.com/recipes/ThaiRecipes.pdf")

# Get an TextLoader Obj from langchain # todo is this the same string?
raw_documents = TextLoader(str(document_orig)).load()
# Split the document into chunks
text_splitter = CharacterTextSplitter(chunk_size=1000, chunk_overlap=0)
documents = text_splitter.split_documents(raw_documents)

#create vectorstore neo4j
neo4jVector = Neo4jVector.from_documents(
    embedding=embeddings,
    documents=documents,
    url=url,
    username=username,
    password=password,
)

# Retrieve more documents with higher diversity
# Useful if your dataset has many similar documents
# Create a retriever from the vector store
#retriever = neo4jVector.as_retriever(
#    search_type="mmr",
#    search_kwargs={'k': 6, 'lambda_mult': 0.25}
#)

# Fetch more documents for the MMR algorithm to consider
# But only return the top 5 docs
retriever = neo4jVector.as_retriever(
    search_type="mmr",
    search_kwargs={'k': 5, 'fetch_k': 50}
)

# Only retrieve documents that have a relevance score
# Above a certain threshold
#retriever = neo4jVector.as_retriever(
#    search_type="similarity_score_threshold",
#    search_kwargs={'score_threshold': 0.8}
#)

# Only get the single most similar document from the dataset
#retriever = neo4jVector.as_retriever(search_kwargs={'k': 1})

# Use a filter to only retrieve documents from a specific paper
#create vectorstore in neo4j
#retriever = neo4jVector.as_retriever(
#    search_kwargs={'filter': {'paper_title':'Thai'}}
#)

# Create a knowledge base from the vector store
knowledge_base = LangChainKnowledgeBase(retriever=retriever)

# Create an agent with the knowledge base
agent = Agent(knowledge_base=knowledge_base, use_tools=True, 
              show_tool_calls=True,
              read_chat_history=False,)
agent.print_response("How to make Thai curry?", markdown=True)
