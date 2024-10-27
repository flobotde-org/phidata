# Neo4j-Vector Agent

> Fork and clone the repository if needed.

### 1. Create a virtual environment

```shell
python3 -m venv ~/.venvs/aienv
source ~/.venvs/aienv/bin/activate
```

### 2. Install libraries

```shell
pip install -U neo4j langchain-community pypdf "psycopg[binary]" phidata docker
```

### 3. Run Neo4J locally
### Tip: alternative you can also use an AURA instance for free https://neo4j.com/product/auradb/)

> Install [docker desktop](https://docs.docker.com/desktop/install/) first.
> Install [docker-compose](https://docs.docker.com/compose/install/) second.

- Run using a helper script

```shell
./cookbook/run_neo4j.sh
```

- OR run using the docker-compose command

```shell
docker-compose -f cookbook/vectordb/neo4j/docker-compose.yml up -d
```

### 4. Run Neo4j-Vector Agent

```shell
python cookbook/integrations/neo4j/agent.py
```
