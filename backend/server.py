from dotenv import load_dotenv
load_dotenv()

import os

import opik
from groq import Groq
from langchain_openai import OpenAIEmbeddings
from mcp.server.fastmcp import FastMCP
from pinecone import Pinecone


def _safe_track(*args, **kwargs):
    try:
        return opik.track(*args, **kwargs)
    except Exception:
        def noop(fn): return fn
        return noop


pc = Pinecone(api_key=os.environ["PINECONE_API_KEY"])
index = pc.Index(os.environ["PINECONE_INDEX_NAME"])
embeddings = OpenAIEmbeddings(model="text-embedding-3-small")
groq_client = Groq(api_key=os.environ["GROQ_API_KEY"])

mcp = FastMCP("pycelonis-mcp")

GROQ_MODEL = "llama-3.3-70b-versatile"
TOP_K = 5


def _retrieve(query: str, top_k: int = TOP_K, filter: dict | None = None) -> list[dict]:
    vector = embeddings.embed_query(query)
    kwargs = {"vector": vector, "top_k": top_k, "include_metadata": True}
    if filter:
        kwargs["filter"] = filter
    result = index.query(**kwargs)
    return result.matches


def _format_context(matches: list[dict]) -> tuple[str, list[str]]:
    chunks, sources = [], []
    for m in matches:
        text = m.metadata.get("text", "")
        source = m.metadata.get("source", "")
        if text:
            chunks.append(text)
        if source and source not in sources:
            sources.append(source)
    return "\n\n---\n\n".join(chunks), sources


def _chat(system: str, user: str) -> str:
    response = groq_client.chat.completions.create(
        model=GROQ_MODEL,
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
    )
    return response.choices[0].message.content


@mcp.tool()
@_safe_track(name="ask_pycelonis")
def ask_pycelonis(question: str) -> str:
    """General Q&A about the PyCelonis 2.14.2 SDK."""
    matches = _retrieve(question)
    context, sources = _format_context(matches)

    system = (
        "You are a PyCelonis expert assistant. Answer questions about the PyCelonis 2.14.2 "
        "Python SDK based only on the provided documentation context. Include code examples "
        "when relevant. If the answer is not in the context, say so clearly. Never make up information."
    )
    user = f"Documentation context:\n\n{context}\n\nQuestion: {question}"
    answer = _chat(system, user)

    sources_block = "\n".join(f"- {s}" for s in sources)
    return f"{answer}\n\nSources:\n{sources_block}"


@mcp.tool()
@_safe_track(name="get_code_example")
def get_code_example(task: str) -> str:
    """Return a ready-to-use PyCelonis 2.14.2 code example for a given task."""
    matches = _retrieve(task)
    context, sources = _format_context(matches)

    system = (
        "You are a PyCelonis code assistant. Given a task description, return a complete, "
        "ready-to-use Python code example using PyCelonis 2.14.2. Return only code with brief "
        "inline comments. No prose explanations. If you cannot find a relevant example in the "
        "context, say so."
    )
    user = f"Documentation context:\n\n{context}\n\nTask: {task}"
    code = _chat(system, user)

    sources_block = "\n".join(f"- {s}" for s in sources)
    return f"{code}\n\n# Sources:\n{sources_block}"


@mcp.tool()
@_safe_track(name="list_available_classes")
def list_available_classes() -> str:
    """Return a structured list of all PyCelonis 2.14.2 classes and modules by category."""
    return """\
PyCelonis 2.14.2 — Available Classes & Modules

Connection:
  - get_celonis
  - Celonis

Data Integration:
  - DataIntegration
  - DataPool
  - DataPoolTable
  - DataModel
  - DataModelTable
  - DataConnection
  - DataPushJob
  - Job
  - Task
  - TableExtraction

Studio:
  - Studio
  - Space
  - Package
  - Analysis
  - KnowledgeModel
  - View
  - Skill
  - ActionFlow

PQL:
  - PQL
  - PQLColumn
  - PQLFilter
  - DataFrame

SaolaPy:
  - SaolaConnector
  - DataModelSaolaConnector
  - KnowledgeModelSaolaConnector

Utils:
  - CelonisCollection
  - errors
"""


@mcp.tool()
@_safe_track(name="get_migration_help")
def get_migration_help(question: str) -> str:
    """Help developers migrate from PyCelonis 1.x to PyCelonis 2.x."""
    matches = _retrieve(question)
    context, sources = _format_context(matches)

    system = (
        "You are a PyCelonis migration expert. Help developers migrate from PyCelonis 1.x to "
        "PyCelonis 2.x. Focus on breaking changes, deprecated methods, and their 2.x equivalents. "
        "Always provide both the old 1.x way and the new 2.x way. Be specific and include code examples."
    )
    user = f"Documentation context:\n\n{context}\n\nQuestion: {question}"
    answer = _chat(system, user)

    sources_block = "\n".join(f"- {s}" for s in sources)
    return f"{answer}\n\nSources:\n{sources_block}"


if __name__ == "__main__":
    mcp.run(transport="stdio")
