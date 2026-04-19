# PyCelonis MCP Server

Give your coding agent deep knowledge of the PyCelonis 2.14.2 SDK.

## What is this?

An MCP server that indexes the full PyCelonis 2.14.2 documentation and exposes it as tools that coding agents (Claude Desktop, Claude Code, Cursor) can call directly. Ask any question about PyCelonis and get a grounded answer with source citations — without leaving your IDE.

## Tools available

- **ask_pycelonis** — General Q&A about the PyCelonis SDK
- **get_code_example** — Returns a ready-to-use code snippet for a specific task
- **list_available_classes** — Lists all available PyCelonis classes organized by category
- **get_migration_help** — Helps migrate from PyCelonis 1.x to 2.x

## Setup — Claude Desktop (2 minutes)

**Step 1:** Add this to your Claude Desktop config file at `~/Library/Application Support/Claude/claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "pycelonis-mcp": {
      "command": "npx",
      "args": ["-y", "mcp-remote@latest", "https://pycelonis-mcp.onrender.com/sse"]
    }
  }
}
```

**Step 2:** Restart Claude Desktop

**Step 3:** Ask anything about PyCelonis

## Setup — Claude Code

Run this command in your project:

```
claude mcp add pycelonis-mcp -- /path/to/.venv/bin/python /path/to/backend/server.py
```

## How it works

Two phases: **Ingestion** (one time) — crawls all 130 PyCelonis 2.14.2 doc pages, chunks, embeds with OpenAI, stores in Pinecone. **Query** (every request) — embeds your question, finds relevant chunks, generates grounded answer with Groq.

## Stack

OpenAI text-embedding-3-small · Pinecone · Groq llama-3.3-70b · LangChain · MCP Python SDK · OPIK · FastAPI · Render

## Author

Built by **Raul Vallejo** — PM building and shipping production AI agents. [linkedin.com/in/raulvallejo](https://linkedin.com/in/raulvallejo)

MIT License
