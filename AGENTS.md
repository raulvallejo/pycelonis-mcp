# PyCelonis MCP Server

A RAG-powered MCP (Model Context Protocol) server that lets coding agents (Claude Code, Cursor, GitHub Copilot) ask questions about the PyCelonis Python SDK and get grounded, cited answers.

## Project Overview

This server indexes the full PyCelonis 2.14.2 documentation using OpenAI embeddings stored in Pinecone. It exposes a single MCP tool — `ask_pycelonis` — that coding agents can call to retrieve grounded answers with source citations.

This is agent-to-agent infrastructure. There is no web UI, no human-facing interface.

## Architecture

Two-phase pipeline:

**Phase 1 — Ingestion (runs once)**
```
Crawl PyCelonis docs → chunk text → embed with OpenAI → store in Pinecone
```

**Phase 2 — MCP Server (runs persistently)**
```
Agent calls ask_pycelonis tool
  → embed query with OpenAI
  → similarity search Pinecone
  → retrieve top 5 chunks
  → Groq generates grounded answer
  → return answer + sources to agent
```

## Key Files

| File | Purpose |
|---|---|
| `backend/ingest.py` | Crawl, chunk, embed, and store docs in Pinecone. Run once before first server start. |
| `backend/server.py` | MCP server exposing the `ask_pycelonis` tool. |
| `backend/mcp_config.json` | MCP configuration for Claude Code / Cursor. |

## Environment Variables

| Variable | Value |
|---|---|
| `OPENAI_API_KEY` | Your OpenAI key |
| `GROQ_API_KEY` | Your Groq key |
| `OPIK_API_KEY` | Your OPIK key |
| `OPIK_PROJECT_NAME` | `pycelonis-mcp` |
| `OPIK_WORKSPACE` | `ra-l-vallejo` |
| `PINECONE_API_KEY` | Your Pinecone key |
| `PINECONE_INDEX_NAME` | `pycelonis-mcp` |

Never commit API keys. Use a `.env` file (gitignored).

## OPIK Observability

OPIK (by Comet) is configured via environment variables only.

- Never call `opik.configure()` anywhere in the codebase.
- Use the `_safe_track` decorator pattern for instrumentation.

## Critical Rules

1. Run `backend/ingest.py` before starting the server for the first time.
2. The MCP server runs locally — it is not deployed to Render, Vercel, or any cloud host.
3. Never commit API keys or `.env` files.

## Known Gotchas

None yet — will be updated as the project evolves.

## Stack

| Layer | Technology |
|---|---|
| Embeddings | OpenAI `text-embedding-3-small` |
| Vector store | Pinecone (cloud) |
| Generation | Groq `llama-3.3-70b-versatile` |
| RAG chain | LangChain |
| MCP server | MCP Python SDK |
| Observability | OPIK by Comet |
