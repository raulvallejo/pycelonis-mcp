from dotenv import load_dotenv
load_dotenv()

import os
import time

import requests
from bs4 import BeautifulSoup
from langchain_text_splitters import RecursiveCharacterTextSplitter
from openai import OpenAI
from pinecone import Pinecone

URLS = [
    "https://celonis.github.io/pycelonis/2.14.2/tutorials/executed/01_quickstart/01_installation/",
    "https://celonis.github.io/pycelonis/2.14.2/tutorials/executed/01_quickstart/02_celonis_basics/",
    "https://celonis.github.io/pycelonis/2.14.2/tutorials/executed/02_data_integration/01_intro_data_integration/",
    "https://celonis.github.io/pycelonis/2.14.2/tutorials/executed/02_data_integration/02_data_push/",
    "https://celonis.github.io/pycelonis/2.14.2/tutorials/executed/02_data_integration/03_data_pull/",
    "https://celonis.github.io/pycelonis/2.14.2/tutorials/executed/02_data_integration/04_data_model_advand/",
    "https://celonis.github.io/pycelonis/2.14.2/tutorials/executed/02_data_integration/05_data_push_pull_advanced/",
    "https://celonis.github.io/pycelonis/2.14.2/tutorials/executed/02_data_integration/06_data_job/",
    "https://celonis.github.io/pycelonis/2.14.2/tutorials/executed/03_studio/01_intro_studio/",
    "https://celonis.github.io/pycelonis/2.14.2/tutorials/executed/03_studio/02_Pulling_Data_From_Analysis/",
    "https://celonis.github.io/pycelonis/2.14.2/tutorials/executed/03_studio/03_Pulling_Data_From_Knowledge_Model/",
    "https://celonis.github.io/pycelonis/2.14.2/tutorials/executed/04_migration/01_migration_guide/",
    "https://celonis.github.io/pycelonis/2.14.2/tutorials/executed/05_saolapy/01_saolapy_quickstart/",
    "https://celonis.github.io/pycelonis/2.14.2/tutorials/executed/05_saolapy/02_series/",
    "https://celonis.github.io/pycelonis/2.14.2/tutorials/executed/05_saolapy/03_dataframe/",
    "https://celonis.github.io/pycelonis/2.14.2/tutorials/executed/05_saolapy/04_arithmetic_aggregation/",
    "https://celonis.github.io/pycelonis/2.14.2/tutorials/executed/05_saolapy/05_filtering/",
    "https://celonis.github.io/pycelonis/2.14.2/tutorials/executed/05_saolapy/06_string/",
    "https://celonis.github.io/pycelonis/2.14.2/tutorials/executed/05_saolapy/07_datetime/",
    "https://celonis.github.io/pycelonis/2.14.2/tutorials/executed/06_pycelonis_llm/01_pycelonis_llm_tutorial/",
    "https://celonis.github.io/pycelonis/2.14.2/license/",
    "https://celonis.github.io/pycelonis/2.14.2/changelog/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/saolapy/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/saolapy/errors/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/saolapy/operator/aggregation/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/saolapy/operator/arithmetic/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/saolapy/operator/base/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/saolapy/operator/boolean/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/saolapy/operator/data_type/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/saolapy/operator/datetime/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/saolapy/operator/pull_up_aggregation/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/saolapy/operator/range/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/saolapy/operator/scalar/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/saolapy/operator/string/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/saolapy/pandas/data_frame/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/saolapy/pandas/datetime/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/saolapy/pandas/filters/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/saolapy/pandas/group_by_aggregation/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/saolapy/pandas/pull_up_aggregation/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/saolapy/pandas/series/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/saolapy/pandas/string/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/saolapy/pandas/util/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/saolapy/pql/base/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/saolapy/pql/formatter/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/saolapy/saola_connector/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/saolapy/types/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/pycelonis/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/pycelonis/auth_token/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/pycelonis/celonis/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/pycelonis/config/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/pycelonis/errors/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/pycelonis/ems/apps/content_node/action_flow/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/pycelonis/ems/apps/content_node/analysis/component/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/pycelonis/ems/apps/content_node/analysis/content/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/pycelonis/ems/apps/content_node/folder/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/pycelonis/ems/apps/content_node/package/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/pycelonis/ems/apps/content_node/published_content_node_factory/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/pycelonis/ems/apps/content_node/simulation/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/pycelonis/ems/apps/content_node/skill/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/pycelonis/ems/apps/content_node/view/component/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/pycelonis/ems/apps/content_node/view/component_factory/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/pycelonis/ems/apps/content_node/view/content/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/pycelonis/ems/apps/content_node/view/settings/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/pycelonis/ems/apps/space/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/pycelonis/ems/data_integration/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/pycelonis/ems/data_integration/augmentation_table/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/pycelonis/ems/data_integration/data_connection/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/pycelonis/ems/data_integration/data_export/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/pycelonis/ems/data_integration/data_model/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/pycelonis/ems/data_integration/data_model_execution/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/pycelonis/ems/data_integration/data_model_table/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/pycelonis/ems/data_integration/data_model_table_column/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/pycelonis/ems/data_integration/data_pool/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/pycelonis/ems/data_integration/data_pool_table/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/pycelonis/ems/data_integration/data_push_job/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/pycelonis/ems/data_integration/foreign_key/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/pycelonis/ems/data_integration/job/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/pycelonis/ems/data_integration/pool_variable/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/pycelonis/ems/data_integration/process_configuration/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/pycelonis/ems/data_integration/replication/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/pycelonis/ems/data_integration/table_extraction/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/pycelonis/ems/data_integration/task/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/pycelonis/ems/data_integration/task_variable/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/pycelonis/ems/studio/content_node/action_flow/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/pycelonis/ems/studio/content_node/analysis/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/pycelonis/ems/studio/content_node/content_node_factory/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/pycelonis/ems/studio/content_node/folder/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/pycelonis/ems/studio/content_node/knowledge_model/component/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/pycelonis/ems/studio/content_node/knowledge_model/content/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/pycelonis/ems/studio/content_node/knowledge_model/data_export/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/pycelonis/ems/studio/content_node/knowledge_model/filter/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/pycelonis/ems/studio/content_node/knowledge_model/kpi/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/pycelonis/ems/studio/content_node/knowledge_model/variable/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/pycelonis/ems/studio/content_node/package/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/pycelonis/ems/studio/content_node/simulation/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/pycelonis/ems/studio/content_node/skill/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/pycelonis/ems/studio/content_node/variable/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/pycelonis/ems/studio/content_node/view/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/pycelonis/ems/studio/space/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/pycelonis/ems/team/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/pycelonis/pql/data_frame/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/pycelonis/pql/pql/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/pycelonis/pql/pql_debugger/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/pycelonis/pql/pql_parser/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/pycelonis/pql/saola_connector/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/pycelonis/pql/series/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/pycelonis/pql/util/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/pycelonis/service/augmentation/service/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/pycelonis/service/blueprint/service/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/pycelonis/service/data_ingestion/service/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/pycelonis/service/integration/service/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/pycelonis/service/package_manager/service/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/pycelonis/service/pql_language/service/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/pycelonis/service/process_analytics/service/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/pycelonis/service/replication/service/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/pycelonis/service/semantic_layer/service/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/pycelonis/service/team/service/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/pycelonis/utils/deprecation/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/pycelonis/utils/json/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/pycelonis/utils/parquet/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/pycelonis/utils/polling/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/pycelonis/utils/yaml/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/pycelonis_core/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/pycelonis_core/base/base_model/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/pycelonis_core/base/collection/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/pycelonis_core/client/client/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/pycelonis_core/client/request_body_extractor/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/pycelonis_core/client/response_processor/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/pycelonis_core/utils/errors/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/pycelonis_core/utils/httpx/",
    "https://celonis.github.io/pycelonis/2.14.2/reference/pycelonis_core/utils/ml_workbench/",
]

CHUNK_SIZE = 1000
CHUNK_OVERLAP = 200
BATCH_SIZE = 100
FETCH_DELAY = 0.5
EMBEDDING_MODEL = "text-embedding-3-small"
COST_PER_MILLION_TOKENS = 0.02


def extract_text(soup: BeautifulSoup) -> str:
    for tag in soup.select("nav, footer, script, style, .sidebar, .toc, header, [role='navigation']"):
        tag.decompose()
    main = soup.find("main") or soup.find("article") or soup.find("div", class_="md-content") or soup.body
    return main.get_text(separator="\n", strip=True) if main else ""


def fetch_pages() -> list[dict]:
    session = requests.Session()
    session.headers["User-Agent"] = "pycelonis-mcp-ingest/1.0"
    pages: list[dict] = []

    for n, url in enumerate(URLS, start=1):
        print(f"Crawling [{n}]: {url}")
        try:
            response = session.get(url, timeout=15)
            response.raise_for_status()
        except Exception as exc:
            print(f"  [skip] {url} — {exc}")
            time.sleep(FETCH_DELAY)
            continue

        soup = BeautifulSoup(response.text, "html.parser")
        text = extract_text(soup)
        if text:
            pages.append({"url": url, "text": text})

        time.sleep(FETCH_DELAY)

    with open("crawled_urls.txt", "w") as f:
        f.write("\n".join(p["url"] for p in pages))

    return pages


def chunk_pages(pages: list[dict]) -> list:
    splitter = RecursiveCharacterTextSplitter(chunk_size=CHUNK_SIZE, chunk_overlap=CHUNK_OVERLAP)
    docs = []
    for page in pages:
        chunks = splitter.create_documents([page["text"]], metadatas=[{"source": page["url"]}])
        docs.extend(chunks)
    return docs


def embed_and_upsert(docs: list) -> int:
    client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])
    pc = Pinecone(api_key=os.environ["PINECONE_API_KEY"])
    index = pc.Index(os.environ["PINECONE_INDEX_NAME"])

    total_tokens = 0
    vectors = []

    for i, doc in enumerate(docs):
        response = client.embeddings.create(model=EMBEDDING_MODEL, input=doc.page_content)
        embedding = response.data[0].embedding
        total_tokens += response.usage.total_tokens
        vectors.append({
            "id": f"chunk-{i}",
            "values": embedding,
            "metadata": {
                "text": doc.page_content,
                "source": doc.metadata.get("source", ""),
            },
        })

        if len(vectors) == BATCH_SIZE:
            index.upsert(vectors=vectors)
            batch_num = (i + 1) // BATCH_SIZE
            if batch_num % 100 == 0:
                print(f"  Upserted {i + 1} chunks ({batch_num} batches)...")
            vectors = []

    if vectors:
        index.upsert(vectors=vectors)

    return total_tokens


def main():
    print(f"Fetching {len(URLS)} URLs...\n")
    pages = fetch_pages()
    print(f"\nFetched {len(pages)} pages. Chunking...")

    docs = chunk_pages(pages)
    print(f"Created {len(docs)} chunks. Embedding and upserting to Pinecone...\n")

    total_tokens = embed_and_upsert(docs)

    estimated_cost = (total_tokens / 1_000_000) * COST_PER_MILLION_TOKENS
    print(f"\nDone.")
    print(f"  Total URLs fetched : {len(pages)}")
    print(f"  Total chunks       : {len(docs)}")
    print(f"  Total tokens used  : {total_tokens:,}")
    print(f"  Estimated cost     : ${estimated_cost:.4f}")


if __name__ == "__main__":
    main()
