from __future__ import annotations

import asyncio
import io
import logging
import os
import uuid
from datetime import datetime
from typing import Optional, List

from app.services.logger import get_logger

logger = get_logger(__name__)

CHUNK_SIZE         = int(os.environ.get("RAG_CHUNK_SIZE",        "5000"))
CHUNK_OVERLAP      = int(os.environ.get("RAG_CHUNK_OVERLAP",      "200"))
INGEST_CONCURRENCY = int(os.environ.get("RAG_INGEST_CONCURRENCY", "20"))


def _chunk_text(text: str, chunk_size: int = CHUNK_SIZE, overlap: int = CHUNK_OVERLAP) -> list[str]:
    text = text.strip()
    if not text:
        return []
    chunks: list[str] = []
    start = 0
    step = max(1, chunk_size - overlap)
    while start < len(text):
        chunk = text[start : start + chunk_size].strip()
        if chunk:
            chunks.append(chunk)
        start += step
    return chunks

def _extract_pdf_text_mistral(raw_bytes: bytes) -> str:
    """Extract text from PDF using Mistral OCR API, falls back to pypdf."""
    api_key = os.environ.get("MISTRAL_API_KEY", "")
    if not api_key:
        logger.warning("[ingest] MISTRAL_API_KEY not set — falling back to pypdf")
        return _extract_pdf_text_pypdf(raw_bytes)

    try:
        from mistralai.client import Mistral

        client = Mistral(api_key=api_key)
        uploaded = client.files.upload(
            file={
                "file_name": f"doc_{uuid.uuid4().hex[:8]}.pdf",
                "content": raw_bytes,
            },
            purpose="ocr",
        )
        file_id = uploaded.id
        ocr_response = client.ocr.process(
            model="mistral-ocr-latest",
            document={"type": "file", "file_id": file_id},
        )

        full_text = ""
        for page in ocr_response.pages:
            full_text += (page.markdown or "") + "\n\n"

        try:
            client.files.delete(file_id=file_id)
        except Exception:
            pass

        return full_text

    except Exception as e:
        logger.error(f"[ingest] Mistral OCR failed: {e}", exc_info=True)
        return _extract_pdf_text_pypdf(raw_bytes)


def _extract_pdf_text_pypdf(raw_bytes: bytes) -> str:
    """Fallback: plain text extraction via pypdf."""
    try:
        from pypdf import PdfReader

        reader = PdfReader(io.BytesIO(raw_bytes))
        return "\n".join(page.extract_text() or "" for page in reader.pages)
    except Exception as e:
        logger.error(f"[ingest] pypdf extraction failed: {e}")
        return raw_bytes.decode("utf-8", errors="ignore")


class GraphitiRAGService:
    """
    Graph-based RAG using Graphiti + FalkorDB.
    """

    def __init__(self) -> None:
        self.graphiti = None
        self._init_lock: Optional[asyncio.Lock] = None

    def _get_lock(self) -> asyncio.Lock:
        if self._init_lock is None:
            self._init_lock = asyncio.Lock()
        assert self._init_lock is not None
        return self._init_lock

    async def _ensure_initialized(self) -> None:
        if self.graphiti is not None:
            return

        async with self._get_lock():
            if self.graphiti is not None:
                return

            from graphiti_core import Graphiti
            from graphiti_core.nodes import EpisodicNode, EntityNode
            from graphiti_core.driver.falkordb_driver import FalkorDriver
            from app.llm.llm_client import get_graphiti_llm_client, get_graphiti_embedder

            host = os.environ.get("FALKORDB_HOST", "falkordb")
            port = int(os.environ.get("FALKORDB_PORT", "6379"))

            logger.info(f"[rag] Connecting to FalkorDB at {host}:{port}")
            driver = FalkorDriver(
                host=host, 
                port=port, 
                database="graphiti"
            )
            self.graphiti = Graphiti(
                graph_driver=driver,
                llm_client=get_graphiti_llm_client(),
                embedder=get_graphiti_embedder(),
            )
            logger.info("[rag] Graphiti initialized ✓")

    async def initialize(self) -> None:
        await self._ensure_initialized()

    async def insert_document(self, doc_id: str, text: str, filename: str = "", group_id: str = "default") -> None:
        await self._ensure_initialized()
        if self.graphiti:
            await self.graphiti.add_episode(
                name=filename or doc_id,
                episode_body=text,
                source_description="Financial Document",
                reference_time=datetime.now(),
                group_id=group_id,
            )

    async def query(self, question: str, mode: str = "graph", group_ids: Optional[List[str]] = None) -> str:
        await self._ensure_initialized()
        if not self.graphiti:
            return "[Graphiti not initialized]"

        effective_group_ids = group_ids if group_ids else None
        if group_ids is not None and len(group_ids) == 0:
            logger.info(f"[rag] No chat-doc mapping found, searching across all documents")

        limit = 5 if mode == "graph" else 10
        try:
            search_results = await self.graphiti.search(
                query=question, 
                num_results=limit,
                group_ids=effective_group_ids
            )
        except Exception as e:
            logger.error(f"Graphiti search failed for question '{question}': {e}", exc_info=True)
            return f"[Search failed due to technical error: {str(e)}]"

        if not search_results:
            logger.warning(f"[rag] No relevant context found in Graphiti for query: {question} (groups: {effective_group_ids})")
            return "[No relevant context found]"

        logger.info(f"[rag] Found {len(search_results)} search results for: {question}")
        facts = [edge.fact for edge in search_results if edge.fact]
        if not facts:
            logger.warning(f"[rag] Search returned {len(search_results)} edges but none had facts")
            return "[No relevant context found]"
        return "\n".join(f"- {fact}" for fact in facts)

    async def finalize(self) -> None:
        pass

    async def delete_document(self, doc_id: str) -> None:
        await self._ensure_initialized()
        if not self.graphiti:
            logger.warning(f"[rag] Graphiti not initialized, cannot delete {doc_id}")
            return

        driver = self.graphiti.driver
        try:
            await driver.client.select_graph(doc_id).delete()
            logger.info(f"[rag] Isolated graph '{doc_id}' deleted from FalkorDB")
        except Exception as e:
            logger.debug(f"[rag] No isolated graph found for '{doc_id}' or already deleted: {e}")

        try:
            await driver.execute_query(
                """
                MATCH (e:Episodic {group_id: $group_id})-[r]-()
                DELETE r
                """,
                group_id=doc_id,
            )
            await driver.execute_query(
                """
                MATCH (e:Episodic {group_id: $group_id})
                DELETE e
                """,
                group_id=doc_id,
            )

            await driver.execute_query(
                """
                MATCH (n:Entity {group_id: $group_id})
                WHERE NOT (n)--(:Episodic)
                DETACH DELETE n
                """,
                group_id=doc_id,
            )

            await driver.execute_query(
                """
                MATCH ()-[r {group_id: $group_id}]-()
                DELETE r
                """,
                group_id=doc_id,
            )

            logger.info(f"[rag] Document {doc_id} fully cleared from graph")

        except Exception as e:
            logger.error(f"[rag] Failed to delete document {doc_id} from graph: {e}", exc_info=True)
            raise


HybridRAGService = GraphitiRAGService

_rag_service_instance: Optional[GraphitiRAGService] = None
_rag_instance_lock: Optional[asyncio.Lock] = None


async def _get_rag_service() -> GraphitiRAGService:
    """Return the module-level singleton, initializing on first call."""
    global _rag_service_instance, _rag_instance_lock

    if _rag_service_instance is not None:
        return _rag_service_instance

    if _rag_instance_lock is None:
        _rag_instance_lock = asyncio.Lock()

    async with _rag_instance_lock:
        if _rag_service_instance is None:
            svc = GraphitiRAGService()
            await svc.initialize()
            _rag_service_instance = svc

    return _rag_service_instance

def _mark_status(doc_id: str, status: str) -> None:
    """Update document status in FalkorDB via Redis-protocol layer."""
    try:
        from app.database.document_store import set_document_status

        status_map = {
            "processing": "processing",
            "done":       "completed",
            "completed":  "completed",
            "failed":     "failed",
            "ready":      "completed",
            "error":      "failed",
        }
        normalised = status_map.get(status, "failed")
        if set_document_status(doc_id, normalised):
            logger.info(f"[ingest] doc {doc_id} status → {normalised}")
    except Exception as e:
        logger.error(f"[ingest] failed to update status for {doc_id}: {e}")


async def _ingest_async(doc_id: str, raw_bytes: bytes, filename: str) -> None:
    """
    Full ingestion pipeline as a coroutine.
    """
    logger.info(f"[ingest] - Starting: {filename} ({doc_id})")
    _mark_status(doc_id, "processing")

    try:
        if filename.lower().endswith(".pdf"):
            logger.info(f"[ingest] Extracting PDF text via Mistral OCR: {filename}")
            loop = asyncio.get_running_loop()
            text = await loop.run_in_executor(None, _extract_pdf_text_mistral, raw_bytes)
        else:
            text = raw_bytes.decode("utf-8", errors="ignore")

        if not text.strip():
            logger.warning(f"[ingest] No text extracted from {filename} — marking failed")
            _mark_status(doc_id, "failed")
            return

        chunks = _chunk_text(text)
        logger.info(f"[ingest] {len(chunks)} chunks to ingest for {filename}")

        rag = await _get_rag_service()
        semaphore = asyncio.Semaphore(INGEST_CONCURRENCY)
        success_count = 0
        fail_count = 0

        async def _ingest_one(i: int, chunk: str) -> None:
            nonlocal success_count, fail_count
            async with semaphore:
                chunk_id    = f"{doc_id}_{i}"
                chunk_label = f"{filename} (part {i + 1}/{len(chunks)})"
                try:
                    logger.debug(f"[ingest] - chunk {i + 1}/{len(chunks)}")
                    await rag.insert_document(
                        doc_id=chunk_id,
                        text=chunk,
                        filename=chunk_label,
                        group_id=doc_id, 
                    )
                    success_count += 1
                except Exception as e:
                    fail_count += 1
                    logger.warning(f"[ingest] - chunk {i + 1} failed (continuing): {e}")

        await asyncio.gather(*[
            _ingest_one(i, chunk)
            for i, chunk in enumerate(chunks)
            if chunk.strip()
        ])

        logger.info(
            f"[ingest] - Done: {filename} — "
            f"{success_count} ok, {fail_count} failed out of {len(chunks)} chunks"
        )
        _mark_status(doc_id, "completed" if fail_count < len(chunks) else "failed")

    except Exception as e:
        logger.error(f"[ingest] - Fatal error for {filename}: {e}", exc_info=True)
        _mark_status(doc_id, "failed")


def ingest_document(doc_id: str, raw_bytes: bytes, filename: str) -> None:
    """
    Fire-and-forget entry point. Kept for backwards compatibility.
    """
    from app.services.worker_threads import submit_async
    logger.info(f"[ingest] Queuing background ingest: {filename} ({doc_id})")
    submit_async(_ingest_async(doc_id, raw_bytes, filename))

async def _delete_document_async(doc_id: str) -> None:
    """Async task to delete document from RAG service."""
    try:
        rag = await _get_rag_service()
        await rag.delete_document(doc_id)
    except Exception as e:
        logger.error(f"[ingest] Failed to delete document {doc_id} from graph: {e}", exc_info=True)

def delete_document_data(doc_id: str) -> None:
    """
    Remove all data for a document from the RAG graph.
    Fire-and-forget background task.
    """
    from app.services.worker_threads import submit_async
    logger.info(f"[rag] Queuing cleanup for doc: {doc_id}")
    submit_async(_delete_document_async(doc_id))
