import os
import asyncio
import logging
from lightrag import LightRAG, QueryParam
from lightrag.llm.openai import gpt_4o_mini_complete, openai_embed
from lightrag.utils import setup_logger

setup_logger("lightrag", level="INFO")
logger = logging.getLogger(__name__)


class LiteRAGService:
    def __init__(self, working_dir: str = "./rag_storage"):
        self.working_dir = working_dir

        if not os.environ.get("OPENAI_API_KEY"):
            raise ValueError("OPENAI_API_KEY environment variable is not set.")

        if not os.path.exists(self.working_dir):
            os.makedirs(self.working_dir)

        self.rag = LightRAG(
            working_dir=self.working_dir,
            llm_model_func=gpt_4o_mini_complete,
            embedding_func=openai_embed,
        )

    async def initialize(self):
        await self.rag.initialize_storages()

    async def insert_text(self, text: str):
        await self.rag.ainsert(text)

    async def query(self, question: str, mode: str = "hybrid") -> str:
        return await self.rag.aquery(
            question,
            param=QueryParam(mode=mode),
        )

    async def finalize(self):
        await self.rag.finalize_storages()


def ingest_document(doc_id: str, raw_bytes: bytes, filename: str) -> None:
    """
    Decode raw file bytes to text and insert into LightRAG.
    Called by app/services/worker_threads.py in a background thread.
    Updates document status in the DB: pending -> processing -> done/failed.

    Uses a fresh event loop per call to avoid conflicts with Flask's event loop
    or any existing loop running in the worker thread.
    """
    async def _run():
        _mark_status(doc_id, "processing")

        rag_service = LiteRAGService()
        await rag_service.initialize()

        try:
            if filename.lower().endswith(".pdf"):
                text = _extract_pdf_text(raw_bytes)
            else:
                text = raw_bytes.decode("utf-8", errors="ignore")
        except Exception as e:
            logger.error(f"[ingest] decode failed for {filename}: {e}")
            await rag_service.finalize()
            _mark_status(doc_id, "failed")
            return

        try:
            await rag_service.insert_text(text)
            logger.info(f"[ingest] {filename} ({doc_id}) indexed successfully")
            _mark_status(doc_id, "done")
        except Exception as e:
            logger.error(f"[ingest] insert failed for {filename}: {e}")
            _mark_status(doc_id, "failed")
        finally:
            await rag_service.finalize()

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(_run())
    except Exception as e:
        logger.error(f"[ingest] Unhandled error for {doc_id}: {e}")
        _mark_status(doc_id, "failed")
    finally:
        loop.close()


def _extract_pdf_text(raw_bytes: bytes) -> str:
    """Extract plain text from PDF bytes using pypdf."""
    try:
        import io
        from pypdf import PdfReader

        reader = PdfReader(io.BytesIO(raw_bytes))
        return "\n".join(page.extract_text() or "" for page in reader.pages)
    except ImportError:
        logger.warning("pypdf not installed — treating PDF as raw bytes")
        return raw_bytes.decode("utf-8", errors="ignore")


def _mark_status(doc_id: str, status: str) -> None:
    """Update document status in the database."""
    try:
        from app.database.connection import get_session
        from app.database.models import Document, StatusEnum

        status_map = {
            "processing": StatusEnum.processing,
            "done":       StatusEnum.done,
            "failed":     StatusEnum.failed,
            "ready":      StatusEnum.done,
            "error":      StatusEnum.failed,
        }
        status_val = status_map.get(status, StatusEnum.failed)

        with get_session() as session:
            doc = session.query(Document).filter_by(id=doc_id).first()
            if doc:
                doc.status = status_val
                session.commit()
                logger.info(f"[ingest] doc {doc_id} status -> {status}")
            else:
                logger.warning(
                    f"[ingest] doc {doc_id} not found in DB when marking {status}"
                )
    except Exception as e:
        logger.error(f"[ingest] failed to update status for {doc_id}: {e}")