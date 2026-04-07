from __future__ import annotations

import asyncio
import inspect
import io
import os
import random
import re
import time
import uuid
from datetime import datetime, timezone, timedelta
from typing import Any, Optional, List

from app.services.logger import get_logger

logger = get_logger(__name__)


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        logger.warning(f"[rag] Invalid integer for {name}: {raw!r}; using {default}")
        return default


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        return float(raw)
    except ValueError:
        logger.warning(f"[rag] Invalid float for {name}: {raw!r}; using {default}")
        return default


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}



DEFAULT_INGEST_CONCURRENCY = 4                      
DEFAULT_GRAPHITI_MAX_COROUTINES = max(2, min(12, DEFAULT_INGEST_CONCURRENCY * 3))

CHUNK_SIZE = max(500, _env_int("RAG_CHUNK_SIZE", 5000))                        
CHUNK_OVERLAP = max(0, min(CHUNK_SIZE // 2, _env_int("RAG_CHUNK_OVERLAP", 200)))  
INGEST_CONCURRENCY = max(1, _env_int("RAG_INGEST_CONCURRENCY", DEFAULT_INGEST_CONCURRENCY))
INGEST_RETRY_ATTEMPTS = max(0, _env_int("RAG_INGEST_RETRY_ATTEMPTS", 4))
INGEST_RETRY_BASE_DELAY_SECONDS = max(
    0.25,
    _env_float("RAG_INGEST_RETRY_BASE_DELAY_SECONDS", 2.0),
)
INGEST_RETRY_MAX_DELAY_SECONDS = max(
    INGEST_RETRY_BASE_DELAY_SECONDS,
    _env_float("RAG_INGEST_RETRY_MAX_DELAY_SECONDS", 30.0),
)
MIN_CHUNK_MERGE_SIZE = max(200, _env_int("RAG_MIN_CHUNK_MERGE_SIZE", min(600, CHUNK_SIZE // 5)))
MAX_MERGED_CHUNK_SIZE = max(CHUNK_SIZE, _env_int("RAG_MAX_MERGED_CHUNK_SIZE", CHUNK_SIZE + CHUNK_OVERLAP))
INGEST_ATTEMPT_SPACING_SECONDS = max(
    0.0,
    _env_float("RAG_INGEST_ATTEMPT_SPACING_SECONDS", 0.1),   
)
INGEST_SERIALIZE_ON_RATE_LIMIT = _env_bool("RAG_INGEST_SERIALIZE_ON_RATE_LIMIT", True)
GRAPHITI_MAX_COROUTINES = max(
    1,
    _env_int(
        "GRAPHITI_MAX_COROUTINES",
        _env_int("SEMAPHORE_LIMIT", DEFAULT_GRAPHITI_MAX_COROUTINES),
    ),
)
GRAPHITI_BUILD_INDICES = _env_bool("GRAPHITI_BUILD_INDICES", True)
GRAPHITI_ENABLE_SAGA = _env_bool("GRAPHITI_ENABLE_SAGA", False)
GRAPHITI_USE_ADVANCED_SEARCH = _env_bool("GRAPHITI_USE_ADVANCED_SEARCH", True)
GRAPHITI_GRAPH_SEARCH_LIMIT = max(1, _env_int("GRAPHITI_GRAPH_SEARCH_LIMIT", 8))
GRAPHITI_HYBRID_SEARCH_LIMIT = max(1, _env_int("GRAPHITI_HYBRID_SEARCH_LIMIT", 12))
GRAPHITI_FACT_LIMIT = max(1, _env_int("GRAPHITI_CONTEXT_FACT_LIMIT", 25))
GRAPHITI_NODE_LIMIT = max(0, _env_int("GRAPHITI_CONTEXT_NODE_LIMIT", 15))
GRAPHITI_EPISODE_LIMIT = max(0, _env_int("GRAPHITI_CONTEXT_EPISODE_LIMIT", 4))
GRAPHITI_COMMUNITY_LIMIT = max(0, _env_int("GRAPHITI_CONTEXT_COMMUNITY_LIMIT", 4))
GRAPHITI_PREVIOUS_EPISODE_WINDOW = max(0, _env_int("GRAPHITI_PREVIOUS_EPISODE_WINDOW", 0))   
GRAPHITI_BULK_INGEST_BATCH_SIZE = max(1, _env_int("GRAPHITI_BULK_INGEST_BATCH_SIZE", 5))     
GRAPHITI_BULK_MAX_BATCH_CHARS = max(
    CHUNK_SIZE,
    _env_int("GRAPHITI_BULK_MAX_BATCH_CHARS", CHUNK_SIZE * 6),   
)
PDF_OCR_MODE = os.environ.get("PDF_OCR_MODE", "auto").strip().lower()
PDF_DIRECT_TEXT_MIN_CHARS = max(100, _env_int("PDF_DIRECT_TEXT_MIN_CHARS", 600))

DEFAULT_GRAPHITI_EXTRACTION_INSTRUCTIONS = (
    "Extract densely and preserve retrieval value. Create entities for materially relevant "
    "people, organizations, products, systems, processes, document sections, policies, risks, "
    "controls, metrics, dates, amounts, and domain-specific terms. Keep exact numbers, dates, "
    "units, and qualifiers in facts. Prefer distinct nodes for distinct concepts instead of "
    "collapsing them into generic summaries, and only merge entities when they clearly refer "
    "to the same real-world thing."
)
GRAPHITI_EXTRACTION_INSTRUCTIONS = os.environ.get(
    "GRAPHITI_EXTRACTION_INSTRUCTIONS",
    DEFAULT_GRAPHITI_EXTRACTION_INSTRUCTIONS,
).strip()

_SENTENCE_BOUNDARY_RE = re.compile(r"(?<=[.!?])\s+(?=[A-Z0-9])")
_INGEST_RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}
_INGEST_RETRYABLE_ERROR_MARKERS = (
    "429",
    "rate limit",
    "rate_limit",
    "too many requests",
    "temporarily unavailable",
    "service unavailable",
    "internal server error",
    "server error",
    "timeout",
    "timed out",
    "connection error",
    "api connection",
    "try again later",
)


def _normalize_document_text(text: str) -> str:
    if not text:
        return ""

    lines: list[str] = []
    blank_streak = 0
    cleaned = text.replace("\x00", "").replace("\u000c", "\n")
    for raw_line in cleaned.splitlines():
        line = re.sub(r"[ \t]+", " ", raw_line).strip()
        if line:
            blank_streak = 0
            lines.append(line)
        elif blank_streak == 0:
            lines.append("")
            blank_streak = 1

    return "\n".join(lines).strip()


def _split_large_unit(unit: str, chunk_size: int) -> list[str]:
    unit = unit.strip()
    if not unit:
        return []
    if len(unit) <= chunk_size:
        return [unit]

    parts = [part.strip() for part in _SENTENCE_BOUNDARY_RE.split(unit) if part.strip()]
    if len(parts) <= 1:
        parts = [part.strip() for part in unit.splitlines() if part.strip()]
    if len(parts) <= 1:
        return [
            unit[i : i + chunk_size].strip()
            for i in range(0, len(unit), chunk_size)
            if unit[i : i + chunk_size].strip()
        ]

    segments: list[str] = []
    current: list[str] = []
    current_len = 0

    for part in parts:
        if len(part) > chunk_size:
            if current:
                segments.append(" ".join(current).strip())
                current = []
                current_len = 0
            segments.extend(_split_large_unit(part, chunk_size))
            continue

        separator_len = 1 if current else 0
        if current and current_len + separator_len + len(part) > chunk_size:
            segments.append(" ".join(current).strip())
            current = [part]
            current_len = len(part)
            continue

        current.append(part)
        current_len += separator_len + len(part)

    if current:
        segments.append(" ".join(current).strip())

    return segments


def _take_overlap_units(units: list[str], overlap: int) -> list[str]:
    if overlap <= 0 or not units:
        return []

    selected: list[str] = []
    total = 0
    for unit in reversed(units):
        separator_len = 2 if selected else 0
        projected = total + separator_len + len(unit)
        if selected and projected > overlap:
            break
        selected.append(unit)
        total = projected
        if total >= overlap:
            break

    return list(reversed(selected))


def _chunk_text(text: str, chunk_size: int = CHUNK_SIZE, overlap: int = CHUNK_OVERLAP) -> list[str]:
    text = _normalize_document_text(text)
    if not text:
        return []

    units: list[str] = []
    for block in re.split(r"\n{2,}", text):
        block = block.strip()
        if not block:
            continue
        units.extend(_split_large_unit(block, chunk_size))

    if not units:
        return []

    chunks: list[str] = []
    current: list[str] = []
    current_len = 0

    for unit in units:
        separator_len = 2 if current else 0
        if current and current_len + separator_len + len(unit) > chunk_size:
            chunks.append("\n\n".join(current).strip())
            current = _take_overlap_units(current, overlap)
            current_len = sum(len(part) for part in current) + (2 * max(0, len(current) - 1))

        separator_len = 2 if current else 0
        if current and current_len + separator_len + len(unit) > chunk_size:
            chunks.append(unit.strip())
            current = []
            current_len = 0
            continue

        current.append(unit)
        current_len += separator_len + len(unit)

    if current:
        chunks.append("\n\n".join(current).strip())

    return [chunk for chunk in chunks if chunk]


def _compact_chunks(
    chunks: list[str],
    min_chunk_size: int = MIN_CHUNK_MERGE_SIZE,
    max_merged_size: int = MAX_MERGED_CHUNK_SIZE,
) -> list[str]:
    if not chunks:
        return []

    compacted: list[str] = []
    for chunk in chunks:
        chunk = chunk.strip()
        if not chunk:
            continue

        if not compacted:
            compacted.append(chunk)
            continue

        if len(chunk) < min_chunk_size:
            merged = f"{compacted[-1]}\n\n{chunk}".strip()
            if len(merged) <= max_merged_size:
                compacted[-1] = merged
                continue

        compacted.append(chunk)

    if len(compacted) >= 2 and len(compacted[-1]) < min_chunk_size:
        merged_tail = f"{compacted[-2]}\n\n{compacted[-1]}".strip()
        if len(merged_tail) <= max_merged_size:
            compacted[-2] = merged_tail
            compacted.pop()

    return compacted


def _group_chunks_for_bulk_ingest(
    chunks_with_indices: list[tuple[int, str]],
    max_batch_size: int = GRAPHITI_BULK_INGEST_BATCH_SIZE,
    max_batch_chars: int = GRAPHITI_BULK_MAX_BATCH_CHARS,
) -> list[list[tuple[int, str]]]:
    if not chunks_with_indices:
        return []

    batches: list[list[tuple[int, str]]] = []
    current_batch: list[tuple[int, str]] = []
    current_chars = 0

    for chunk_item in chunks_with_indices:
        _, chunk = chunk_item
        chunk_chars = len(chunk)
        would_exceed_size = len(current_batch) >= max_batch_size
        would_exceed_chars = current_batch and current_chars + chunk_chars > max_batch_chars

        if would_exceed_size or would_exceed_chars:
            batches.append(current_batch)
            current_batch = []
            current_chars = 0

        current_batch.append(chunk_item)
        current_chars += chunk_chars

    if current_batch:
        batches.append(current_batch)

    return batches


def _looks_like_useful_pdf_text(text: str) -> bool:
    normalized = _normalize_document_text(text)
    if len(normalized) < PDF_DIRECT_TEXT_MIN_CHARS:
        return False

    alnum_ratio = sum(ch.isalnum() for ch in normalized) / max(len(normalized), 1)
    alpha_tokens = sum(1 for token in normalized.split() if any(ch.isalpha() for ch in token))
    return alnum_ratio >= 0.35 and alpha_tokens >= 50


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


def _extract_pdf_text(raw_bytes: bytes) -> str:
    if PDF_OCR_MODE == "never":
        return _normalize_document_text(_extract_pdf_text_pypdf(raw_bytes))
    if PDF_OCR_MODE == "always":
        return _normalize_document_text(_extract_pdf_text_mistral(raw_bytes))

    direct_text = _extract_pdf_text_pypdf(raw_bytes)
    if _looks_like_useful_pdf_text(direct_text):
        logger.info("[ingest] Using fast pypdf extraction for text-based PDF")
        return _normalize_document_text(direct_text)

    if os.environ.get("MISTRAL_API_KEY", "").strip():
        logger.info("[ingest] Falling back to Mistral OCR for scanned or low-text PDF")
        return _normalize_document_text(_extract_pdf_text_mistral(raw_bytes))

    logger.info("[ingest] OCR unavailable; using best-effort pypdf extraction")
    return _normalize_document_text(direct_text)


def _filter_supported_kwargs(callable_obj: Any, kwargs: dict[str, Any]) -> dict[str, Any]:
    try:
        signature = inspect.signature(callable_obj)
    except (TypeError, ValueError):
        return kwargs

    if any(param.kind == inspect.Parameter.VAR_KEYWORD for param in signature.parameters.values()):
        return kwargs

    return {key: value for key, value in kwargs.items() if key in signature.parameters}


def _copy_with_updates(model: Any, updates: dict[str, Any]) -> Any:
    if model is None:
        return None
    if hasattr(model, "model_copy"):
        return model.model_copy(update=updates)
    if hasattr(model, "copy"):
        return model.copy(update=updates)

    for key, value in updates.items():
        try:
            setattr(model, key, value)
        except Exception:
            pass
    return model


def _extract_episode_uuid(result: Any) -> Optional[str]:
    if result is None:
        return None
    if isinstance(result, dict):
        episode = result.get("episode")
        if isinstance(episode, dict):
            return episode.get("uuid")
        return getattr(episode, "uuid", None) or result.get("uuid")

    episode = getattr(result, "episode", None)
    if episode is not None:
        return getattr(episode, "uuid", None)
    return getattr(result, "uuid", None)


def _truncate(text: str, limit: int = 280) -> str:
    cleaned = " ".join(str(text).split()).strip()
    if len(cleaned) <= limit:
        return cleaned
    return cleaned[: limit - 3].rstrip() + "..."


def _dedupe_strings(values: list[str], limit: int) -> list[str]:
    if limit <= 0:
        return []

    seen: set[str] = set()
    deduped: list[str] = []
    for value in values:
        cleaned = " ".join(value.split()).strip()
        if not cleaned:
            continue
        key = cleaned.lower()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(cleaned)
        if len(deduped) >= limit:
            break
    return deduped


def _flatten_exception_messages(exc: BaseException) -> str:
    parts: list[str] = []
    seen: set[int] = set()
    current: BaseException | None = exc

    while current is not None and id(current) not in seen:
        seen.add(id(current))
        parts.append(str(current))

        status_code = getattr(current, "status_code", None)
        if status_code is not None:
            parts.append(str(status_code))

        body = getattr(current, "body", None)
        if body is not None:
            parts.append(str(body))

        response = getattr(current, "response", None)
        if response is not None:
            response_status = getattr(response, "status_code", None)
            if response_status is not None:
                parts.append(str(response_status))
            response_text = getattr(response, "text", None)
            if response_text:
                parts.append(str(response_text))

        current = current.__cause__ or current.__context__

    return " ".join(part for part in parts if part).lower()


def _is_retryable_ingest_error(exc: BaseException) -> bool:
    if isinstance(exc, (asyncio.TimeoutError, TimeoutError)):
        return True

    status_code = getattr(exc, "status_code", None)
    if status_code in _INGEST_RETRYABLE_STATUS_CODES:
        return True

    details = _flatten_exception_messages(exc)
    return any(marker in details for marker in _INGEST_RETRYABLE_ERROR_MARKERS)


def _is_rate_limit_ingest_error(exc: BaseException) -> bool:
    status_code = getattr(exc, "status_code", None)
    if status_code == 429:
        return True

    details = _flatten_exception_messages(exc)
    return any(marker in details for marker in ("429", "rate limit", "rate_limit", "too many requests"))


def _parse_retry_after_seconds(value: Any) -> Optional[float]:
    if value is None:
        return None

    if isinstance(value, (int, float)):
        parsed = float(value)
        return parsed if parsed >= 0 else None

    raw = str(value).strip().lower()
    if not raw:
        return None

    try:
        parsed = float(raw)
        return parsed if parsed >= 0 else None
    except ValueError:
        pass

    total = 0.0
    matches = re.findall(r"(\d+(?:\.\d+)?)(ms|s|m|h)", raw)
    if matches:
        for number, unit in matches:
            amount = float(number)
            if unit == "ms":
                total += amount / 1000.0
            elif unit == "s":
                total += amount
            elif unit == "m":
                total += amount * 60.0
            elif unit == "h":
                total += amount * 3600.0
        return total

    return None


def _extract_retry_after_seconds(exc: BaseException) -> Optional[float]:
    seen: set[int] = set()
    current: BaseException | None = exc

    while current is not None and id(current) not in seen:
        seen.add(id(current))

        for attr_name in ("retry_after", "retry_after_seconds"):
            parsed = _parse_retry_after_seconds(getattr(current, attr_name, None))
            if parsed is not None:
                return parsed

        response = getattr(current, "response", None)
        headers = getattr(response, "headers", None)
        if headers:
            for header_name in (
                "retry-after",
                "Retry-After",
                "x-ratelimit-reset-requests",
                "x-ratelimit-reset-tokens",
            ):
                parsed = _parse_retry_after_seconds(headers.get(header_name))
                if parsed is not None:
                    return parsed

        current = current.__cause__ or current.__context__

    return None


def _ingest_retry_delay_seconds(retry_number: int) -> float:
    delay = min(
        INGEST_RETRY_BASE_DELAY_SECONDS * (2 ** max(retry_number - 1, 0)),
        INGEST_RETRY_MAX_DELAY_SECONDS,
    )
    jitter = min(1.0, INGEST_RETRY_BASE_DELAY_SECONDS) * random.uniform(0.0, 0.5)
    return min(delay + jitter, INGEST_RETRY_MAX_DELAY_SECONDS)


class _IngestBackpressure:
    def __init__(self, concurrency: int) -> None:
        self._concurrency = max(1, concurrency)
        self._control_lock: Optional[asyncio.Lock] = None
        self._serial_lock: Optional[asyncio.Lock] = None
        self._cooldown_until = 0.0
        self._cooldown_reason = ""
        self._next_attempt_at = 0.0
        self._serialize_attempts = self._concurrency == 1
        self._serialize_on_rate_limit = INGEST_SERIALIZE_ON_RATE_LIMIT and self._concurrency > 1
        self._last_wait_log_at = 0.0

    def _get_control_lock(self) -> asyncio.Lock:
        if self._control_lock is None:
            self._control_lock = asyncio.Lock()
        assert self._control_lock is not None
        return self._control_lock

    def _get_serial_lock(self) -> asyncio.Lock:
        if self._serial_lock is None:
            self._serial_lock = asyncio.Lock()
        assert self._serial_lock is not None
        return self._serial_lock

    async def wait_for_turn(self, chunk_number: int, total_chunks: int) -> None:
        while True:
            async with self._get_control_lock():
                now = time.monotonic()
                wait_until = max(self._cooldown_until, self._next_attempt_at)
                remaining = wait_until - now
                if remaining <= 0:
                    self._next_attempt_at = now + INGEST_ATTEMPT_SPACING_SECONDS
                    return

                reason = (
                    self._cooldown_reason
                    if self._cooldown_until > now
                    else "staggering chunk uploads"
                )
                should_log = now - self._last_wait_log_at >= 5.0 or remaining >= 5.0
                if should_log:
                    self._last_wait_log_at = now

            if should_log:
                logger.info(
                    f"[ingest] Shared backpressure delaying chunk {chunk_number}/{total_chunks} "
                    f"for {remaining:.1f}s ({reason})"
                )
            await asyncio.sleep(min(remaining, 5.0))

    async def acquire_attempt(self) -> Optional[asyncio.Lock]:
        if not self._serialize_attempts:
            return None

        serial_lock = self._get_serial_lock()
        await serial_lock.acquire()
        return serial_lock

    async def register_retryable_failure(
        self,
        exc: BaseException,
        fallback_delay: float,
    ) -> float:
        delay = max(
            fallback_delay,
            min(
                INGEST_RETRY_MAX_DELAY_SECONDS,
                _extract_retry_after_seconds(exc) or 0.0,
            ),
        )
        cooldown_seconds = delay
        is_rate_limit = _is_rate_limit_ingest_error(exc)
        switched_to_serial = False

        async with self._get_control_lock():
            now = time.monotonic()
            self._cooldown_until = max(self._cooldown_until, now + cooldown_seconds)
            if is_rate_limit:
                self._cooldown_reason = "upstream rate limit"
            else:
                self._cooldown_reason = f"retryable {type(exc).__name__}"

            if is_rate_limit and self._serialize_on_rate_limit and not self._serialize_attempts:
                self._serialize_attempts = True
                switched_to_serial = True

        if switched_to_serial:
            logger.warning(
                "[ingest] Shared rate-limit backpressure enabled; "
                "serializing remaining chunk uploads for this document"
            )

        return delay


class GraphitiRAGService:
    """
    Graph-based RAG using Graphiti + FalkorDB.
    """

    def __init__(self) -> None:
        self.graphiti = None
        self._init_lock: Optional[asyncio.Lock] = None
        self._episode_type_text = None
        self._search_recipe = None

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

            os.environ.setdefault("SEMAPHORE_LIMIT", str(GRAPHITI_MAX_COROUTINES))

            from graphiti_core import Graphiti
            from graphiti_core.driver.falkordb_driver import FalkorDriver
            from app.llm.llm_client import get_graphiti_llm_client, get_graphiti_embedder

            host = os.environ.get("FALKORDB_HOST", "falkordb")
            port = int(os.environ.get("FALKORDB_PORT", "6379"))
            llm_client = get_graphiti_llm_client()
            llm_config = getattr(llm_client, "config", None)

            logger.info(f"[rag] Connecting to FalkorDB at {host}:{port}")
            driver = FalkorDriver(
                host=host,
                port=port,
                database="graphiti"
            )
            graphiti_kwargs = {
                "graph_driver": driver,
                "llm_client": llm_client,
                "embedder": get_graphiti_embedder(),
                "max_coroutines": GRAPHITI_MAX_COROUTINES,
            }
            self.graphiti = Graphiti(**_filter_supported_kwargs(Graphiti, graphiti_kwargs))

            try:
                from graphiti_core.nodes import EpisodeType

                self._episode_type_text = EpisodeType.text
            except Exception:
                self._episode_type_text = None

            if GRAPHITI_USE_ADVANCED_SEARCH:
                try:
                    from graphiti_core.search.search_config_recipes import (
                        COMBINED_HYBRID_SEARCH_CROSS_ENCODER,
                    )

                    self._search_recipe = COMBINED_HYBRID_SEARCH_CROSS_ENCODER
                except Exception as exc:
                    logger.info(f"[rag] Advanced Graphiti search recipe unavailable: {exc}")

            if GRAPHITI_BUILD_INDICES:
                build_indices = getattr(self.graphiti, "build_indices_and_constraints", None)
                if callable(build_indices):
                    start = time.perf_counter()
                    try:
                        await build_indices()
                        logger.info(
                            f"[rag] Graphiti indices ensured in {time.perf_counter() - start:.2f}s"
                        )
                    except Exception as exc:
                        logger.warning(f"[rag] Failed to ensure Graphiti indices: {exc}")

            logger.info(
                "[rag] Graphiti initialized ✓ "
                f"(max_coroutines={GRAPHITI_MAX_COROUTINES}, "
                f"ingest_concurrency={INGEST_CONCURRENCY}, "
                f"attempt_spacing={INGEST_ATTEMPT_SPACING_SECONDS:.2f}s, "
                f"chunk_size={CHUNK_SIZE}, overlap={CHUNK_OVERLAP}, "
                f"previous_episode_window={GRAPHITI_PREVIOUS_EPISODE_WINDOW}, "
                f"bulk_batch_size={GRAPHITI_BULK_INGEST_BATCH_SIZE}, "
                f"bulk_max_batch_chars={GRAPHITI_BULK_MAX_BATCH_CHARS}, "
                f"graphiti_model={getattr(llm_config, 'model', 'default')}, "
                f"graphiti_small_model={getattr(llm_config, 'small_model', 'default')})"
            )

    async def initialize(self) -> None:
        await self._ensure_initialized()

    def _build_search_kwargs(
        self,
        question: str,
        limit: int,
        group_ids: Optional[List[str]],
    ) -> dict[str, Any]:
        search_kwargs: dict[str, Any] = {
            "query": question,
            "group_ids": group_ids,
        }

        try:
            search_signature = inspect.signature(self.graphiti.search)
            search_params = search_signature.parameters
        except (TypeError, ValueError, AttributeError):
            search_params = {}

        if "num_results" in search_params:
            search_kwargs["num_results"] = limit

        if self._search_recipe is not None:
            search_config = _copy_with_updates(self._search_recipe, {"limit": limit})
            if "config" in search_params:
                search_kwargs["config"] = search_config
            elif "search_config" in search_params:
                search_kwargs["search_config"] = search_config

        return search_kwargs

    def _normalize_search_results(self, search_results: Any) -> tuple[list[Any], list[Any], list[Any], list[Any]]:
        if search_results is None:
            return [], [], [], []

        if (
            hasattr(search_results, "edges")
            or hasattr(search_results, "nodes")
            or hasattr(search_results, "episodes")
            or hasattr(search_results, "communities")
        ):
            return (
                list(getattr(search_results, "edges", []) or []),
                list(getattr(search_results, "nodes", []) or []),
                list(getattr(search_results, "episodes", []) or []),
                list(getattr(search_results, "communities", []) or []),
            )

        if isinstance(search_results, list):
            return search_results, [], [], []

        try:
            coerced = list(search_results)
        except TypeError:
            coerced = [search_results]
        return coerced, [], [], []

    def _format_search_context(
        self,
        edges: list[Any],
        nodes: list[Any],
        episodes: list[Any],
        communities: list[Any],
    ) -> str:
        facts = _dedupe_strings(
            [getattr(edge, "fact", "") for edge in edges if getattr(edge, "fact", "")],
            GRAPHITI_FACT_LIMIT,
        )

        node_lines = _dedupe_strings(
            [
                (
                    f"{getattr(node, 'name', 'Entity')}: "
                    f"{_truncate(getattr(node, 'summary', '') or '', 220)}"
                ).rstrip(": ")
                for node in nodes
            ],
            GRAPHITI_NODE_LIMIT,
        )

        episode_lines = _dedupe_strings(
            [
                (
                    f"{getattr(episode, 'name', 'Episode')}: "
                    f"{_truncate(getattr(episode, 'content', '') or getattr(episode, 'source_description', ''), 220)}"
                ).rstrip(": ")
                for episode in episodes
            ],
            GRAPHITI_EPISODE_LIMIT,
        )

        community_lines = _dedupe_strings(
            [
                (
                    f"{getattr(community, 'name', 'Community')}: "
                    f"{_truncate(getattr(community, 'summary', '') or '', 220)}"
                ).rstrip(": ")
                for community in communities
            ],
            GRAPHITI_COMMUNITY_LIMIT,
        )

        lines: list[str] = []
        if facts:
            lines.append("Facts:")
            lines.extend(f"- {fact}" for fact in facts)
        if node_lines:
            lines.append("Entities:")
            lines.extend(f"- {line}" for line in node_lines)
        if episode_lines:
            lines.append("Episodes:")
            lines.extend(f"- {line}" for line in episode_lines)
        if community_lines:
            lines.append("Communities:")
            lines.extend(f"- {line}" for line in community_lines)
        return "\n".join(lines)

    async def insert_document(
        self,
        doc_id: str,
        text: str,
        filename: str = "",
        group_id: str = "default",
        previous_episode_uuid: Optional[str] = None,
        previous_episode_uuids: Optional[List[str]] = None,
    ) -> Optional[str]:
        await self._ensure_initialized()
        if not self.graphiti:
            return None

        has_explicit_previous_episodes = previous_episode_uuids is not None
        effective_previous_episode_uuids = [
            episode_uuid
            for episode_uuid in (previous_episode_uuids or [])
            if episode_uuid
        ]

        add_episode_kwargs: dict[str, Any] = {
            "name": filename or doc_id,
            "episode_body": text,
            "source_description": filename or doc_id,
            "reference_time": datetime.now(timezone.utc),
            "group_id": group_id,
            "excluded_entity_types": [],
        }
        if has_explicit_previous_episodes:
            add_episode_kwargs["previous_episode_uuids"] = effective_previous_episode_uuids
        elif previous_episode_uuid and not GRAPHITI_ENABLE_SAGA:
            add_episode_kwargs["previous_episode_uuids"] = [previous_episode_uuid]
        if self._episode_type_text is not None:
            add_episode_kwargs["source"] = self._episode_type_text
        if GRAPHITI_EXTRACTION_INSTRUCTIONS:
            add_episode_kwargs["custom_extraction_instructions"] = GRAPHITI_EXTRACTION_INSTRUCTIONS
        if GRAPHITI_ENABLE_SAGA:
            add_episode_kwargs["saga"] = group_id
            if previous_episode_uuid:
                add_episode_kwargs["saga_previous_episode_uuid"] = previous_episode_uuid

        start = time.perf_counter()
        result = await self.graphiti.add_episode(
            **_filter_supported_kwargs(self.graphiti.add_episode, add_episode_kwargs)
        )
        elapsed = time.perf_counter() - start
        logger.info(
            f"[rag] add_episode completed in {elapsed:.2f}s "
            f"(~{int(len(text)/max(elapsed,0.001)/1000)}K chars/s) for {filename or doc_id}"
        )
        return _extract_episode_uuid(result)

    async def insert_documents_bulk(
        self,
        documents: List[dict[str, str]],
        group_id: str = "default",
    ) -> list[Optional[str]]:
        await self._ensure_initialized()
        if not self.graphiti or not documents:
            return []

        from graphiti_core.utils.bulk_utils import RawEpisode

        episode_source = self._episode_type_text
        if episode_source is None:
            from graphiti_core.nodes import EpisodeType

            episode_source = EpisodeType.text

        reference_time = datetime.now(timezone.utc)
        raw_episodes = [
            RawEpisode(
                name=document["filename"] or document["doc_id"],
                content=document["text"],
                source_description=document["filename"] or document["doc_id"],
                source=episode_source,
                reference_time=reference_time + timedelta(milliseconds=index),
            )
            for index, document in enumerate(documents)
        ]

        add_episode_bulk_kwargs: dict[str, Any] = {
            "bulk_episodes": raw_episodes,
            "group_id": group_id,
            "excluded_entity_types": [],
        }
        if GRAPHITI_EXTRACTION_INSTRUCTIONS:
            add_episode_bulk_kwargs["custom_extraction_instructions"] = GRAPHITI_EXTRACTION_INSTRUCTIONS
        if GRAPHITI_ENABLE_SAGA:
            add_episode_bulk_kwargs["saga"] = group_id

        start = time.perf_counter()
        result = await self.graphiti.add_episode_bulk(
            **_filter_supported_kwargs(self.graphiti.add_episode_bulk, add_episode_bulk_kwargs)
        )
        elapsed = time.perf_counter() - start
        episode_uuids = [getattr(episode, "uuid", None) for episode in getattr(result, "episodes", []) or []]
        total_chars = sum(len(document["text"]) for document in documents)
        logger.info(
            f"[rag] add_episode_bulk completed in {elapsed:.2f}s "
            f"(~{int(total_chars / max(elapsed, 0.001) / 1000)}K chars/s, "
            f"episodes={len(documents)}) for group {group_id}"
        )
        return episode_uuids

    async def query(self, question: str, mode: str = "graph", group_ids: Optional[List[str]] = None) -> str:
        await self._ensure_initialized()
        if not self.graphiti:
            return "[Graphiti not initialized]"

        effective_group_ids = group_ids if group_ids else None
        if group_ids is not None and len(group_ids) == 0:
            logger.info(f"[rag] No chat-doc mapping found, searching across all documents")

        limit = GRAPHITI_GRAPH_SEARCH_LIMIT if mode == "graph" else GRAPHITI_HYBRID_SEARCH_LIMIT
        try:
            start = time.perf_counter()
            search_results = await self.graphiti.search(
                **self._build_search_kwargs(question, limit, effective_group_ids)
            )
            elapsed_ms = (time.perf_counter() - start) * 1000
        except Exception as e:
            logger.error(f"Graphiti search failed for question '{question}': {e}", exc_info=True)
            return f"[Search failed due to technical error: {str(e)}]"

        edges, nodes, episodes, communities = self._normalize_search_results(search_results)
        total_results = len(edges) + len(nodes) + len(episodes) + len(communities)
        if total_results == 0:
            logger.warning(f"[rag] No relevant context found in Graphiti for query: {question} (groups: {effective_group_ids})")
            return "[No relevant context found]"

        logger.info(
            "[rag] Search returned "
            f"edges={len(edges)}, nodes={len(nodes)}, episodes={len(episodes)}, "
            f"communities={len(communities)} in {elapsed_ms:.1f}ms for: {question}"
        )
        context = self._format_search_context(edges, nodes, episodes, communities)
        if not context:
            logger.warning(
                f"[rag] Search returned {total_results} graph items but no usable context for: {question}"
            )
            return "[No relevant context found]"
        return context

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
    started_at = time.perf_counter()
    rag_task: asyncio.Task[GraphitiRAGService] = asyncio.create_task(_get_rag_service())

    try:
        if filename.lower().endswith(".pdf"):
            logger.info(f"[ingest] Extracting PDF text: {filename}")
            loop = asyncio.get_running_loop()
            text = await loop.run_in_executor(None, _extract_pdf_text, raw_bytes)
        else:
            text = _normalize_document_text(raw_bytes.decode("utf-8", errors="ignore"))

        if not text.strip():
            logger.warning(f"[ingest] No text extracted from {filename} — marking failed")
            _mark_status(doc_id, "failed")
            if not rag_task.done():
                rag_task.cancel()
            return

        chunks = _chunk_text(text)
        chunk_count_before_compaction = len(chunks)
        chunks = _compact_chunks(chunks)
        non_empty_chunks = [(i, chunk) for i, chunk in enumerate(chunks) if chunk.strip()]
        total_chunks = len(non_empty_chunks)

        if total_chunks == 0:
            logger.warning(f"[ingest] No usable chunks produced for {filename} — marking failed")
            _mark_status(doc_id, "failed")
            if not rag_task.done():
                rag_task.cancel()
            return

        avg_chunk_size = sum(len(chunk) for chunk in chunks) // max(len(chunks), 1)
        logger.info(
            f"[ingest] {total_chunks} chunks to ingest for {filename} "
            f"(avg_chars={avg_chunk_size}, chunk_concurrency={INGEST_CONCURRENCY}, "
            f"compacted_from={chunk_count_before_compaction}, "
            f"previous_episode_window={GRAPHITI_PREVIOUS_EPISODE_WINDOW}, "
            f"bulk_batch_size={GRAPHITI_BULK_INGEST_BATCH_SIZE}, "
            f"bulk_max_batch_chars={GRAPHITI_BULK_MAX_BATCH_CHARS})"
        )

        rag = await rag_task
        success_count = 0
        fail_count = 0
        previous_episode_uuid: Optional[str] = None
        recent_episode_uuids: list[str] = []
        state_lock = asyncio.Lock()
        backpressure = _IngestBackpressure(INGEST_CONCURRENCY)

        async def _ingest_one(
            i: int,
            chunk: str,
            chain_previous_uuid: Optional[str] = None,
            context_episode_uuids: Optional[List[str]] = None,
        ) -> Optional[str]:
            nonlocal success_count, fail_count
            chunk_id = f"{doc_id}_{i}"
            chunk_label = f"{filename} (part {i + 1}/{total_chunks})"
            retry_count = 0

            while True:
                await backpressure.wait_for_turn(i + 1, total_chunks)
                serial_lock = await backpressure.acquire_attempt()
                try:
                    logger.debug(f"[ingest] - chunk {i + 1}/{total_chunks}")
                    episode_uuid = await rag.insert_document(
                        doc_id=chunk_id,
                        text=chunk,
                        filename=chunk_label,
                        group_id=doc_id,
                        previous_episode_uuid=chain_previous_uuid,
                        previous_episode_uuids=context_episode_uuids,
                    )
                    if retry_count:
                        logger.info(
                            f"[ingest] - chunk {i + 1}/{total_chunks} succeeded after {retry_count} retr"
                            f"{'y' if retry_count == 1 else 'ies'}"
                        )
                    async with state_lock:
                        success_count += 1
                    return episode_uuid
                except Exception as e:
                    retryable = _is_retryable_ingest_error(e)
                    if retryable and retry_count < INGEST_RETRY_ATTEMPTS:
                        retry_count += 1
                        delay = await backpressure.register_retryable_failure(
                            e,
                            _ingest_retry_delay_seconds(retry_count),
                        )
                        logger.warning(
                            f"[ingest] - chunk {i + 1}/{total_chunks} retry {retry_count}/"
                            f"{INGEST_RETRY_ATTEMPTS} in {delay:.1f}s after {type(e).__name__}: {e}"
                        )
                        continue

                    async with state_lock:
                        fail_count += 1
                    suffix = " (retries exhausted)" if retryable and retry_count else ""
                    logger.warning(f"[ingest] - chunk {i + 1} failed{suffix} (continuing): {e}")
                    return None
                finally:
                    if serial_lock is not None and serial_lock.locked():
                        serial_lock.release()

        async def _ingest_batch(
            batch_idx: int,
            batch_items: list[tuple[int, str]],
            total_batches: int,
        ) -> bool:
            nonlocal success_count, fail_count, previous_episode_uuid, recent_episode_uuids
            retry_count = 0
            batch_number = batch_idx + 1
            batch_start_chunk = batch_items[0][0] + 1
            batch_end_chunk = batch_items[-1][0] + 1
            batch_char_count = sum(len(chunk) for _, chunk in batch_items)
            batch_documents = [
                {
                    "doc_id": f"{doc_id}_{i}",
                    "text": chunk,
                    "filename": f"{filename} (part {i + 1}/{total_chunks})",
                }
                for i, chunk in batch_items
            ]

            while True:
                await backpressure.wait_for_turn(batch_number, total_batches)
                serial_lock = await backpressure.acquire_attempt()
                try:
                    logger.info(
                        f"[ingest] batch {batch_number}/{total_batches} "
                        f"(chunks {batch_start_chunk}-{batch_end_chunk}, {batch_char_count} chars) "
                        f"— graphiti bulk"
                    )
                    episode_uuids = await rag.insert_documents_bulk(batch_documents, group_id=doc_id)

                    async with state_lock:
                        success_count += len(batch_items)
                        if GRAPHITI_PREVIOUS_EPISODE_WINDOW > 0:
                            recent_episode_uuids.extend(
                                [uuid for uuid in episode_uuids if uuid]
                            )
                            recent_episode_uuids = recent_episode_uuids[-GRAPHITI_PREVIOUS_EPISODE_WINDOW:]
                        previous_episode_uuid = next(
                            (uuid for uuid in reversed(episode_uuids) if uuid),
                            previous_episode_uuid,
                        )

                    if retry_count:
                        logger.info(
                            f"[ingest] - batch {batch_number}/{total_batches} succeeded after {retry_count} retr"
                            f"{'y' if retry_count == 1 else 'ies'}"
                        )
                    return True
                except Exception as e:
                    retryable = _is_retryable_ingest_error(e)
                    if retryable and retry_count < INGEST_RETRY_ATTEMPTS:
                        retry_count += 1
                        delay = await backpressure.register_retryable_failure(
                            e,
                            _ingest_retry_delay_seconds(retry_count),
                        )
                        logger.warning(
                            f"[ingest] - batch {batch_number}/{total_batches} retry {retry_count}/"
                            f"{INGEST_RETRY_ATTEMPTS} in {delay:.1f}s after {type(e).__name__}: {e}"
                        )
                        continue

                    logger.warning(
                        f"[ingest] - batch {batch_number}/{total_batches} failed after bulk attempt: {e}. "
                        "Falling back to single-chunk ingestion for this batch"
                    )
                    batch_success = True
                    for i, chunk in batch_items:
                        episode_uuid = await _ingest_one(
                            i,
                            chunk,
                            previous_episode_uuid,
                            recent_episode_uuids[-GRAPHITI_PREVIOUS_EPISODE_WINDOW:]
                            if GRAPHITI_PREVIOUS_EPISODE_WINDOW > 0
                            else [],
                        )
                        async with state_lock:
                            if episode_uuid and GRAPHITI_PREVIOUS_EPISODE_WINDOW > 0:
                                recent_episode_uuids.append(episode_uuid)
                                recent_episode_uuids = recent_episode_uuids[-GRAPHITI_PREVIOUS_EPISODE_WINDOW:]
                            previous_episode_uuid = episode_uuid or previous_episode_uuid
                            if episode_uuid is None:
                                batch_success = False
                    return batch_success
                finally:
                    if serial_lock is not None and serial_lock.locked():
                        serial_lock.release()

        if GRAPHITI_BULK_INGEST_BATCH_SIZE > 1 and total_chunks > 1:
            bulk_batches = _group_chunks_for_bulk_ingest(non_empty_chunks)
            total_batches = len(bulk_batches)
            logger.info(
                f"[ingest] Bulk Graphiti ingestion enabled: batch_size={GRAPHITI_BULK_INGEST_BATCH_SIZE}, "
                f"max_batch_chars={GRAPHITI_BULK_MAX_BATCH_CHARS}, "
                f"total_batches={total_batches}, "
                f"concurrency={INGEST_CONCURRENCY}, "
                f"GRAPHITI_MAX_COROUTINES={GRAPHITI_MAX_COROUTINES}"
            )

            batch_semaphore = asyncio.Semaphore(INGEST_CONCURRENCY)

            async def _guarded_batch(batch_idx: int, batch: list) -> None:
                async with batch_semaphore:
                    await _ingest_batch(batch_idx, batch, total_batches)

            await asyncio.gather(*[
                _guarded_batch(batch_idx, batch)
                for batch_idx, batch in enumerate(bulk_batches)
            ])

        elif INGEST_CONCURRENCY == 1:
            for idx, (i, chunk) in enumerate(non_empty_chunks):
                logger.info(f"[ingest] chunk {idx + 1}/{total_chunks} ({len(chunk)} chars) — sequential")
                episode_uuid = await _ingest_one(
                    i,
                    chunk,
                    previous_episode_uuid,
                    recent_episode_uuids[-GRAPHITI_PREVIOUS_EPISODE_WINDOW:]
                    if GRAPHITI_PREVIOUS_EPISODE_WINDOW > 0
                    else [],
                )
                async with state_lock:
                    if episode_uuid and GRAPHITI_PREVIOUS_EPISODE_WINDOW > 0:
                        recent_episode_uuids.append(episode_uuid)
                        recent_episode_uuids = recent_episode_uuids[-GRAPHITI_PREVIOUS_EPISODE_WINDOW:]
                    previous_episode_uuid = episode_uuid or previous_episode_uuid
        else:
            logger.info(
                f"[ingest] Parallel ingestion: {total_chunks} chunks, "
                f"concurrency={INGEST_CONCURRENCY}, GRAPHITI_MAX_COROUTINES={GRAPHITI_MAX_COROUTINES}"
            )
            semaphore = asyncio.Semaphore(INGEST_CONCURRENCY)

            async def _guarded_ingest(idx: int, i: int, chunk: str) -> None:
                async with semaphore:
                    logger.info(f"[ingest] chunk {idx + 1}/{total_chunks} ({len(chunk)} chars) — parallel")
                    await _ingest_one(i, chunk, context_episode_uuids=[])

            await asyncio.gather(*[
                _guarded_ingest(idx, i, chunk)
                for idx, (i, chunk) in enumerate(non_empty_chunks)
            ])

        logger.info(
            f"[ingest] - Done: {filename} — "
            f"{success_count} ok, {fail_count} failed out of {total_chunks} chunks "
            f"in {time.perf_counter() - started_at:.2f}s"
        )
        if fail_count > 0:
            if success_count > 0:
                logger.warning(
                    f"[ingest] Partial ingest detected for {filename}; "
                    f"cleaning up {success_count} successful chunks before marking failed"
                )
                try:
                    await rag.delete_document(doc_id)
                    logger.info(f"[ingest] Removed partial graph data for {filename} ({doc_id})")
                except Exception as cleanup_exc:
                    logger.error(
                        f"[ingest] Failed to remove partial graph data for {filename} ({doc_id}): "
                        f"{cleanup_exc}",
                        exc_info=True,
                    )
            else:
                logger.warning(f"[ingest] No chunks were ingested successfully for {filename}")
            _mark_status(doc_id, "failed")
        else:
            _mark_status(doc_id, "completed")

    except Exception as e:
        if not rag_task.done():
            rag_task.cancel()
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