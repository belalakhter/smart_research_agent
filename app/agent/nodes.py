import logging
import json
import os

from app.agent.state import AgentState
from app.agent.memory import trim_messages, last_user_message
from app.agent.mcp_client import web_search, should_search_web
from app.llm.llm_client import chat_completion
from app.llm.prompts import (
    AGENT_SYSTEM_PROMPT,
    RAG_CONTEXT_TEMPLATE,
    WEB_SEARCH_TEMPLATE,
    ROUTER_PROMPT,
    QUERY_REFORMULATE_PROMPT,
    RESPONSE_PLAN_TEMPLATE,
)

logger = logging.getLogger(__name__)
FINAL_RESPONSE_MAX_TOKENS = int(os.environ.get("AGENT_MAX_RESPONSE_TOKENS", "2800"))


def _contains_any(text: str, phrases: list[str]) -> bool:
    lowered = text.lower()
    return any(phrase in lowered for phrase in phrases)


def _build_conversation_context(messages: list[dict], max_messages: int = 6, max_chars: int = 2400) -> str:
    relevant = [
        m for m in messages
        if m.get("role") in {"user", "assistant"} and (m.get("content") or "").strip()
    ]
    tail = relevant[-max_messages:]
    parts: list[str] = []
    total_chars = 0

    for message in reversed(tail):
        content = " ".join(message.get("content", "").split())
        if not content:
            continue
        line = f"{message['role']}: {content}"
        if total_chars + len(line) > max_chars:
            remaining = max(0, max_chars - total_chars - 3)
            if remaining > 40:
                line = line[:remaining].rstrip() + "..."
                parts.append(line)
            break
        parts.append(line)
        total_chars += len(line) + 1

    return "\n".join(reversed(parts))


def _extract_json_object(raw: str) -> dict:
    if not raw:
        return {}

    text = raw.strip()
    if text.startswith("```"):
        lines = [line for line in text.splitlines() if not line.strip().startswith("```")]
        text = "\n".join(lines).strip()

    start = text.find("{")
    end = text.rfind("}")
    if start == -1 or end == -1 or end < start:
        return {}

    try:
        payload = json.loads(text[start : end + 1])
    except json.JSONDecodeError:
        return {}

    return payload if isinstance(payload, dict) else {}


def _infer_analysis_focus(user_message: str) -> list[str]:
    lowered = user_message.lower()
    focus: list[str] = []

    if _contains_any(lowered, ["experience", "career", "background", "journey", "profile"]):
        focus.extend([
            "career trajectory",
            "role progression",
            "technical depth",
            "notable signals",
        ])

    if _contains_any(lowered, ["risk", "10-k", "10k", "annual report", "form 10-k", "form 10k"]):
        focus.extend([
            "key risk categories",
            "business implications",
            "investor takeaways",
            "watch items",
        ])

    if _contains_any(lowered, ["financial", "revenue", "margin", "cash flow", "balance sheet"]):
        focus.extend([
            "financial signals",
            "operating pressure points",
            "trend interpretation",
        ])

    if _contains_any(lowered, ["compare", "comparison", "versus", "vs"]):
        focus.extend([
            "comparison points",
            "major differences",
            "decision implications",
        ])

    if _contains_any(lowered, ["insight", "insights", "analysis", "analyze", "what does this mean"]):
        focus.extend([
            "key insights",
            "patterns",
            "practical implications",
        ])

    deduped: list[str] = []
    for item in focus:
        if item not in deduped:
            deduped.append(item)

    return deduped[:5] or ["key facts", "important implications"]


def _infer_response_mode(user_message: str) -> str:
    lowered = user_message.lower()
    if _contains_any(
        lowered,
        [
            "insight", "insights", "analyze", "analysis", "annual report",
            "10-k", "10k", "risk", "tell me about", "experience",
            "background", "overview", "report", "deep", "detailed",
        ],
    ):
        return "mini_report"
    if _contains_any(lowered, ["brief", "short", "quick", "one-line", "one line"]):
        return "brief"
    return "standard"


def _infer_requested_depth(user_message: str, response_mode: str) -> str:
    lowered = user_message.lower()
    if _contains_any(lowered, ["deep", "detailed", "comprehensive", "thorough"]):
        return "deep"
    if _contains_any(lowered, ["brief", "short", "quick", "concise"]):
        return "light" if response_mode != "standard" else "standard"
    if response_mode == "mini_report":
        return "standard"
    return "standard"


def _infer_include_table(user_message: str, response_mode: str) -> bool:
    lowered = user_message.lower()
    if _contains_any(lowered, ["table", "tabular", "spreadsheet", "grid"]):
        return True
    if _contains_any(
        lowered,
        [
            "timeline", "experience", "career", "risk", "10-k", "10k",
            "annual report", "breakdown", "financial", "compare", "comparison",
            "versus", "vs", "metrics",
        ],
    ):
        return True
    return response_mode == "mini_report" and _contains_any(
        lowered,
        ["summary", "insights", "overview", "analysis"],
    )


def _infer_strategy(user_message: str) -> str:
    lowered = user_message.lower()
    follow_up_phrases = [
        "what about", "and what", "also", "same as", "previous", "above",
    ]
    pronouns = {"that", "those", "them", "it", "this", "these"}
    tokens = {token.strip(".,!?;:()[]{}") for token in lowered.split()}

    if len(lowered.split()) <= 8 and (
        _contains_any(lowered, follow_up_phrases) or bool(tokens & pronouns)
    ):
        return "A"
    if _contains_any(lowered, ["compare", "difference", "similar", "same as"]):
        return "A"
    return "B"


def _infer_response_guidance(state: AgentState) -> str:
    if state.response_mode == "mini_report":
        if state.include_table:
            return (
                "Write a compact mini-report with an executive summary, key insights, "
                "and a Markdown table where it improves clarity. Separate direct evidence "
                "from inference and end with a short takeaway."
            )
        return (
            "Write a compact mini-report with an executive summary, key insights, and a "
            "short closing takeaway. Separate direct evidence from inference."
        )
    if state.response_mode == "brief":
        return "Answer briefly in a few high-signal bullets without losing the core insight."
    return "Answer clearly with synthesis, not just raw fact repetition."


def _fallback_search_query(state: AgentState) -> str:
    if state.strategy == "A" and state.conversation_context:
        try:
            reformulated = chat_completion(
                messages=[{
                    "role": "user",
                    "content": (
                        f"History:\n{state.conversation_context}\n\n"
                        f"Question: {state.last_user_message}"
                    ),
                }],
                system_prompt=QUERY_REFORMULATE_PROMPT,
                temperature=0,
                max_tokens=250,
            )
            if reformulated.strip():
                return reformulated.strip()
        except Exception as exc:
            logger.warning(f"[router] Query reformulation fallback failed: {exc}")

    return state.last_user_message


def _apply_fallback_plan(state: AgentState) -> AgentState:
    state.strategy = _infer_strategy(state.last_user_message)
    state.response_mode = _infer_response_mode(state.last_user_message)
    state.requested_depth = _infer_requested_depth(state.last_user_message, state.response_mode)
    state.include_table = _infer_include_table(state.last_user_message, state.response_mode)
    state.use_web_search = should_search_web(state.last_user_message, state.has_documents)
    state.analysis_focus = _infer_analysis_focus(state.last_user_message)
    state.search_query = _fallback_search_query(state)
    state.response_guidance = _infer_response_guidance(state)
    return state


def _response_plan_block(state: AgentState) -> str:
    focus_text = ", ".join(state.analysis_focus) if state.analysis_focus else "key facts"
    return RESPONSE_PLAN_TEMPLATE.format(
        response_mode=state.response_mode,
        requested_depth=state.requested_depth,
        include_table="yes" if state.include_table else "no",
        analysis_focus=focus_text,
        response_guidance=state.response_guidance or "Answer clearly and stay grounded in evidence.",
    )


def node_prepare(state: AgentState) -> AgentState:
    """Extract information, trim history, and load available docs."""
    state.last_user_message = last_user_message(state.messages)
    state.messages = trim_messages(state.messages)
    history_messages = state.messages
    if history_messages and history_messages[-1].get("role") == "user":
        history_messages = history_messages[:-1]
    state.conversation_context = _build_conversation_context(history_messages)
    
    from app.services.map_store import doc_map
    state.available_doc_ids = doc_map.get_docs(state.chat_id)
    state.has_documents = len(state.available_doc_ids) > 0
    
    if state.has_documents:
        logger.info(f"[prepare] Chat {state.chat_id} has {len(state.available_doc_ids)} docs.")
    
    return state


def node_router(state: AgentState) -> AgentState:
    """
    Decide between:
    - Strategy A: Semantic RAG + History (for follow-ups/conversational)
    - Strategy B: Pure Graph Context (for factual extraction/entity-based)
    """
    if not state.last_user_message:
        return state

    try:
        planner_input = (
            f"Has uploaded documents: {'yes' if state.has_documents else 'no'}\n"
            f"Conversation context:\n{state.conversation_context or '[none]'}\n\n"
            f"Latest user message:\n{state.last_user_message}"
        )
        decision_raw = chat_completion(
            messages=[{"role": "user", "content": planner_input}],
            system_prompt=ROUTER_PROMPT,
            temperature=0,
            max_tokens=600,
        )
        plan = _extract_json_object(decision_raw)

        state.strategy = str(plan.get("strategy", "")).strip().upper()[:1]
        if state.strategy not in {"A", "B"}:
            state.strategy = _infer_strategy(state.last_user_message)

        state.response_mode = str(plan.get("response_mode", "")).strip().lower()
        if state.response_mode not in {"brief", "standard", "mini_report"}:
            state.response_mode = _infer_response_mode(state.last_user_message)

        state.requested_depth = str(plan.get("requested_depth", "")).strip().lower()
        if state.requested_depth not in {"light", "standard", "deep"}:
            state.requested_depth = _infer_requested_depth(state.last_user_message, state.response_mode)

        include_table = plan.get("include_table")
        if isinstance(include_table, bool):
            state.include_table = include_table
        else:
            state.include_table = _infer_include_table(state.last_user_message, state.response_mode)

        use_web_search = plan.get("use_web_search")
        if isinstance(use_web_search, bool):
            state.use_web_search = use_web_search
        else:
            state.use_web_search = should_search_web(state.last_user_message, state.has_documents)

        if state.has_documents and not _contains_any(
            state.last_user_message.lower(),
            ["latest", "current", "today", "news", "recent", "search the web", "online"],
        ):
            state.use_web_search = False

        analysis_focus = plan.get("analysis_focus")
        if isinstance(analysis_focus, list):
            state.analysis_focus = [str(item).strip() for item in analysis_focus if str(item).strip()][:5]
        if not state.analysis_focus:
            state.analysis_focus = _infer_analysis_focus(state.last_user_message)

        response_guidance = str(plan.get("response_guidance", "")).strip()
        state.response_guidance = response_guidance or _infer_response_guidance(state)

        search_query = str(plan.get("search_query", "")).strip()
        state.search_query = search_query or _fallback_search_query(state)

        logger.info(
            "[router] "
            f"strategy={state.strategy}, "
            f"response_mode={state.response_mode}, "
            f"depth={state.requested_depth}, "
            f"table={state.include_table}, "
            f"web={state.use_web_search}, "
            f"focus={state.analysis_focus}"
        )

    except Exception as e:
        logger.warning(f"Router node failed: {e}")
        state = _apply_fallback_plan(state)

    return state

def node_rag_semantic(state: AgentState) -> AgentState:
    """Strategy A: Semantic RAG using full history context."""
    if state.strategy != "A" or not state.search_query:
        return state

    hybrid_ctx = _sync_rag_query(
        state.search_query, mode="hybrid", doc_ids=state.available_doc_ids
    )
    graph_ctx = _sync_rag_query(
        state.search_query, mode="graph", doc_ids=state.available_doc_ids
    )

    contexts = [c for c in [hybrid_ctx, graph_ctx] if c and "[No relevant" not in c]
    state.rag_context = "\n\n".join(contexts) if contexts else ""
    return state

def node_rag_graph(state: AgentState) -> AgentState:
    """Strategy B: Pure Graph context without history dependence."""
    if state.strategy != "B" or not state.search_query:
        return state

    queries = [state.search_query]
    
    if state.analysis_focus and len(state.analysis_focus) > 1:
        for focus in state.analysis_focus[:3]:
            focused_q = f"{focus}: {state.last_user_message}"
            if focused_q not in queries:
                queries.append(focused_q)

    contexts = []
    for q in queries:
        ctx = _sync_rag_query(q, mode="graph", doc_ids=state.available_doc_ids)
        if ctx and "[No relevant" not in ctx:
            contexts.append(ctx)

    state.rag_context = "\n\n".join(contexts) if contexts else ""
    return state

def _sync_rag_query(query: str, mode: str, doc_ids: list[str] = None) -> str:
    try:
        from app.services.worker_threads import submit_async
        from app.rag.rag_processing import _get_rag_service

        async def _do_query():
            rag = await _get_rag_service()
            return await rag.query(query, mode=mode, group_ids=doc_ids)

        context = submit_async(_do_query(), wait=True, timeout=30)
        logger.info(f"[rag] Query='{query}' mode={mode} doc_ids={doc_ids} → {len(context or '')} chars returned")
        if context and "[No relevant" not in context:
            return RAG_CONTEXT_TEMPLATE.format(context=context)
    except Exception as e:
        logger.warning(f"RAG query failed: {e}", exc_info=True)
    return ""


def node_web_search(state: AgentState) -> AgentState:
    """Run Tavily web search if needed."""
    if not state.last_user_message:
        return state

    should_use_web = state.use_web_search or should_search_web(
        state.last_user_message,
        state.has_documents,
    )
    if not should_use_web:
        return state

    try:
        results = web_search(state.last_user_message)
        if results and not results.startswith("["):
            state.web_results = WEB_SEARCH_TEMPLATE.format(
                query=state.last_user_message,
                results=results,
            )
    except Exception as e:
        logger.warning(f"Web search node failed: {e}")

    return state


def node_llm(state: AgentState) -> AgentState:
    """Final assembly and LLM response."""

    system_parts = [AGENT_SYSTEM_PROMPT]
    system_parts.append(_response_plan_block(state))

    if state.rag_context:
        system_parts.append(state.rag_context)

    if state.web_results:
        system_parts.append(state.web_results)

    system_prompt = "\n\n".join(system_parts)

    messages = state.messages
    if state.strategy == "B":
        messages = [{"role": "user", "content": state.last_user_message}]

    try:
        reply = chat_completion(
            messages=messages,
            system_prompt=system_prompt,
            temperature=0.2 if (state.rag_context or state.web_results) else 0.5,
            max_tokens=FINAL_RESPONSE_MAX_TOKENS,
        )
        state.final_reply = reply
    except Exception as e:
        logger.error(f"LLM node failed: {e}")
        state.final_reply = f"Error: {e}"

    return state
