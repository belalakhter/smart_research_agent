import logging
from langgraph.graph import StateGraph, END
from app.agent.state import AgentState
from app.agent.nodes import (
    node_prepare,
    node_router,
    node_rag_semantic,
    node_rag_graph,
    node_web_search,
    node_llm,
)

logger = logging.getLogger(__name__)

def router_condition(state: AgentState):
    if state.strategy == "B":
        return "rag_graph"
    return "rag_semantic"

def create_graph():
    workflow = StateGraph(AgentState)

    workflow.add_node("prepare", node_prepare)
    workflow.add_node("router", node_router)
    workflow.add_node("rag_semantic", node_rag_semantic)
    workflow.add_node("rag_graph", node_rag_graph)
    workflow.add_node("web_search", node_web_search)
    workflow.add_node("llm", node_llm)

    workflow.set_entry_point("prepare")
    workflow.add_edge("prepare", "router")

    workflow.add_conditional_edges(
        "router",
        router_condition,
        {
            "rag_semantic": "rag_semantic",
            "rag_graph": "rag_graph"
        }
    )

    workflow.add_edge("rag_semantic", "web_search")
    workflow.add_edge("rag_graph", "web_search")
    workflow.add_edge("web_search", "llm")
    workflow.add_edge("llm", END)

    return workflow.compile()

_app = create_graph()

def run_agent(chat_id: str, messages: list[dict]) -> str:
    """
    Entry-point called by the chat route.
    """
    initial_state = AgentState(chat_id=chat_id, messages=messages)

    try:
        final_state = _app.invoke(initial_state)
        return final_state["final_reply"] or "I was unable to generate a response."
    except Exception as e:
        logger.error(f"[graph] Error invoking graph: {e}", exc_info=True)
        return f"An error occurred: {e}"