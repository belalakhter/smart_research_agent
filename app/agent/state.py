from dataclasses import dataclass, field
from typing import Optional, List

@dataclass
class AgentState:
    chat_id: str
    messages: list[dict]
    last_user_message: str = ""
    strategy: str = "A"
    search_query: str = ""
    rag_context: Optional[str] = None
    web_results: Optional[str] = None
    final_reply: Optional[str] = None
    error: Optional[str] = None
    available_doc_ids: List[str] = field(default_factory=list)
    has_documents: bool = False