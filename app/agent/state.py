from dataclasses import dataclass, field
from typing import Optional, List


@dataclass
class AgentState:
    chat_id: str
    messages: list[dict]
    last_user_message: str = ""
    conversation_context: str = ""
    strategy: str = "A"
    search_query: str = ""
    response_mode: str = "standard"
    requested_depth: str = "standard"
    include_table: bool = False
    use_web_search: bool = False
    analysis_focus: List[str] = field(default_factory=list)
    response_guidance: str = ""
    rag_context: Optional[str] = None
    web_results: Optional[str] = None
    final_reply: Optional[str] = None
    error: Optional[str] = None
    available_doc_ids: List[str] = field(default_factory=list)
    has_documents: bool = False
