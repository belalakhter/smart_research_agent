import os
from typing import Optional
from openai import OpenAI
from graphiti_core.llm_client.openai_client import OpenAIClient
from graphiti_core.llm_client.config import LLMConfig
from graphiti_core.embedder.openai import OpenAIEmbedder, OpenAIEmbedderConfig

_client = OpenAI(
    api_key=os.environ.get("OPENAI_API_KEY", ""),
)

LLM_MODEL = os.environ.get("LLM_MODEL", "gpt-4o")
GRAPHITI_SMALL_MODEL = os.environ.get("GRAPHITI_SMALL_MODEL", "gpt-4o-mini").strip() or "gpt-4o-mini"
GRAPHITI_MODEL = os.environ.get("GRAPHITI_MODEL", GRAPHITI_SMALL_MODEL).strip() or GRAPHITI_SMALL_MODEL


def chat_completion(
    messages: list[dict],
    system_prompt: Optional[str] = None,
    temperature: float = 0.7,
    max_tokens: int = 2048,
) -> str:
    """
    Send a list of {"role": "user"|"assistant", "content": "..."} messages
    to OpenAI and return the assistant reply as a string.
    """
    formatted: list[dict] = []

    if system_prompt:
        formatted.append({"role": "system", "content": system_prompt})

    for m in messages:
        formatted.append({"role": m["role"], "content": m["content"]})

    response = _client.chat.completions.create(
        model=LLM_MODEL,
        messages=formatted,
        temperature=temperature,
        max_tokens=max_tokens,
    )

    return response.choices[0].message.content


def get_graphiti_llm_client() -> OpenAIClient:
    """Return a Graphiti-compatible OpenAI LLM client.
    """
    config = LLMConfig(
        api_key=os.environ.get("OPENAI_API_KEY", ""),
        model=GRAPHITI_MODEL,
        small_model=GRAPHITI_SMALL_MODEL,
    )
    return OpenAIClient(config=config)


def get_graphiti_embedder() -> OpenAIEmbedder:
    """Return a Graphiti-compatible OpenAI Embedder."""
    config = OpenAIEmbedderConfig(
        api_key=os.environ.get("OPENAI_API_KEY", ""),
        embedding_model="text-embedding-3-small"
    )
    return OpenAIEmbedder(config=config)
