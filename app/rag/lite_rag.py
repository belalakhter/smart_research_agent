import os
import asyncio
import numpy as np
from lightrag import LightRAG, QueryParam
from lightrag.llm.gemini import gemini_model_complete, gemini_embed
from lightrag.utils import setup_logger, wrap_embedding_func_with_attrs

setup_logger("lightrag", level="INFO")

class LiteRAGService:
    def __init__(self, working_dir: str = "./rag_storage"):
        self.working_dir = working_dir
        self.api_key = os.environ.get("GEMINI_API_KEY")
        
        if not self.api_key:
            raise ValueError("GEMINI_API_KEY environment variable is not set.")

        if not os.path.exists(self.working_dir):
            os.makedirs(self.working_dir)

        self.rag = LightRAG(
            working_dir=self.working_dir,
            llm_model_func=self._llm_model_func,
            embedding_func=self._embedding_func,
            llm_model_name="gemini-2.0-flash",
        )

    async def _llm_model_func(self, prompt, system_prompt=None, history_messages=[], **kwargs):
        return await gemini_model_complete(
            prompt,
            system_prompt=system_prompt,
            history_messages=history_messages,
            api_key=self.api_key,
            model_name="gemini-2.0-flash",
            **kwargs,
        )

    @wrap_embedding_func_with_attrs(
        embedding_dim=768,
        send_dimensions=True,
        max_token_size=2048,
        model_name="models/text-embedding-004",
    )
    async def _embedding_func(self, texts: list[str]) -> np.ndarray:
        return await gemini_embed.func(
            texts, 
            api_key=self.api_key, 
            model="models/text-embedding-004"
        )

    async def initialize(self):
        """Must be called on app startup"""
        await self.rag.initialize_storages()

    async def insert_text(self, text: str):
        """Index a string of text"""
        await self.rag.ainsert(text)

    async def query(self, question: str, mode: str = "hybrid"):
        """
        Query the RAG system.
        Modes: 'naive', 'local', 'global', 'hybrid'
        """
        return await self.rag.aquery(
            question, 
            param=QueryParam(mode=mode)
        )

    async def finalize(self):
        """Safely close storage connections"""
        await self.rag.finalize_storages()