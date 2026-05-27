from langgraph.graph import StateGraph, END
from langchain_ollama.embeddings import OllamaEmbeddings
from langchain_ollama.chat_models import ChatOllama
from langchain_community.vectorstores import FAISS
from langchain_core.tools.retriever import create_retriever_tool
from typing import Annotated, TypedDict, Sequence
from langchain_core.messages import BaseMessage, SystemMessage, ToolMessage
from operator import add as add_messages
import faiss
from langchain_community.docstore.in_memory import InMemoryDocstore
import os
import aiosqlite
from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver
from dotenv import load_dotenv
from langchain_core.runnables import RunnableConfig

load_dotenv()

OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL","http://host.docker.internal:11434")

class AgentState(TypedDict):
    messages: Annotated[Sequence[BaseMessage], add_messages]

system_prompt = """You are a helpful and concise AI assistant. You have access to a retrieval tool to look up private documents and database records.

CRITICAL RULES FOR TOOL USAGE:
1. CASUAL CHAT: If the user says hello, asks how you are, or makes small talk, respond naturally. DO NOT use any tools.
2. GENERAL KNOWLEDGE: If the user asks a basic question (e.g., "What is the capital of France?"), answer directly. DO NOT use any tools.
3. SPECIFIC QUERIES: ONLY call the retrieval tool when the user asks about [Insert Your Specific Domain Here, e.g., internal company policies, user data, or specific product manuals].
4. NO GUESSING: If you use the tool and it does not contain the answer, explicitly state "I cannot find that information in the documents." Do not invent answers.

Think carefully about the user's intent before deciding to use a tool."""

class AiAssistant:
    def __init__(self):
        ollama_url = OLLAMA_BASE_URL
        # 1. Use a dedicated embedding model (ensure you run `ollama pull nomic-embed-text`)
        self.embedding = OllamaEmbeddings(model='nomic-embed-text', base_url=ollama_url)
        
        self.index_path = "faiss_db_store"

        if os.path.exists(self.index_path):
            #load faiss db from local path
            self.vector_store = FAISS.load_local(
                folder_path=self.index_path, 
                embeddings=self.embedding, 
                allow_dangerous_deserialization=True
            )
        else:
            # 2. Initialize Vector Store
            index = faiss.IndexFlatL2(len(self.embedding.embed_query("hello world")))
            self.vector_store = FAISS(
                embedding_function=self.embedding,
                index=index, 
                docstore=InMemoryDocstore(), 
                index_to_docstore_id={}
            )
        self.retriever = self.vector_store.as_retriever(search_kwargs={'k': 5})
        
        # 3. Create the tool using LangChain's native function (avoids 'self' bugs)
        self.retriever_tool = create_retriever_tool(
            self.retriever,
            name="retriever_tool",
            description="Search the knowledge base for relevant information."
        )
        self.tools = [self.retriever_tool]
        
        # 4. Initialize and bind LLM
        self.llm = ChatOllama(model="qwen3:4b", 
        temperature=0,      # Zero creativity for strict tool discipline
        num_ctx=2048, base_url=ollama_url)
        self.llm_with_tools = self.llm.bind_tools(self.tools)
        
        self.db_conn = None
        self.memory = None
        self.rag_agent = None

        # 5. Build Graph
        self.graph = StateGraph(AgentState)
        self._create_graph()

    async def initialize_checkpointer(self, db_file: str = "checkpoints.sqlite"):
        """Asynchronously sets up the database checkpointer and compiles the graph."""
        self.db_conn = await aiosqlite.connect(db_file, check_same_thread=False)
        self.memory = AsyncSqliteSaver(self.db_conn)
        
        await self.memory.setup()
        
        self.rag_agent = self.graph.compile(checkpointer=self.memory)
    async def close(self):
        """Cleanly releases database resources."""
        if self.db_conn:
            await self.db_conn.close()
    def ingest_documents(self, texts: list[str]):
        """Call this method to actually put data into your vector store."""
        self.vector_store.add_texts(texts)
        self.vector_store.save_local(self.index_path)

    def should_continue(self, state: AgentState) -> str:
        last_message = state['messages'][-1]
        if hasattr(last_message, 'tool_calls') and len(last_message.tool_calls) > 0:
            return "retriever_agent"
        return END
    
    async def call_llm(self, state: AgentState, config:RunnableConfig = None, **kwargs) -> dict:
        
        run_config = config or kwargs.get("runnable_config") or kwargs.get("config")
        
        messages = list(state['messages'])
        # Only inject system prompt if it's the first message to save tokens
        if not any(isinstance(m, SystemMessage) for m in messages):
            messages = [SystemMessage(content=system_prompt)] + messages
            
        response = await self.llm_with_tools.ainvoke(messages, config=run_config)
        return {"messages": [response]}
    
    def tool_action(self, state: AgentState) -> dict:
        last_message = state['messages'][-1]
        results = []
        
        for t in last_message.tool_calls:
            # Match tool name and invoke
            if t['name'] == self.retriever_tool.name:
                tool_result = self.retriever_tool.invoke(t['args'])
            else:
                tool_result = "Error: Tool not found."
                
            results.append(ToolMessage(
                tool_call_id=t['id'], 
                name=t['name'], 
                content=str(tool_result)
            ))

        return {'messages': results}
    
    def _create_graph(self):
        self.graph.add_node("llm", self.call_llm)
        self.graph.add_node("retriever_agent", self.tool_action)
        
        self.graph.add_conditional_edges("llm", self.should_continue)
        self.graph.add_edge("retriever_agent", "llm")
        self.graph.set_entry_point("llm")