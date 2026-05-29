from langgraph.graph import StateGraph, END,START
from langchain_ollama.embeddings import OllamaEmbeddings
from langchain_ollama.chat_models import ChatOllama
from langchain_community.vectorstores import FAISS
from langchain_core.tools.retriever import create_retriever_tool
from typing import Annotated, TypedDict, Sequence, Literal
from langchain_core.messages import BaseMessage, SystemMessage, ToolMessage
from operator import add as add_messages
import faiss
from langchain_community.docstore.in_memory import InMemoryDocstore
import os
import aiosqlite
from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver
from dotenv import load_dotenv
from langchain_core.runnables import RunnableConfig
from pydantic import BaseModel
from router_dataset import router_examples
load_dotenv()

OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL","http://host.docker.internal:11434")

class AgentState(TypedDict):
    messages: Annotated[Sequence[BaseMessage], add_messages]
    next_agent:str

class Router(BaseModel):
    """Route the user query to specialize agent"""
    next:Literal["rag_agent","chat_agent","FINISH"]

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

        text = [ex[0] for ex in router_examples]
        metadata = [{"route":ex[1]} for ex in router_examples]

        self.semantic_router = FAISS.from_texts(texts=text, embedding=self.embedding, metadatas=metadata)
        
        # 4. Initialize and bind LLM
        self.llm = ChatOllama(model="qwen3:4b", 
        temperature=0,      # Zero creativity for strict tool discipline
        num_ctx=2048, base_url=ollama_url)
        self.llm_with_tools = self.llm.bind_tools(self.tools)
        self.supervise_llm = self.llm.with_structured_output(Router)
        self.db_conn = None
        self.memory = None
        self.multi_agent = None

        # 5. Build Graph
        self.graph = StateGraph(AgentState)
        self._create_graph()

    async def get_semantic_route(self,state:AgentState)-> str:
        """Determine which agent should handle the request"""
        user_search = state['messages'][-1].content

        closest = self.semantic_router.similarity_search(user_search,k=1)
        if closest:
            chosen = closest[0].metadata['route']
            return chosen
        return 'chat_agent'
    
    async def chat_agent_node(self, state: AgentState, config: RunnableConfig= None, **kwargs):
        """Handle casual conversation without tool"""
        run_config = config or kwargs.get("runnable_config") or kwargs.get("config")
        system_prompt = "You are a friendly, helpful AI assistant. Answer general questions naturally."
        messages = [SystemMessage(content=system_prompt)] + list(state['messages'])
        response = await self.llm.ainvoke(messages,config=run_config)
        return {"messages":[response]}
    
    async def rag_agent_node(self, state: AgentState, config: RunnableConfig= None, **kwargs):
        """Handle document retrievel query"""
        run_config = config or kwargs.get("runnable_config") or kwargs.get("config")
        system_prompt = (
            "You are a specialized retrieval assistant. Use your tools to look up information. "
            "If the information is not in the documents, explicitly state that you cannot find it."
        )
        messages = [SystemMessage(content=system_prompt)] + list(state['messages'])
        response = await self.llm_with_tools.ainvoke(messages,config=run_config)
        return {"messages":[response]}

    async def initialize_checkpointer(self, db_file: str = "checkpoints.sqlite"):
        """Asynchronously sets up the database checkpointer and compiles the graph."""
        self.db_conn = await aiosqlite.connect(db_file, check_same_thread=False)
        self.memory = AsyncSqliteSaver(self.db_conn)
        
        await self.memory.setup()
        
        self.multi_agent = self.graph.compile(checkpointer=self.memory)
    async def close(self):
        """Cleanly releases database resources."""
        if self.db_conn:
            await self.db_conn.close()
    def ingest_documents(self, texts: list[str]):
        """Call this method to actually put data into your vector store."""
        self.vector_store.add_texts(texts)
        self.vector_store.save_local(self.index_path)

    def should_continue_rag(self, state: AgentState) -> str:
        last_message = state['messages'][-1]
        if hasattr(last_message, 'tool_calls') and len(last_message.tool_calls) > 0:
            return "retriever_tool"
        return "END"
    
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
        self.graph.add_node("chat_agent",self.chat_agent_node)
        self.graph.add_node("rag_agent",self.rag_agent_node)
        self.graph.add_node("retriever_tool",self.tool_action)

        self.graph.add_conditional_edges(START,
                                         self.get_semantic_route,
                                         {"chat_agent":"chat_agent",
                                          "rag_agent":"rag_agent"})
        
        self.graph.add_conditional_edges("rag_agent",
                                         self.should_continue_rag,
                                         {"retriever_tool":"retriever_tool",
                                          "END": END })
        
        self.graph.add_edge("retriever_tool","rag_agent")
        self.graph.add_edge("chat_agent",END)