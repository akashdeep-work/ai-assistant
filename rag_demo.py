from langgraph.graph import StateGraph, START, END
from langgraph.checkpoint.memory import MemorySaver
from langchain_ollama.embeddings import OllamaEmbeddings
from langchain_ollama.chat_models import ChatOllama
from langchain_community.vectorstores import FAISS
from langchain_core.tools.retriever import create_retriever_tool
from typing import Annotated, TypedDict, Sequence
from langchain_core.messages import BaseMessage, SystemMessage, ToolMessage
from operator import add as add_messages
import faiss
from langchain_community.docstore.in_memory import InMemoryDocstore

class AgentState(TypedDict):
    message: Annotated[Sequence[BaseMessage], add_messages]

system_prompt = "You are a helpful AI assistant with access to a knowledge base."

class AiAssistant:
    def __init__(self):
        # 1. Use a dedicated embedding model (ensure you run `ollama pull nomic-embed-text`)
        self.embedding = OllamaEmbeddings(model='nomic-embed-text')
        
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
        self.llm = ChatOllama(model="llama3.1", temperature=0)
        self.llm_with_tools = self.llm.bind_tools(self.tools)
        
        # 5. Build Graph
        self.graph = StateGraph(AgentState)
        self._create_graph()

    def ingest_documents(self, texts: list[str]):
        """Call this method to actually put data into your vector store."""
        self.vector_store.add_texts(texts)

    def should_continue(self, state: AgentState) -> str:
        last_message = state['message'][-1]
        if hasattr(last_message, 'tool_calls') and len(last_message.tool_calls) > 0:
            return "retriever_agent"
        return END
    
    def call_llm(self, state: AgentState) -> dict:
        messages = list(state['message'])
        # Only inject system prompt if it's the first message to save tokens
        if not any(isinstance(m, SystemMessage) for m in messages):
            messages = [SystemMessage(content=system_prompt)] + messages
            
        response = self.llm_with_tools.invoke(messages)
        return {"message": [response]}
    
    def tool_action(self, state: AgentState) -> dict:
        last_message = state['message'][-1]
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

        return {'message': results}
    
    def _create_graph(self):
        self.graph.add_node("llm", self.call_llm)
        self.graph.add_node("retriever_agent", self.tool_action)
        
        self.graph.add_conditional_edges("llm", self.should_continue)
        self.graph.add_edge("retriever_agent", "llm")
        self.graph.set_entry_point("llm")
        
        # Add checkpointer for API memory handling
        memory = MemorySaver()
        self.rag_agent = self.graph.compile(checkpointer=memory)