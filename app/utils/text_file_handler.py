from langchain_community.document_loaders import PyPDFLoader, TextLoader, Docx2txtLoader
from langchain_text_splitters import RecursiveCharacterTextSplitter
from ai_assistant import AiAssistant
from app.utils.logger import logger
import os

def pdf_doc_ingestion(path:str,ext:str,ai_assistant:AiAssistant):
    try:
        if ext==".pdf":
            loader = PyPDFLoader(file_path=path)
        else:
            loader = TextLoader(file_path=path)
        
        doc = loader.load()

        text_splitter = RecursiveCharacterTextSplitter(chunk_size=1000,chunk_overlap=200,separators=["\n","\n\n"," ",""])
        split_docs = text_splitter.split_documents(documents=doc)
        
        text_chunk = [d.page_content for d in split_docs]
        if text_chunk:
            ai_assistant.ingest_documents(texts=text_chunk)
        else:
            logger.info("No viable chunk found in document")

    except Exception as e:
        logger.info(f"Error in file ingestion: {e}")
    finally:
        if os.path.exists(path=path):
            os.remove(path)
            logger.info("file is deleted from temp storage")