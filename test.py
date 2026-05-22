import pytest
from fastapi.testclient import TestClient
from main import app
import sqlite3
from langgraph.checkpoint.sqlite import SqliteSaver
from langchain_core.messages import HumanMessage, AIMessage

client = TestClient(app=app)

@pytest.fixture(autouse=True)
def setup_db():
    conn = sqlite3.connect("checkpoints.sqlite")
    cursor = SqliteSaver(conn=conn)

    yield

    conn.close()

def test_api_health():
    response = client.get("/health_check")
    assert response.status_code == 200 and response.json()["status"] == "OK"


def test_api_all_chats():
    with client:
        response = client.post("/v1/api/chats/all",json={"thread_ids":["id1_non","id2_non"]})
        assert response.status_code == 200
        assert "chats" in response.json()
        assert len(response.json()["chats"]) == 0