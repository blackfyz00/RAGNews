from fastapi import FastAPI
from pydantic import BaseModel, Field


app = FastAPI(title="RAGNews LLM Service")


class SummarizeRequest(BaseModel):
    title: str | None = None
    text: str = Field(..., min_length=1)
    source_url: str | None = None


class SummarizeResponse(BaseModel):
    summary: str
    topic: str | None = None
    keywords: list[str] = []


class RagSource(BaseModel):
    title: str | None = None
    text: str
    url: str | None = None
    source_name: str | None = None


class RagAnswerRequest(BaseModel):
    question: str = Field(..., min_length=1)
    sources: list[RagSource]


class RagAnswerResponse(BaseModel):
    answer: str
    used_sources: list[str] = []


@app.get("/health")
def health():
    return {
        "status": "ok",
        "service": "llm-service",
        "responsibility": "GigaChat API text generation only"
    }


@app.post("/summarize", response_model=SummarizeResponse)
def summarize_news(payload: SummarizeRequest):
    return {
        "summary": "not_implemented",
        "topic": None,
        "keywords": []
    }


@app.post("/rag-answer", response_model=RagAnswerResponse)
def rag_answer(payload: RagAnswerRequest):
    return {
        "answer": "not_implemented",
        "used_sources": [
            source.url for source in payload.sources if source.url
        ]
    }