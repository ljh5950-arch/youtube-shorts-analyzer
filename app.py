from fastapi import FastAPI

app = FastAPI(title="YouTube Shorts Analyzer (MVP)")

@app.get("/health")
def health():
    return {"ok": True}
