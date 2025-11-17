from fastapi import FastAPI, Request, Response
import uvicorn
from opensearch_lambda import lambda_handler

app = FastAPI()

@app.get("/search")
async def search(request: Request, q: str = ""):
    """GET /search?q=xxx のCORS確認テスト"""
    event = {
        "httpMethod": "GET",
        "headers": dict(request.headers),
        "queryStringParameters": {"q": q},
    }
    response = lambda_handler(event, None)
    return Response(
        content=response["body"],
        status_code=response["statusCode"],
        headers=response["headers"],
        media_type="application/json"
    )

@app.options("/search")
async def options_search(request: Request):
    """OPTIONS (preflight) のCORSテスト"""
    event = {
        "httpMethod": "OPTIONS",
        "headers": dict(request.headers),
    }
    response = lambda_handler(event, None)
    return Response(
        content=response["body"],
        status_code=response["statusCode"],
        headers=response["headers"]
    )

if __name__ == "__main__":
    # FastAPI サーバー起動
    uvicorn.run(app, host="127.0.0.1", port=8080)
