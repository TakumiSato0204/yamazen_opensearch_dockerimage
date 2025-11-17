import os
import json
import boto3
import requests
from requests_aws4auth import AWS4Auth

# ===== 基本設定 =====
region = "ap-northeast-1"
service = "aoss"

# Lambda 実行ロールの認証情報（Lambda想定）
_credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(
    _credentials.access_key,
    _credentials.secret_key,
    region,
    service,
    session_token=_credentials.token,
)

# ===== Parameter Store Keys =====
SRCH_HOST_STORE_KEY = "%2Flambda%2Fopensearch-host"             # OpenSearch 検索エンドポイント
ALLOWED_ORIGIN_STORE_KEY = "%2Flambda%2Fopensearch-allowed-origin"  # 許可するOrigin（環境別）

def _get_allowed_origin() -> str | None:
    """Parameter Storeから許可Originを取得（失敗時はNoneを返す）"""
    try:
        return _get_store_param(ALLOWED_ORIGIN_STORE_KEY)
    except Exception as e:
        print(f"❌ Failed to load allowed origin from Parameter Store: {e}")
        return None


# ===== CORS ヘッダー =====
def _cors_headers(_origin: str | None) -> dict:
    allowed_origin = _get_allowed_origin()
    if not allowed_origin:
        # 許可リストが取得できない
        raise RuntimeError("Allowed origin could not be loaded from Parameter Store.")

    allowed_headers = "content-type"
    allowed_methods = "GET,OPTIONS"

    return {
        "Access-Control-Allow-Origin": allowed_origin,
        "Access-Control-Allow-Methods": allowed_methods,
        "Access-Control-Allow-Headers": allowed_headers,
        "Vary": "Origin, Access-Control-Request-Method, Access-Control-Request-Headers",
    }


# ===== Lambda Handler =====
def lambda_handler(event, context):
    try:
        method = event.get("httpMethod") or event.get("requestContext", {}).get("http", {}).get("method")
        origin = (event.get("headers") or {}).get("origin") or (event.get("headers") or {}).get("Origin")
        headers = _cors_headers(origin)
    except Exception as e:
        print(f"❌ CORS configuration error: {e}")
        return {"statusCode": 500, "body": "Server CORS misconfiguration"}
  
    _headers_json = {"Content-Type": "application/json"}

    # --- OPTIONS: preflight ---
    if method == "OPTIONS":
        return {"statusCode": 204, "headers": headers, "body": ""}

    # --- 検索クエリを実行 ---
    qsp = event.get("queryStringParameters") or {}
    query = {
        "size": 300,
        "query": {
            "multi_match": {
                "query": qsp.get("q", ""),
                "type": "phrase_prefix",
                "fields": ["title", "content", "categoryL", "categoryM", "categoryS"],
            }
        },
    }

    try:
        request_url = _get_search_url()
        r = requests.get(request_url, auth=awsauth, headers=_headers_json, data=json.dumps(query))
        r.raise_for_status()
        return {"statusCode": 200, "headers": headers, "body": r.text, "isBase64Encoded": False}
    except Exception as e:
        print(f"❌ Failed to query OpenSearch: {e}")
        return {"statusCode": 500, "headers": headers, "body": "Internal Server Error"}


# ===== ユーティリティ =====
def _get_search_url() -> str:
    """OpenSearch 検索エンドポイント URL を生成"""
    host = _get_store_param(SRCH_HOST_STORE_KEY)
    index = "genbato-index"
    return f"{host}/{index}/_search"


def _get_store_param(param_store_key: str, retry: int = 0) -> str:
    """
    Lambda Extensions (localhost:2773) 経由で Parameter Store を取得。
    失敗時は最大2回までリトライ。
    """
    try:
        request_path = (
            f"http://localhost:2773/systemsmanager/parameters/get/?name={param_store_key}"
        )
        request_header = {"X-Aws-Parameters-Secrets-Token": _credentials.token}
        resp = requests.get(request_path, headers=request_header)
        resp.raise_for_status()
        data = resp.json()
        return data["Parameter"]["Value"]
    except Exception as e:
        if retry < 2:
            return _get_store_param(param_store_key, retry + 1)
        print(f"Failed to get parameter: {param_store_key}")
        raise e
