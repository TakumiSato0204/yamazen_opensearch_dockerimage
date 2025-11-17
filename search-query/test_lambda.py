# test_lambda.py
import json
from unittest.mock import patch
import opensearch_lambda as m


# 汎用モックレスポンス
class MockResp:
    def __init__(self, status=200, text="", json_obj=None):
        self.status_code = status
        self.text = text
        self._json = json_obj or {}
    def raise_for_status(self):
        if 400 <= self.status_code:
            raise Exception(f"HTTP {self.status_code}")
    def json(self):
        return self._json

def make_event(method="GET", origin="https://dgcoiodttmmm.com", q="テスト"):
    return {
        "httpMethod": method,
        "headers": {"origin": origin},
        "queryStringParameters": {"q": q},
        "requestContext": {"http": {"method": method}},  # 互換
    }

@patch("opensearch_lambda.requests.get")
def test_preflight_returns_204_with_fixed_origin(mock_get):
    # 1発目: Allowed-Origin (SSM)
    mock_get.side_effect = [
        MockResp(json_obj={"Parameter": {"Value": "https://stg.genbato.jp"}}),
    ]
    resp = m.lambda_handler(make_event(method="OPTIONS"), None)
    assert resp["statusCode"] == 204
    assert resp["headers"]["Access-Control-Allow-Origin"] == "https://stg.genbato.jp"
    # ワイルドカードでないこと
    assert resp["headers"]["Access-Control-Allow-Origin"] != "*"

@patch("opensearch_lambda.requests.get")
def test_get_returns_200_and_calls_ssm_then_os(mock_get):
    # 1) Allowed-Origin (SSM) → 2) OS host (SSM) → 3) OpenSearch
    mock_get.side_effect = [
        MockResp(json_obj={"Parameter": {"Value": "https://stg.genbato.jp"}}),            # allowed origin
        MockResp(json_obj={"Parameter": {"Value": "https://os.example.com"}}),            # host
        MockResp(text=json.dumps({"hits":{"total":{"value":1},"hits":[{"_id":"1"}]}})),   # OS search
    ]
    resp = m.lambda_handler(make_event(method="GET"), None)
    assert resp["statusCode"] == 200
    assert resp["headers"]["Access-Control-Allow-Origin"] == "https://stg.genbato.jp"

    calls = mock_get.call_args_list
    # 1発目: Parameter Store（allowed-origin）
    assert "localhost:2773/systemsmanager/parameters/get" in calls[0].args[0]
    # 2発目: Parameter Store（host）
    assert "localhost:2773/systemsmanager/parameters/get" in calls[1].args[0]
    # 3発目: OpenSearch 検索URL
    assert calls[2].args[0].startswith("https://os.example.com/genbato-index/_search")

@patch("opensearch_lambda.requests.get")
def test_ssm_failure_returns_500_without_wildcard(mock_get):
    # Allowed-Origin の取得に失敗させる
    def boom(*a, **kw):
        raise Exception("SSM down")
    mock_get.side_effect = boom
    resp = m.lambda_handler(make_event(method="GET"), None)
    assert resp["statusCode"] == 500
    # ヘッダが無い（または付いていても * ではない）ことを確認
    headers = resp.get("headers", {})
    assert "Access-Control-Allow-Origin" not in headers or headers["Access-Control-Allow-Origin"] != "*"
