import json
import time

import httpx
from typesafe_sdk import Choice, Noul, Score, TypeSafeClient


from jev_gateway.app import Settings, config_section

ROOT = f"http://127.0.0.1:{config_section()['port']}"
key = Settings.from_config().api_key
headers = {"Authorization": "Bearer " + key}
body = {
    "model": "jev-latest",
    "state": "The production API is unavailable and every customer request fails.",
    "questions": {
        "urgent": {"type": "noul", "instructions": "Does this require immediate attention?"},
        "team": {"type": "choice", "instructions": "Which team should handle this?", "criteria": {"engineering": "Service outages", "billing": "Invoices"}},
        "severity": {"type": "score", "instructions": "Rate impact.", "criteria": ["Minor", "Moderate", "Severe"]},
    },
}


def clean(data):
    text = json.dumps(data).lower()
    assert "opencode" not in text and "jev-1.13-free" not in text
    assert key not in text


def check_result(result):
    clean(result)
    assert result["model"] == "jev-1.13.0"
    assert set(result["answers"]) == set(body["questions"])
    assert result["answers"]["urgent"]["noul"] > 0.8
    assert result["answers"]["team"]["choice"] == "engineering"
    assert result["answers"]["severity"]["score"] > 1.5
    assert result["usage"]["input_tokens"] > 0


with httpx.Client(base_url=ROOT, timeout=180, trust_env=False) as http:
    r = http.get("/healthz")
    assert r.status_code == 200 and r.json()["status"] == "ok"
    assert http.get("/v1/models").status_code == 401
    r = http.get("/v1/models", headers=headers)
    r.raise_for_status()
    clean(r.json())
    assert {m["name"] for m in r.json()["models"]} == {"jev-latest", "jev-preview", "jev-1.13.0"}
    assert http.post("/v1/systemone", headers=headers, json={"state": "invalid"}).status_code == 422
    r = http.post("/v1/systemone", headers=headers, json=body)
    r.raise_for_status()
    single = r.json()
    check_result(single)
    with TypeSafeClient(api_key=key, base_url=ROOT, model="jev-latest") as sdk:
        model_names = [m.name for m in sdk.models.list().models]
        response = sdk.system_one(body["state"], {"urgent": Noul(instructions="Does this require immediate attention?"), "team": Choice(instructions="Which team should handle this?", criteria={"engineering": "Service outages", "billing": "Invoices"}), "severity": Score(instructions="Rate impact.", criteria=["Minor", "Moderate", "Severe"])})
        assert response.model == "jev-1.13.0"
        assert response.nouls["urgent"].noul > 0.8
        assert response.choices["team"].choice == "engineering"
        assert response.scores["severity"].score > 1.5
    submit = http.post("/v1/batches", headers=headers, json={"requests": [{"custom_id": str(i), "body": body} for i in range(3)], "policy": {"concurrency": 2}})
    assert submit.status_code == 202, submit.text
    batch_id = submit.json()["id"]
    until = time.monotonic() + 180
    while True:
        meta = http.get(f"/v1/batches/{batch_id}", headers=headers).json()
        if meta["status"] == "completed":
            break
        assert time.monotonic() < until, meta
        time.sleep(0.5)
    assert meta["total"] == 3 and meta["completed"] == 3 and meta["failed"] == 0
    results = []
    offset = 0
    while True:
        page = http.get(f"/v1/batches/{batch_id}/results", headers=headers, params={"offset": offset, "limit": 2}).json()
        assert page["total"] == 3
        results.extend(page["results"])
        if not page["has_more"]:
            break
        offset += len(page["results"])
    assert len(results) == 3 and [r["custom_id"] for r in results] == ["0", "1", "2"]
    for item in results:
        assert item["status_code"] == 200
        check_result(item["response"])
    evidence = {"health": "ok", "auth_rejection": 401, "invalid_input": 422, "single": single, "sdk_models": model_names, "sdk": {"model": response.model, "urgent": response.nouls["urgent"].noul}, "batch": meta, "results": results}
    print(json.dumps(evidence, ensure_ascii=False))
