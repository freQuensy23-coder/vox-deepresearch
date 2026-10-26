import json
from pathlib import Path

import httpx
import pytest

from test_gateway import BODY, answer, service


def test_official_schema_requiredness_and_nullable_parity():
    from jev_gateway.models import ChoiceQuestion, NoulCriteria, NoulQuestion, ScoreQuestion, SystemOneRequest, SystemOneResponse, Usage
    official = json.loads((Path(__file__).parent / "official-openapi.json").read_text())["components"]["schemas"]
    for cls in [ChoiceQuestion, NoulCriteria, NoulQuestion, ScoreQuestion, SystemOneRequest, SystemOneResponse, Usage]:
        schema = cls.model_json_schema()
        assert set(schema.get("required", [])) == set(official[cls.__name__].get("required", []))
    for cls in [ChoiceQuestion, NoulQuestion, ScoreQuestion]:
        actual = cls.model_json_schema()["properties"]["instructions"]
        expected = official[cls.__name__]["properties"]["instructions"]
        assert {v["type"] for v in actual["anyOf"]} == {v["type"] for v in expected["anyOf"]}
    assert ScoreQuestion.model_json_schema()["properties"]["criteria"]["minItems"] == official["ScoreQuestion"]["properties"]["criteria"]["minItems"] == 1


@pytest.mark.asyncio
async def test_preserve_accepted_response_except_model(tmp_path):
    expected = answer(BODY)
    expected["answers"]["q"]["noul"] = 1
    async with service(tmp_path, lambda r: httpx.Response(200, json=expected)) as (client, app):
        result = (await client.post("/v1/systemone", json=BODY)).json()
        assert result == dict(expected, model="jev-1.13.0")
        assert type(result["answers"]["q"]["noul"]) is int


@pytest.mark.asyncio
async def test_openapi_consumer_surface_hides_implementation(tmp_path):
    async with service(tmp_path, lambda r: httpx.Response(500)) as (client, app):
        schema = (await client.get("/openapi.json")).json()
        encoded = json.dumps(schema).lower()
        assert not any(leak in encoded for leak in ["microsoft", "azure", "decision-1", "api-key"])
        assert schema["components"]["securitySchemes"]["HTTPBearer"]["scheme"] == "bearer"
        assert schema["paths"]["/v1/systemone"]["post"]["responses"]["200"]["content"]["application/json"]["schema"] == {"$ref": "#/components/schemas/SystemOneResponse"}
