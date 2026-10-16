import math
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


ALIASES = ("jev-latest", "jev-preview", "jev-1.13.0")
Structured = str | dict[str, Any] | list[Any]


def safe_json(value):
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError("Non-finite value")
    if isinstance(value, str):
        value.encode("utf-8")
    if isinstance(value, dict):
        for key, item in value.items():
            safe_json(key)
            safe_json(item)
    if isinstance(value, list):
        for item in value:
            safe_json(item)
    return value


class RequestModel(BaseModel):
    model_config = ConfigDict(extra="allow", strict=True)

    @model_validator(mode="before")
    @classmethod
    def valid_json(cls, value):
        return safe_json(value)


class NoulCriteria(RequestModel):
    true: Structured | None = None
    false: Structured | None = None


class NoulQuestion(RequestModel):
    type: Literal["noul"]
    instructions: Structured | None = None
    criteria: NoulCriteria | None = None


class ChoiceQuestion(RequestModel):
    type: Literal["choice"]
    instructions: Structured | None = None
    criteria: dict[str, Structured | None]


class ScoreQuestion(RequestModel):
    type: Literal["score"]
    instructions: Structured | None = None
    criteria: list[Structured] = Field(min_length=1)


Question = Annotated[NoulQuestion | ChoiceQuestion | ScoreQuestion, Field(discriminator="type")]


class SystemOneRequest(RequestModel):
    state: Structured
    model: str
    questions: dict[str, Question] = Field(min_length=1)

    @model_validator(mode="after")
    def supported_model(self):
        if self.model not in ALIASES:
            raise ValueError("Unsupported Jev model")
        return self


class BatchPolicy(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, allow_inf_nan=False)
    concurrency: int = Field(default=64, ge=1, le=128)


class BatchItem(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    custom_id: str = Field(min_length=1, max_length=256)
    body: SystemOneRequest


class BatchRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    requests: list[BatchItem] = Field(min_length=1)
    policy: BatchPolicy = Field(default_factory=BatchPolicy)

    @model_validator(mode="after")
    def unique_ids(self):
        if len({item.custom_id for item in self.requests}) != len(self.requests):
            raise ValueError("Duplicate custom_id")
        return self


class ModelMetadata(BaseModel):
    name: str
    description: str
    release_date: str


class ModelMetadataList(BaseModel):
    models: list[ModelMetadata]


class ResponseModel(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, allow_inf_nan=False)


Probability = Annotated[float, Field(ge=0, le=1)]


class NoulAnswer(ResponseModel):
    type: Literal["noul"]
    noul: Probability


class ChoiceAnswer(ResponseModel):
    type: Literal["choice"]
    choice: str
    confidence: Probability
    probabilities: dict[str, Probability]


class ScoreAnswer(ResponseModel):
    type: Literal["score"]
    score: float
    confidence: Probability
    legend: dict[str, Structured]
    probabilities: dict[str, Probability]


Answer = Annotated[NoulAnswer | ChoiceAnswer | ScoreAnswer, Field(discriminator="type")]


class Usage(ResponseModel):
    input_tokens: int = Field(ge=0)
    output_tokens: int = Field(ge=0)


class SystemOneResponse(ResponseModel):
    model: str
    answers: dict[str, Answer] = Field(min_length=1)
    usage: Usage


def validate_response(raw, request):
    safe_json(raw)
    response = SystemOneResponse.model_validate(raw)
    if response.answers.keys() != request.questions.keys():
        raise ValueError("Answer keys mismatch")
    for key, question in request.questions.items():
        result = response.answers[key]
        if result.type != question.type:
            raise ValueError("Answer type mismatch")
        if result.type == "noul":
            continue
        probabilities = result.probabilities
        if not math.isclose(sum(probabilities.values()), 1, rel_tol=0, abs_tol=0.02):
            raise ValueError("Invalid distribution")
        if result.type == "choice":
            if probabilities.keys() != question.criteria.keys() or result.choice not in question.criteria:
                raise ValueError("Choice keys mismatch")
            if probabilities[result.choice] < max(probabilities.values()):
                raise ValueError("Choice not maximum")
        else:
            expected = {str(i): item for i, item in enumerate(question.criteria)}
            if result.legend != expected or probabilities.keys() != expected.keys():
                raise ValueError("Score keys mismatch")
            if not 0 <= result.score <= len(question.criteria) - 1:
                raise ValueError("Score out of range")
    return dict(raw, model="jev-1.13.0")
