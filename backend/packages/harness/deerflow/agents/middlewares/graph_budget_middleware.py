from collections.abc import Sequence
from typing import Annotated, Any

from langchain.agents import AgentState
from langchain.agents.middleware import AgentMiddleware, hook_config
from langchain.agents.middleware.types import PrivateStateAttr
from langchain_core.messages import AIMessage, ToolMessage
from langgraph.managed.is_last_step import RemainingStepsManager

from deerflow.subagents.turn_budget import count_invocation_steps, count_turn_steps

GRAPH_BUDGET_YIELD = "deerflow_graph_budget_yield"


class GraphBudgetState(AgentState):
    remaining_steps: Annotated[int, PrivateStateAttr, RemainingStepsManager]


def is_graph_budget_yield(message: Any) -> bool:
    if isinstance(message, dict):
        return message.get("type") == "ai" and (message.get("additional_kwargs") or {}).get(GRAPH_BUDGET_YIELD) is True
    return isinstance(message, AIMessage) and message.additional_kwargs.get(GRAPH_BUDGET_YIELD) is True


class GraphBudgetMiddleware(AgentMiddleware):
    state_schema = GraphBudgetState

    def __init__(self, middlewares: Sequence[AgentMiddleware]):
        self.reserve_steps = count_turn_steps(middlewares) + count_invocation_steps(middlewares) + 3

    @hook_config(can_jump_to=["end"])
    def before_model(self, state, runtime):
        goal = state.get("goal")
        if not isinstance(goal, dict) or goal.get("status") != "active":
            return None
        remaining = state.get("remaining_steps")
        if remaining is None or remaining > self.reserve_steps:
            return None
        context = runtime.context if isinstance(runtime.context, dict) else {}
        if context.get("stop_reason"):
            return None
        answered = set()
        for message in reversed(state.get("messages") or []):
            if isinstance(message, ToolMessage):
                answered.add(message.tool_call_id)
            elif isinstance(message, AIMessage):
                if any(call["id"] not in answered for call in message.tool_calls):
                    return None
                break
        context[GRAPH_BUDGET_YIELD] = True
        return {
            "messages": [
                AIMessage(
                    content=(
                        "The current execution slice has reached its safe graph-step boundary. "
                        "The task is not complete. Completed tool results and unfinished work remain checkpointed; "
                        "this notice is not a final report or new research evidence. "
                        "The active goal must be evaluated before any bounded continuation."
                    ),
                    additional_kwargs={GRAPH_BUDGET_YIELD: True},
                )
            ],
            "jump_to": "end",
        }

    @hook_config(can_jump_to=["end"])
    async def abefore_model(self, state, runtime):
        return self.before_model(state, runtime)
