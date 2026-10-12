import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from langchain.agents import create_agent
from langchain.agents.middleware import AgentMiddleware
from langchain_core.language_models import BaseChatModel
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage
from langchain_core.outputs import ChatGeneration, ChatResult
from langchain_core.tools import StructuredTool
from langgraph.checkpoint.memory import InMemorySaver

from deerflow.agents.lead_agent.agent import build_middlewares
from deerflow.agents.middlewares.graph_budget_middleware import GraphBudgetMiddleware
from deerflow.agents.middlewares.todo_middleware import TodoMiddleware
from deerflow.agents.thread_state import ThreadState
from deerflow.config.app_config import AppConfig
from deerflow.config.memory_config import MemoryConfig
from deerflow.config.paths import Paths
from deerflow.config.run_ownership_config import RunOwnershipConfig
from deerflow.config.sandbox_config import SandboxConfig
from deerflow.config.summarization_config import SummarizationConfig
from deerflow.runtime.goal import build_goal_state, read_thread_goal, write_thread_goal
from deerflow.runtime.runs import worker
from deerflow.runtime.runs.manager import RunManager
from deerflow.runtime.runs.schemas import RunStatus
from deerflow.runtime.runs.store.memory import MemoryRunStore


class ScriptedModel(BaseChatModel):
    finish_at: int = 70
    calls: int = 0
    visible_progress: bool = False

    @property
    def _llm_type(self):
        return 'offline-graph-budget'

    def bind_tools(self, tools, **kwargs):
        return self

    def _generate(self, messages, stop=None, run_manager=None, **kwargs):
        self.calls += 1
        if self.calls < self.finish_at:
            message = AIMessage(content=f'Completed {self.calls - 1} synthetic batches.' if self.visible_progress else '', tool_calls=[{'name': 'ping', 'args': {'index': self.calls}, 'id': f'ping-{self.calls}'}])
        elif self.calls == self.finish_at:
            message = AIMessage(content='', tool_calls=[{'name': 'write_todos', 'args': {'todos': [{'content': 'synthetic work', 'status': 'completed'}]}, 'id': 'todos-done'}])
        else:
            message = AIMessage(content='The synthetic work is complete.')
        return ChatResult(generations=[ChatGeneration(message=message)])


def assembled_gate():
    config = AppConfig(sandbox=SandboxConfig(use='deerflow.sandbox.local:LocalSandboxProvider'), memory=MemoryConfig(enabled=False), summarization=SummarizationConfig(enabled=False))
    chain = build_middlewares({'configurable': {'is_plan_mode': True}}, model_name=None, app_config=config, memory_enabled=False, available_skills=set())
    assert isinstance(chain[0], GraphBudgetMiddleware)
    return [m for m in chain if type(m).__name__ == 'GraphBudgetMiddleware']


def synthetic_chain():
    chain = [type(f'Before{i}', (AgentMiddleware,), {'before_model': lambda self, state, runtime: None})() for i in range(3)]
    chain += [TodoMiddleware()]
    chain += [type(f'After{i}', (AgentMiddleware,), {'after_model': lambda self, state, runtime: None})() for i in range(9)]
    gate = assembled_gate()[0]
    chain.insert(0, type(gate)(chain))
    return chain


async def run_fixture(tmp_path, monkeypatch, *, finish_at=70, max_continuations=8, cancel=False, false_satisfied=False, visible_progress=False, checkpoint_mode='full'):
    monkeypatch.setenv('LANGCHAIN_TRACING_V2', 'false')
    monkeypatch.setenv('LANGSMITH_TRACING', 'false')
    monkeypatch.setattr('deerflow.workspace_changes.recorder.get_paths', lambda: Paths(base_dir=tmp_path))
    checkpointer = InMemorySaver()
    store = MemoryRunStore()
    manager = RunManager(store=store, run_ownership_config=RunOwnershipConfig(heartbeat_enabled=False))
    record = await manager.create('graph-budget-thread')
    bridge = SimpleNamespace(publish=AsyncMock(), publish_end=AsyncMock(), cleanup=AsyncMock())
    model = ScriptedModel(finish_at=finish_at, visible_progress=visible_progress)
    executed = []

    def ping(index: int):
        executed.append(index)
        return 'synthetic tool result'

    graph = create_agent(model=model, tools=[StructuredTool.from_function(ping, name='ping', description='Synthetic tool')], middleware=synthetic_chain(), state_schema=ThreadState, checkpointer=checkpointer)
    await write_thread_goal(checkpointer, record.thread_id, build_goal_state('Complete synthetic work', max_continuations=max_continuations), create_if_missing=True)
    evaluations = []

    class Evaluator:
        async def ainvoke(self, messages, config=None):
            snapshot = await graph.aget_state({'configurable': {'thread_id': record.thread_id}})
            receipt = snapshot.values['messages'][-1]
            yielded = bool(receipt.additional_kwargs.get('deerflow_graph_budget_yield'))
            evaluations.append({'yielded': yielded, 'model_calls': model.calls, 'step': snapshot.metadata['step'], 'todos': snapshot.values.get('todos'), 'next': snapshot.next})
            if cancel:
                record.abort_event.set()
            satisfied = false_satisfied or not yielded
            verdict = {
                'satisfied': satisfied,
                'blocker': 'none' if satisfied else 'goal_not_met_yet',
                'reason': 'Synthetic verdict',
                'evidence_summary': 'Incomplete saved work' if yielded else 'Synthetic work complete',
                'relied_on_assumption': False,
            }
            return AIMessage(content=json.dumps(verdict))

    monkeypatch.setattr(worker, 'create_goal_evaluator_model', lambda **kwargs: Evaluator())
    def factory(config):
        return graph

    await worker.run_agent(
        bridge,
        manager,
        record,
        ctx=worker.RunContext(checkpointer=checkpointer, checkpoint_channel_mode=checkpoint_mode),
        agent_factory=factory,
        graph_input={'messages': [HumanMessage(content='Complete synthetic work')], 'todos': [{'content': 'synthetic work', 'status': 'in_progress'}]},
        config={'recursion_limit': 1000, 'configurable': {'thread_id': record.thread_id}},
        stream_modes=['values'],
    )
    return SimpleNamespace(record=record, durable=await store.get(record.run_id), goal=await read_thread_goal(checkpointer, record.thread_id), evaluations=evaluations, model=model, executed=executed, graph=graph)


def test_lead_assembly_installs_budget_guard_first():
    assert len(assembled_gate()) == 1


@pytest.mark.asyncio
async def test_real_worker_yields_and_continues_same_run_before_recursion_limit(tmp_path, monkeypatch):
    result = await run_fixture(tmp_path, monkeypatch)
    assert result.record.status == RunStatus.success
    assert result.model.calls == 71
    assert result.executed == list(range(1, 70))
    assert len(result.evaluations) == 2
    assert result.evaluations[0]['yielded'] is True
    assert result.evaluations[0]['next'] == ()
    assert result.evaluations[0]['step'] < 1000
    assert result.evaluations[0]['todos'][0]['status'] == 'in_progress'
    assert result.goal is None
    assert result.durable['goal_verdict']['continuations'] == 1
    assert result.durable['goal_verdict']['satisfied'] is True
    assert result.durable['run_id'] == result.record.run_id


@pytest.mark.asyncio
@pytest.mark.parametrize('false_satisfied', [False, True])
async def test_budget_yield_never_becomes_success_when_continuations_are_exhausted(tmp_path, monkeypatch, false_satisfied):
    result = await run_fixture(tmp_path, monkeypatch, max_continuations=0, false_satisfied=false_satisfied)
    assert result.record.status == RunStatus.error
    assert result.durable['stop_reason'] == 'graph_budget_capped'
    assert result.goal['status'] == 'active'
    assert result.goal['continuation_count'] == 0
    assert result.goal['last_evaluation']['satisfied'] is False
    assert result.goal['last_evaluation']['stand_down_reason'] == 'max_continuations_reached'
    assert len(result.evaluations) == 1


@pytest.mark.asyncio
async def test_budget_yield_preserves_native_no_progress_limit(tmp_path, monkeypatch):
    result = await run_fixture(tmp_path, monkeypatch, finish_at=10000)
    assert result.record.status == RunStatus.error
    assert result.goal['continuation_count'] == 2
    assert result.goal['no_progress_count'] == 2
    assert result.goal['last_evaluation']['stand_down_reason'] == 'no_progress_detected'
    assert len(result.evaluations) == 3
    assert len(result.executed) == len(set(result.executed))


@pytest.mark.asyncio
async def test_cancel_after_yield_does_not_start_another_slice(tmp_path, monkeypatch):
    result = await run_fixture(tmp_path, monkeypatch, cancel=True)
    assert result.record.status == RunStatus.interrupted
    assert len(result.evaluations) == 1
    assert result.goal['continuation_count'] == 0


@pytest.mark.asyncio
async def test_budget_yield_preserves_native_eight_continuation_ceiling(tmp_path, monkeypatch):
    result = await run_fixture(tmp_path, monkeypatch, finish_at=10000, visible_progress=True)
    assert result.record.status == RunStatus.error
    assert result.goal['continuation_count'] == 8
    assert result.goal['no_progress_count'] == 0
    assert result.goal['last_evaluation']['stand_down_reason'] == 'max_continuations_reached'
    assert len(result.evaluations) == 9
    assert len(result.executed) == len(set(result.executed))


@pytest.mark.parametrize('stop_reason', ['token_capped', 'loop_capped', 'safety_capped', 'subagent_limit_capped'])
def test_budget_guard_does_not_override_an_existing_guard(stop_reason):
    gate = GraphBudgetMiddleware([])
    state = {'remaining_steps': 1, 'goal': {'status': 'active'}, 'messages': []}
    assert gate.before_model(state, SimpleNamespace(context={'stop_reason': stop_reason})) is None


def test_budget_guard_does_not_yield_an_unanswered_tool_call():
    gate = GraphBudgetMiddleware([])
    call = AIMessage(content='', tool_calls=[{'name': 'ping', 'args': {}, 'id': 'pending'}])
    state = {'remaining_steps': 1, 'goal': {'status': 'active'}, 'messages': [call]}
    runtime = SimpleNamespace(context={})
    assert gate.before_model(state, runtime) is None
    state['messages'].append(ToolMessage(content='real synthetic receipt', tool_call_id='pending'))
    yielded = gate.before_model(state, runtime)
    assert yielded['jump_to'] == 'end'
    assert not yielded['messages'][0].tool_calls


@pytest.mark.parametrize('goal', [None, {'status': 'satisfied'}, {'status': 'cleared'}])
def test_budget_guard_requires_an_active_goal(goal):
    gate = GraphBudgetMiddleware([])
    assert gate.before_model({'goal': goal, 'remaining_steps': 1}, SimpleNamespace(context={})) is None
