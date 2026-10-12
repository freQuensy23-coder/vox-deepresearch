from datetime import datetime, timezone


def observe(identity, run, goal_response, state):
    if not isinstance(identity, dict) or set(identity) != {'thread_id', 'run_id'} or any(not isinstance(value, str) or not value for value in identity.values()):
        raise ValueError('Exact thread/run identity required')
    if not isinstance(run, dict) or any(run.get(key) != value for key, value in identity.items()):
        raise ValueError('Run identity mismatch')
    goal = goal_response.get('goal')
    verdict = run.get('goal_verdict')
    status = 'unverified'
    origin = 'no_satisfaction_evidence'
    if isinstance(goal, dict) and goal.get('status') == 'active':
        status, origin = 'active', 'thread.goal'
    elif run.get('status') == 'success' and isinstance(verdict, dict) and verdict.get('satisfied') is True:
        status, origin = 'satisfied', 'run.goal_verdict'
    candidate = None
    messages = state.get('values', {}).get('messages', [])
    if run.get('status') == 'success' and messages:
        message = messages[-1]
        if message.get('type') in ('ai', 'assistant') and not message.get('tool_calls'):
            content = message.get('content')
            if isinstance(content, list):
                content = '\n'.join(block.get('text', '') for block in content if isinstance(block, dict) and block.get('type') == 'text' and isinstance(block.get('text'), str))
            if isinstance(content, str) and content.strip():
                candidate = {'message_id': message.get('id'), 'text': content, 'evidence_origin': 'last_checkpoint_message', 'semantic_finality_unverified': True}
    return {'identity': dict(identity), 'observed_at': datetime.now(timezone.utc).isoformat(), 'run_status': run.get('status'), 'goal': {**identity, 'status': status, 'evidence_origin': origin}, 'final_candidate': candidate, 'semantic_review_required': True}
