import json
from datetime import datetime, timezone
from pathlib import Path


def capture_empty_start(thread_id, user_data, runs, messages, output):
    if not isinstance(thread_id, str) or not thread_id:
        raise ValueError('Exact thread identity required')
    if not isinstance(runs, list) or not isinstance(messages, list) or runs or messages:
        raise ValueError('A fresh thread must have empty observed run and message lists')
    directory = Path(user_data)
    if directory.is_symlink() or any(parent.is_symlink() for parent in directory.parents):
        raise ValueError('Evidence directory must not resolve through symlinks')
    entries = sorted(str(path.relative_to(directory)) for path in directory.rglob('*')) if directory.exists() else []
    if entries or directory.exists() and not directory.is_dir():
        raise ValueError('Evidence directory is not empty')
    observation = {'thread_id': thread_id, 'observed_at': datetime.now(timezone.utc).isoformat(), 'entries': entries, 'prior_run_ids': [], 'prior_message_ids': []}
    with Path(output).open('x', encoding='utf-8') as handle:
        json.dump(observation, handle, ensure_ascii=False, indent=2)
    return observation
