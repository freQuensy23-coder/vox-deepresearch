"""Run one research prompt on the prod host without the web login.

Uses the embedded DeerFlowClient with the same checkpointer and thread store as
the Gateway, so the thread appears in that user's Web UI. Events stream to
runtime/research-runs/<thread_id>.jsonl.

    cd backend && .venv/bin/python ../local-deploy/research.py USER_ID PROMPT_FILE
"""

import json
import sys
import uuid
from pathlib import Path

from deerflow.client import DeerFlowClient
from deerflow.runtime.checkpointer.provider import get_checkpointer
from deerflow.tui.persistence import build_persistence


def main(user_id, prompt_file):
    prompt = Path(prompt_file).read_text(encoding="utf-8").strip()
    thread_id = str(uuid.uuid4())
    loop, writer = build_persistence()
    writer.user_id = user_id
    writer.ensure_created(thread_id, assistant_id="lead_agent", metadata={"source": "local-deploy/research.py"})
    log = Path(".deer-flow/research-runs") / f"{thread_id}.jsonl"
    log.parent.mkdir(parents=True, exist_ok=True)
    print(json.dumps({"thread_id": thread_id, "log": str(log.resolve())}), flush=True)
    client = DeerFlowClient(checkpointer=get_checkpointer(), model_name="gpt-6.1-sol", subagent_enabled=True, plan_mode=True)
    with log.open("a", encoding="utf-8") as out:
        for event in client.stream(prompt, thread_id=thread_id, user_id=user_id, recursion_limit=1000):
            out.write(json.dumps({"type": event.type, "data": event.data}, ensure_ascii=False, default=str) + "\n")
            out.flush()
            title = event.data.get("title") if event.type == "values" and isinstance(event.data, dict) else None
            if title:
                writer.set_title(thread_id, title)
    loop.close()


if __name__ == "__main__":
    main(*sys.argv[1:3])
