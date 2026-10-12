# Массовое назначение групп из файла

Механизм — `batch_task(items_file=...)`: основной агент пишет JSON-массив
поручений на общем диске, а инструмент читает его из sandbox текущего треда.
Ключи API и запуск новых Gateway-запросов из sandbox не нужны. Все элементы
выполняются native-подагентами того же треда и видят общий `registry.json`.

Пример подготовки очередной волны. `question`, `period` и критерии заполни
по текущему запросу; не заменяй их общей фразой «сделай исследование».
Не включай все группы автоматически: в `selected_ids` основной агент явно
выбирает следующую волну по прочитанным данным.

```python
import json
import sys
from pathlib import Path

sys.path.insert(0, "/mnt/skills/public/telegram-research/scripts")
from research_registry import Registry, RegistryError

root = Path("/mnt/user-data/workspace/telegram-research/current")
registry_path = root / "registry.json"
registry = Registry(registry_path)
# task.md уже содержит исходный вопрос, период и критерии релевантности.
selected_ids = [...]  # реально найденные и выбранные ID
pending = {row["id"]: row for row in registry.snapshot()["groups"]["todo"]}
items = []
wave = "wave-001"
for gid in selected_ids:
    group = pending.get(gid)
    if group is None or "assignment" in group:
        continue
    owner = f"{wave}:{gid}"
    folder = root / "groups" / str(gid)
    try:
        registry.claim_group(gid, owner=owner, folder=str(folder))
    except RegistryError:
        continue  # После чтения реестра назначение уже могло измениться.
    folder.mkdir(parents=True, exist_ok=True)
    prompt = (
        "Прочитай /mnt/skills/public/telegram-research/SKILL.md и "
        "/mnt/skills/public/telegram-research/references/group-research.md. "
        f"Исходная задача и критерии: {root / 'task.md'}. "
        f"Твоё единственное назначение — группа {gid}; owner={owner}. "
        f"Общий реестр: {registry_path}. Твоя папка: {folder}. "
        "Прочитай содержание группы. Сохрани facts.md до 500 слов со ссылками, "
        "findings.json с внешними ссылками и публичными деловыми контактами, "
        "todo.md. Добавляй новые слова и проверенные тематические группы "
        "в общий реестр; проверь китов по гайду. Заверши или сохрани partial "
        "через реестр. Верни пути и статус, не пересказ всего чата."
    )
    items.append({
        "key": str(gid),
        "prompt": prompt,
        "acceptance_criteria": [
            f"file:{folder / 'facts.md'} non-empty",
            f"file:{folder / 'findings.json'} json-valid",
        ],
    })
# Уникальный файл; не перезаписывай manifest уже отправленной волны.
with (root / f"{wave}.json").open("x", encoding="utf-8") as stream:
    json.dump(items, stream, ensure_ascii=False, indent=2)
```

Пример вызова инструмента после подготовки (это аргументы `batch_task`, не
shell-команда и не самостоятельный Python API):

```json
{
  "title": "Углублённое изучение выбранных групп",
  "subagent_type": "general-purpose",
  "items_file": "/mnt/user-data/workspace/telegram-research/current/wave-001.json",
  "max_running_items": 32,
  "max_live_items": 1000
}
```

Передаётся ровно один из `items` и `items_file`. Файл — полный UTF-8 JSON-массив
существующих элементов `{key,prompt,acceptance_criteria?}`, не более 16 MiB;
пустой массив и повторные ключи отвергаются до запуска. Большие задачи дели
на выбранные агентом волны, сохраняя совокупный бюджет текущего запуска.
Не используй новый run/thread только ради обхода исчерпанного лимита.

Сохрани возвращённый batch ID рядом с manifest в журнале. До ответа инструмента
не считай назначения успешно запущенными. Если результат подачи неизвестен,
проверь доступные статусы и журнал вызова; не отправляй ту же работу с новым
ID вслепую. При подтверждённом отказе освободи только её назначения, сохранив
ошибку. Если подготовка manifest прервалась, сверь claims с сохранёнными волнами,
освободи подтверждённо незапущенные.

`batch_status(batch_id=...)` показывает прогресс компактно. Пока фоновые
подагенты работают, продолжай поиск и чтение реестра. Для результата используй
файлы группы; `read_batch_result` позволяет прочитать ответ исполнителя по
позиции без загрузки всей пачки. Наличие файлов проверяется отдельно от их
содержательной корректности. `cancel_batch` останавливает ненужную волну;
после подтверждения отмены обработай незавершённые назначения.

Обычный `task` и durable batches имеют разные счётчики. В этом исследовании
используй batch для всей глубины: `max_items_per_run: 1000` учитывает суммарные
назначения всех волн данного запуска, включая завершённые и ошибочные.
Физические 32 слота Gateway общие. Отмена не освобождает потраченный бюджет
назначений. У подагентов нет общего дедлайна при `timeout_seconds:0`;
пользовательская отмена и ограничения отдельных HTTP/инструментальных операций
сохраняются. Достаточность исследования определяет основной агент.
