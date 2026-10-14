# Подагент: одна группа

## Шаблон задания

```
Исходный вопрос пользователя: ...
Период: с SINCE.
Группа: GROUP_ID (название, ссылка).
Выгрузка: /mnt/user-data/workspace/research/vox/groups/GROUP_ID/messages.json
Карточка: /mnt/user-data/workspace/research/vox/groups/GROUP_ID/card.json
Реестр слов (только читать): /mnt/user-data/workspace/research/keywords.json
Результат: /mnt/user-data/workspace/research/groups/group_ALIAS_ИЛИ_ID.json
Инструкция: /mnt/skills/public/telegram-research/references/group-agent.md
```

## Что делать

Выгрузка — все сообщения группы за период. Читай её скриптами (`jq`, python):
большие файлы не выводи в контекст целиком. Активных авторов посчитает
`python3 /mnt/skills/public/telegram-research/scripts/vox.py authors ВЫГРУЗКА`.

0. **По теме ли группа.** Если в группе нет обсуждений по теме вопроса — запиши
   `on_topic: false` и короткую причину с 1–3 примерами `message_url`. Дальше не иди.
1. **summary** — о чём группа и что в ней говорят по теме вопроса: позиции,
   условия, цифры, проблемы. Каждое утверждение с `message_url`.
2. **links** — внешние источники (сайты, другие каналы и чаты) и **contacts** —
   контакты компаний, агентств, менеджеров, работодателей. С контекстом и
   `message_url`. Личные телефоны частных лиц не собирай.
3. **whales** — пользователи, которые активно и по делу участвуют в обсуждениях
   темы. Отбирай по `authors` и чтению их сообщений: только те, кто точно в теме,
   не спамеры и не боты.
4. **new_keywords** — слова и короткие фразы по теме, которыми пишут в этой
   группе и которых нет в `keywords.json`: жаргон, названия мест, компаний,
   должностей. Только то, что поможет найти новые сообщения и группы.

## Формат результата

```json
{
  "group_id": -1001234567890,
  "name": "...",
  "alias": "...",
  "url": "https://t.me/...",
  "on_topic": true,
  "reason": "только если on_topic=false",
  "messages_analyzed": 2379,
  "summary": "...",
  "links": [{"url": "...", "what": "...", "message_url": "..."}],
  "contacts": [{"name": "...", "kind": "агентство|работодатель|менеджер|...", "contact": "...", "message_url": "..."}],
  "whales": [{"user_id": 123, "user_alias": "...", "messages": 115, "why": "...", "message_urls": ["..."]}],
  "new_keywords": ["..."]
}
```

Верни основному агенту путь к файлу и 2–3 строки вывода.
