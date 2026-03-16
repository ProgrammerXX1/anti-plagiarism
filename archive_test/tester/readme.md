# Archive Test — Массовая загрузка CC100 корпусов

Скрипты для загрузки и тестирования на корпусах CC100 (Russian, Turkish).

## Загрузка по файлам (L1-L4 pipeline)

```bash
python3 ingest_cc100.py --target l14 --root /data/cc100 --org 28 --api http://127.0.0.1:8088 \
  --vol-from 3 --vol-to 3 --chunk-from 1 --chunk-to 100
```

## Загрузка одного volume (L1-L4)

```bash
python3 ingest.py \
  --target l14 \
  --root ../cc100_ru_tomes \
  --org 28 \
  --api http://127.0.0.1:8088 \
  --vol-from 3 --vol-to 3 \
  --chunk-from 1 --chunk-to 100 \
  --concurrency 8 \
  --log ./ingest_l14_v0003.jsonl
```

## Загрузка одного volume (L5 — прямой ingest)

```bash
python3 ingest.py \
  --target l5 \
  --root ../cc100_ru_tomes \
  --org 28 \
  --api http://127.0.0.1:8088 \
  --segment-name cc100_ru_v0003 \
  --shards 32 \
  --vol-from 3 --vol-to 3 \
  --chunk-from 1 --chunk-to 100 \
  --tmp-dir ./tmp_zips \
  --log ./ingest_l5_v0003.jsonl
```

## Загрузка всех volumes (L5)

```bash
python3 ingest.py \
  --target l5 \
  --root ../cc100_ru_tomes \
  --org 28 \
  --api http://127.0.0.1:8088 \
  --segment-name cc100_ru_all \
  --shards 32 \
  --vol-from 1 --vol-to 15 \
  --chunk-from 1 --chunk-to 100 \
  --tmp-dir ./tmp_zips \
  --log ./ingest_l5_all.jsonl
```

## Параметры

| Параметр | Описание |
|----------|----------|
| `--target` | `l14` (через /v1/process) или `l5` (через /v1/orgs/{org}/l5/ingest_zip_stream) |
| `--root` | Корневая директория с CC100 томами |
| `--org` | ID организации |
| `--api` | URL API |
| `--vol-from`, `--vol-to` | Диапазон томов |
| `--chunk-from`, `--chunk-to` | Диапазон чанков внутри тома |
| `--concurrency` | Параллельных потоков загрузки |
| `--segment-name` | Имя сегмента (только L5) |
| `--shards` | Число шардов (только L5, default 32) |
| `--tmp-dir` | Директория для временных ZIP |
| `--log` | Файл лога (JSONL) |

## Разница L1-L4 vs L5 ingest

**L1-L4** (`--target l14`):
- Каждый документ отправляется через `POST /v1/process`
- Буферизация в L1 batch (default 20 документов) → build L1 сегмента
- Автоматическая компактизация L1→L2→L3→L4 при >= 20 сегментов
- Подходит для потоковой индексации

**L5** (`--target l5`):
- Документы упаковываются в ZIP, отправляются через `POST /v1/orgs/{org}/l5/ingest_zip_stream`
- ZIP распаковывается на сервере, строится L5 сегмент напрямую (минуя L1-L4)
- 32 шарда (configurable через `PLAGIO_L5_SHARDS`), документы распределяются по hash(source_id)
- Подходит для массовой начальной загрузки

## Структура данных CC100

```
cc100_ru_tomes/
├── vol_001/
│   ├── sub_001/
│   │   ├── doc_001.txt
│   │   ├── doc_002.txt
│   │   └── ...
│   └── sub_002/
└── vol_002/
    └── ...
```

## Что проверяется при тестировании

- Все документы проиндексированы (count в `/v1/segment_docs`)
- Поиск точной копии находит оригинал (plagiarism >= 30%)
- CPB формат V5 создаётся корректно (varint64 delta encoding)
- Shard bitmap обновлён (пропуск пустых шардов при поиске)
- Tombstones работают (soft-delete через `tombstones.jsonl`, write-first pattern)
