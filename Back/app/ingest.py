import asyncio
import csv
import os
import sys
from typing import Optional

import httpx

# Allow very large CSV fields
_max_int = sys.maxsize
while True:
    try:
        csv.field_size_limit(_max_int)
        break
    except OverflowError:
        _max_int = int(_max_int / 10)

CSV_PATH = os.getenv("CSV_PATH", "./data-Bek.csv")
BASE_URL = os.getenv("BASE_URL", "http://localhost:8000").rstrip("/")
CONCURRENCY = int(os.getenv("CONCURRENCY", "8"))
TIMEOUT = float(os.getenv("TIMEOUT", "120"))

# Optional fallback if some rows have no org_id
ORG_ID_FALLBACK = os.getenv("ORG_ID", "").strip()
ORG_ID_FALLBACK_INT = int(ORG_ID_FALLBACK) if ORG_ID_FALLBACK else None

EMPTY_TEXT_LOG = os.getenv("EMPTY_TEXT_LOG", "empty_text_doc_ids.log")
FAIL_LOG = os.getenv("FAIL_LOG", "ingest_fail.log")


def pick(row: dict, key: str, default: str = "") -> str:
    v = row.get(key)
    if v is None:
        return default
    s = str(v).strip()
    return s if s else default


def resolve_org_id(row: dict) -> int:
    v = pick(row, "org_id") or pick(row, "organization_id")
    if v:
        try:
            return int(v)
        except Exception:
            raise ValueError(f"bad org_id value: {v!r}")

    if ORG_ID_FALLBACK_INT is not None:
        return ORG_ID_FALLBACK_INT

    raise ValueError("org_id missing (no org_id/organization_id in CSV and ORG_ID env not set)")


def make_payload(row: dict) -> dict:
    csv_id = pick(row, "id")
    document_id = pick(row, "document_id") or (f"{csv_id}" if csv_id else "")
    if not document_id:
        raise ValueError("document_id missing (no 'document_id' and no 'id')")

    title = pick(row, "title", default="(no title)")
    author = pick(row, "author", default="") or None
    created_at = pick(row, "created_at", default="") or None

    # keep exact content
    text = row.get("text")
    if text is None:
        text = row.get("text_content")
    if text is None:
        text = ""
    if not isinstance(text, str):
        text = str(text)

    file_name = os.path.basename(pick(row, "document", default="document.txt")) or "document.txt"

    org_id = resolve_org_id(row)

    return {
        "document_id": document_id,
        "title": title,
        "author": author,
        "created_at": created_at,
        "text": text,
        "file_name": file_name,
        "enable_ocr": False,
        "organization_id": org_id,
        "do_index": True,
        "do_search": False,
    }


async def post_one(client: httpx.AsyncClient, payload: dict) -> httpx.Response:
    return await client.post(f"{BASE_URL}/app/ingest", json=payload)


async def worker(
    client: httpx.AsyncClient,
    q: "asyncio.Queue[Optional[tuple[int, dict, dict]]]",
    empty_fh,
    fail_fh,
    stats: dict,
) -> None:
    while True:
        item = await q.get()
        if item is None:
            q.task_done()
            return

        rowno, row_raw, payload = item
        doc_id = payload.get("document_id", "")
        org_id = payload.get("organization_id", "")
        text_val = payload.get("text", "")
        text_len = len(text_val) if isinstance(text_val, str) else 0

        if text_len == 0 or not str(text_val).strip():
            stats["empty_text"] += 1
            msg = f"EMPTY_TEXT row={rowno} org_id={org_id} doc_id={doc_id}\n"
            print(msg, end="")
            empty_fh.write(msg)
            empty_fh.flush()

        try:
            r = await post_one(client, payload)
            if r.status_code == 200:
                stats["ok"] += 1
                if stats["ok"] % 25 == 0:
                    print(
                        f"ok={stats['ok']} fail={stats['fail']} empty_text={stats['empty_text']} total={stats['total']}"
                    )
            else:
                stats["fail"] += 1
                detail = r.text
                msg = (
                    f"FAIL row={rowno} org_id={org_id} doc_id={doc_id} "
                    f"status={r.status_code} text_len={text_len} detail={detail[:5000]}\n"
                )
                print(msg, end="")
                fail_fh.write(msg)
                fail_fh.flush()
        except Exception as e:
            stats["fail"] += 1
            msg = f"EXC row={rowno} org_id={org_id} doc_id={doc_id} text_len={text_len} err={e}\n"
            print(msg, end="")
            fail_fh.write(msg)
            fail_fh.flush()
        finally:
            q.task_done()


async def main() -> None:
    stats = {"ok": 0, "fail": 0, "empty_text": 0, "total": 0}

    empty_fh = open(EMPTY_TEXT_LOG, "a", encoding="utf-8")
    fail_fh = open(FAIL_LOG, "a", encoding="utf-8")

    q: asyncio.Queue[Optional[tuple[int, dict, dict]]] = asyncio.Queue(maxsize=CONCURRENCY * 4)

    async with httpx.AsyncClient(timeout=TIMEOUT) as client:
        workers = [asyncio.create_task(worker(client, q, empty_fh, fail_fh, stats)) for _ in range(CONCURRENCY)]

        with open(CSV_PATH, "r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            rowno = 1
            for row in reader:
                stats["total"] += 1
                try:
                    payload = make_payload(row)
                except Exception as e:
                    # log and continue (so you can inspect doc_id later)
                    stats["fail"] += 1
                    csv_id = pick(row, "id")
                    doc_id = pick(row, "document_id") or (f"BEK_{csv_id}" if csv_id else "")
                    msg = f"PAYLOAD_FAIL row={rowno} doc_id={doc_id} err={e}\n"
                    print(msg, end="")
                    fail_fh.write(msg)
                    fail_fh.flush()
                    rowno += 1
                    continue

                await q.put((rowno, row, payload))
                rowno += 1

        for _ in workers:
            await q.put(None)

        await q.join()
        for t in workers:
            await t

    empty_fh.close()
    fail_fh.close()

    print(f"done total={stats['total']} ok={stats['ok']} fail={stats['fail']} empty_text={stats['empty_text']}")
    print(f"logs: {EMPTY_TEXT_LOG} , {FAIL_LOG}")


if __name__ == "__main__":
    asyncio.run(main())
