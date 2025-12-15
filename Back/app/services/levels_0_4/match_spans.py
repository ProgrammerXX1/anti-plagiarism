from typing import List, Dict


def build_match_spans(
    q_pos: List[int],
    d_pos: List[int],
) -> List[Dict[str, int]]:
    """
    Превращает параллельные массивы q_pos/d_pos
    в читаемые интервалы (spans).

    Возвращает список:
      {q_from, q_to, d_from, d_to, length}
    """
    if not q_pos or not d_pos or len(q_pos) != len(d_pos):
        return []

    spans = []

    q_start = q_pos[0]
    d_start = d_pos[0]
    q_prev = q_pos[0]
    d_prev = d_pos[0]

    for i in range(1, len(q_pos)):
        q = q_pos[i]
        d = d_pos[i]

        # продолжаем span
        if q == q_prev + 1 and d == d_prev + 1:
            q_prev = q
            d_prev = d
            continue

        # закрываем текущий span
        spans.append({
            "q_from": q_start,
            "q_to": q_prev,
            "d_from": d_start,
            "d_to": d_prev,
            "length": (q_prev - q_start + 1),
        })

        # начинаем новый
        q_start = q
        d_start = d
        q_prev = q
        d_prev = d

    # последний span
    spans.append({
        "q_from": q_start,
        "q_to": q_prev,
        "d_from": d_start,
        "d_to": d_prev,
        "length": (q_prev - q_start + 1),
    })

    return spans
