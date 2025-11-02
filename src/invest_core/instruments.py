"""instruments.py

Модуль работы с инструментами через официальный SDK.

Функции:
  search_share(query)        – поиск инструмента (акция) по текстовому запросу.
  get_instrument(uid)        – получить подробности инструмента по UID.
  ensure_perspective_share(uid_or_query) – добавить бумагу в perspective_shares.
  enrich_all_perspective()   – обновить отсутствующие поля для всех бумаг.

Использует слой БД из db.py и обёртки клиента из sdk_client.py.
Все строки прокомментированы.
"""
from __future__ import annotations              # Совместимость аннотаций будущих версий
import logging                                  # Логирование операций
from typing import Optional, Dict, Any, List    # Аннотации типов

from . import db as db_layer                    # Импорт слоя работы с БД
from . import sdk_client                        # Импорт обёрток клиента SDK

log = logging.getLogger(__name__)               # Локальный логгер модуля


def _normalize_instrument(raw: Any) -> Dict[str, Any]:
    """Преобразовать объект SDK инструмента в плоский dict.

    raw: объект ответа SDK (share или instrument).
    Возвращает словарь с ожидаемыми ключами для таблицы perspective_shares.
    """
    # Инициализируем словарь результата.
    out: Dict[str, Any] = {}
    if not raw:                                  # Если объект пустой – возвращаем пустой dict
        return out
    # Извлекаем атрибуты, fallback на None если нет.
    out["ticker"] = getattr(raw, "ticker", None)
    out["name"] = getattr(raw, "name", None)
    out["uid"] = getattr(raw, "uid", None)
    out["secid"] = getattr(raw, "ticker", None)  # По текущей логике secid = ticker
    out["isin"] = getattr(raw, "isin", None)
    out["figi"] = getattr(raw, "figi", None)
    out["classCode"] = getattr(raw, "class_code", None)
    out["instrumentType"] = getattr(raw, "instrument_type", None)
    out["assetUid"] = getattr(raw, "asset_uid", None)
    return out                                   # Возвращаем нормализованный словарь


def search_share(query: str) -> Optional[Dict[str, Any]]:
    """Поиск первой акции по текстовому запросу.

    Возвращает нормализованный dict или None если ничего не найдено.
    """
    # Вызываем SDK поиск.
    resp = sdk_client.find_instrument(query=query)
    # В ответе SDK ожидается поле instruments/items.
    items = getattr(resp, "instruments", None) or getattr(resp, "items", None) or []
    if not items:                                # Если список пуст – ничего не найдено
        return None
    first = items[0]                             # Берём первый элемент
    # Пытаемся извлечь instrument или сам элемент содержит поля.
    inst = getattr(first, "instrument", None) or first
    return _normalize_instrument(inst)           # Возвращаем нормализованный dict


def get_instrument(uid: str) -> Optional[Dict[str, Any]]:
    """Получить подробности инструмента по UID, используя SDK обёртку.
    Возвращает dict или None если не найден.
    """
    raw = sdk_client.instrument_by(uid)          # Получаем объект через SDK
    if not raw:                                  # Если None – не найдено
        log.debug("Instrument not found uid=%s", uid)
        return None
    return _normalize_instrument(raw)            # Нормализуем и возвращаем


def ensure_perspective_share(uid_or_query: str) -> Dict[str, Any]:
    """Убедиться что бумага присутствует в perspective_shares.

    uid_or_query: если строка похожа на UUID (содержит '-') считаем UID, иначе делаем поиск.
    Возвращает структуру статуса.
    """
    # Определяем режим (uid или поисковая фраза).
    is_uid = '-' in uid_or_query and len(uid_or_query) > 20
    if is_uid:                                    # Если это UID
        inst = get_instrument(uid_or_query)       # Получаем данные по UID
    else:                                         # Иначе ищем по запросу
        inst = search_share(uid_or_query)
    if not inst:                                  # Если не нашли инструмент
        return {"status": "not-found", "input": uid_or_query}
    db_layer.init_schema()                        # Инициализируем схему БД
    with db_layer.get_connection() as conn:       # Открываем соединение к БД
        cur = conn.execute("SELECT uid FROM perspective_shares WHERE uid = ?", (inst["uid"],))
        row = cur.fetchone()                      # Проверяем наличие строки
        if row:                                   # Если есть – выходим
            return {"status": "exists", "uid": inst["uid"], "ticker": inst["ticker"]}
        # Вставляем новую запись.
        sql = ("INSERT INTO perspective_shares(ticker, name, uid, secid, isin, figi, classCode, instrumentType, assetUid) "
               "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")
        conn.execute(sql, (
            inst.get("ticker"), inst.get("name"), inst.get("uid"), inst.get("secid"), inst.get("isin"),
            inst.get("figi"), inst.get("classCode"), inst.get("instrumentType"), inst.get("assetUid")
        ))
        if db_layer.BACKEND == "sqlite":         # Коммит для sqlite
            conn.commit()
    return {"status": "inserted", "uid": inst["uid"], "ticker": inst["ticker"]}


def enrich_all_perspective() -> Dict[str, Any]:
    """Проход по perspective_shares: заполнить отсутствующие атрибуты.
    Возвращает статистику checked/updated.
    """
    db_layer.init_schema()                        # Убедиться что таблица существует
    checked = 0                                   # Счётчик проверенных строк
    updated = 0                                   # Счётчик обновлённых строк
    with db_layer.get_connection() as conn:       # Открываем соединение
        cur = conn.execute("SELECT uid, ticker, name, secid, isin, figi, classCode, instrumentType, assetUid FROM perspective_shares")
        rows = cur.fetchall()                     # Получаем все строки
        for r in rows:                            # Итерация по строкам
            checked += 1                          # Инкремент проверенных
            uid, ticker, name, secid, isin, figi, classCode, instrumentType, assetUid = r  # Распаковка
            if all([ticker, name, secid, isin, figi, classCode, instrumentType, assetUid]):  # Полнота
                continue                          # Если всё заполнено – пропускаем
            inst = get_instrument(uid)            # Получаем свежие данные из SDK
            if not inst:                          # Если не удалось
                log.debug("Skip enrich uid=%s (not found via SDK)", uid)
                continue
            sql = ("UPDATE perspective_shares SET ticker=?, name=?, secid=?, isin=?, figi=?, classCode=?, instrumentType=?, assetUid=? WHERE uid=?")
            conn.execute(sql, (
                inst.get("ticker"), inst.get("name"), inst.get("secid"), inst.get("isin"), inst.get("figi"),
                inst.get("classCode"), inst.get("instrumentType"), inst.get("assetUid"), uid
            ))
            updated += 1                          # Инкремент обновлений
        if db_layer.BACKEND == "sqlite":         # Коммит для sqlite
            conn.commit()
    return {"checked": checked, "updated": updated}  # Возвращаем статистику


__all__ = [  # Экспорт публичного API модуля
    "search_share",
    "get_instrument",
    "ensure_perspective_share",
    "enrich_all_perspective",
    "verify_perspective_uids",
]


def verify_perspective_uids(limit: int | None = None) -> Dict[str, Any]:
    """Проверить доступность всех UID из perspective_shares через SDK.

    Для каждого UID вызывается `get_instrument`. Если None -> считается недоступным.
    Возвращает статистику:
      {
        'total': N,
        'checked': C,
        'reachable': R,
        'unreachable': U,
        'uids_unreachable': [...],
        'uids_reachable': [...]
      }
    """
    db_layer.init_schema()
    with db_layer.get_connection() as conn:
        cur = conn.execute("SELECT uid FROM perspective_shares ORDER BY uid")
        uids = [r[0] for r in cur.fetchall() if r[0]]
    if limit is not None:
        uids = uids[:limit]
    reachable: List[str] = []
    unreachable: List[str] = []
    for uid in uids:
        inst = get_instrument(uid)
        if inst:
            reachable.append(uid)
        else:
            unreachable.append(uid)
    return {
        'total': len(uids),
        'checked': len(uids),
        'reachable': len(reachable),
        'unreachable': len(unreachable),
        'uids_reachable': reachable,
        'uids_unreachable': unreachable,
    }
