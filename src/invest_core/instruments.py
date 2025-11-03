"""instruments.py

Модуль работы с инструментами через официальный SDK.

Функции:
  search_share(query)        – поиск инструмента (акция) по текстовому запросу.
  get_instrument(uid)        – получить подробности инструмента по UID.
    ensure_perspective_share(uid_or_query) – добавить бумагу в perspective_shares.
    fill_all_perspective_shares() – массовое обновление атрибутов всех бумаг.

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
    if not items:
        return None
    # Предпочитаем первый элемент с instrument_type == 'share' (возможные варианты регистра / enum знаков)
    share_types = {"share", "INSTRUMENT_TYPE_SHARE", "SHARE"}
    chosen = None
    for it in items:
        cand = getattr(it, "instrument", None) or it
        itype = getattr(cand, "instrument_type", None) or getattr(cand, "instrumentType", None)
        if itype and itype.lower() == "share" or itype in share_types:
            chosen = cand
            break
    if chosen is None:
        # Фолбек: берём первый элемент
        first = items[0]
        chosen = getattr(first, "instrument", None) or first
    return _normalize_instrument(chosen)


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




__all__ = [  # Экспорт публичного API модуля
    "search_share",
    "get_instrument",
    "ensure_perspective_share",
    "verify_perspective_uids",
    "get_uid_instrument",
    "FillingSharesData",
    "fill_all_perspective_shares",
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


def get_uid_instrument(ticker: str) -> Optional[str]:
    """Получить UID инструмента акции по точному совпадению тикера.

    Выполняет поиск через search_share (который возвращает первый объект share),
    но для точности вызываем исходный низкоуровневый поиск find_instrument напрямую,
    фильтруем список по instrument_type in {'share','INSTRUMENT_TYPE_SHARE'} и
    точному совпадению поля ticker.

    Возвращает UID или None.
    """
    from . import sdk_client
    resp = sdk_client.find_instrument(query=ticker)
    items = getattr(resp, "instruments", None) or getattr(resp, "items", None) or []
    if not items:
        return None
    share_types = {"share", "INSTRUMENT_TYPE_SHARE", "SHARE"}
    for it in items:
        cand = getattr(it, "instrument", None) or it
        cand_ticker = getattr(cand, "ticker", None)
        cand_type = getattr(cand, "instrument_type", None) or getattr(cand, "instrumentType", None)
        if cand_ticker and cand_ticker.upper() == ticker.upper() and (cand_type and (cand_type in share_types or str(cand_type).lower() == "share")):
            return getattr(cand, "uid", None)
    return None


def FillingSharesData(uid: str) -> Dict[str, Any]:
	"""Заполнить (обновить) одну запись в perspective_shares по UID.

	Действия:
	  1. Получить инструмент через get_instrument(uid).
	  2. Если не найден – вернуть status=not-found.
	  3. Иначе выполнить UPDATE (ticker,name,secid,isin,figi,classCode,instrumentType,assetUid).
	  4. Вернуть статус и обновлённые поля.
	"""
	inst = get_instrument(uid)
	if not inst:
		return {"status": "not-found", "uid": uid}
	db_layer.init_schema()
	with db_layer.get_connection() as conn:
		sql = ("UPDATE perspective_shares SET ticker=?, name=?, secid=?, isin=?, figi=?, classCode=?, instrumentType=?, assetUid=? WHERE uid=?")
		conn.execute(sql, (
			inst.get("ticker"), inst.get("name"), inst.get("secid"), inst.get("isin"), inst.get("figi"),
			inst.get("classCode"), inst.get("instrumentType"), inst.get("assetUid"), uid
		))
		if db_layer.BACKEND == "sqlite":
			conn.commit()
	return {"status": "updated", "uid": uid, **{k: inst.get(k) for k in ["ticker","name","isin","figi","classCode","instrumentType","assetUid"]}}


def fill_all_perspective_shares(limit: Optional[int] = None) -> Dict[str, Any]:
	"""Последовательно вызвать FillingSharesData для всех UID в perspective_shares.

	limit: ограничить количество UID (для тестов).
	Возвращает статистику updated/not_found.
	"""
	db_layer.init_schema()
	with db_layer.get_connection() as conn:
		cur = conn.execute("SELECT uid FROM perspective_shares ORDER BY uid")
		uids = [r[0] for r in cur.fetchall() if r[0]]
	if limit is not None:
		uids = uids[:limit]
	updated = 0
	not_found = 0
	for u in uids:
		res = FillingSharesData(u)
		if res["status"] == "updated":
			updated += 1
		elif res["status"] == "not-found":
			not_found += 1
	return {"total": len(uids), "updated": updated, "not_found": not_found}
