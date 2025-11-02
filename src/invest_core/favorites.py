"""favorites.py

Модуль получения списка избранных (Favorites) инструментов из профиля Tinkoff Invest
через официальный SDK.

Функционал:
  * list_favorites() – вернуть "сырые" элементы избранного.
  * list_favorite_shares() – отфильтровать только акции (share) и вернуть компактные dict.
  * ensure_perspective_from_favorites() – добавить отсутствующие акции в perspective_shares.

Требования:
  * Нужен токен (INVEST_TOKEN / INVEST_TINKOFF_TOKEN / tinkoff_token.txt)
  * Пакет tinkoff-investments должен быть установлен.

Пример использования:
    from invest_core.favorites import list_favorite_shares
    shares = list_favorite_shares()
    for s in shares:
        print(s['ticker'], s['uid'])

Структура возвращаемой записи (share):
    {
      'uid': ..., 'ticker': ..., 'name': ..., 'figi': ..., 'is_favorite': True,
      'classCode': ..., 'instrumentType': 'share'
    }
"""
from __future__ import annotations
import logging
from typing import List, Dict, Any

from . import db as db_layer
from .sdk_client import client, load_token, assert_token_exists

log = logging.getLogger(__name__)

try:
    from tinkoff.invest import Client
    from tinkoff.invest.schemas import InstrumentStatus
except ImportError:
    Client = None  # type: ignore
    InstrumentStatus = None  # type: ignore


def list_favorites() -> List[Any]:
    """Получить "сырые" объекты избранных инструментов из SDK.

    Возвращает список объектов (как есть из SDK) или пустой список при ошибке/отсутствии пакета.
    """
    token = load_token()
    try:
        assert_token_exists(token)
    except Exception as ex:
        log.warning("Token missing: %s", ex)
        return []
    if Client is None:
        log.warning("tinkoff-investments package not installed")
        return []
    with client() as c:
        try:
            resp = c.users.get_favorites()  # SDK метод получения избранного
        except Exception as ex:
            log.exception("get_favorites failed: %s", ex)
            return []
    # resp.favorite_instruments содержит список объектов FavoriteInstrument (FIGI/UID etc)
    return list(getattr(resp, 'favorite_instruments', []) or [])


def list_favorite_shares() -> List[Dict[str, Any]]:
    """Вернуть список избранных акций в компактном формате.

    Фильтруем по типу инструмента (share). Для этого нужно обогащение данных через get_instrument_by / share_by.
    Делаем отдельные запросы по UID для надежности.
    """
    favorites = list_favorites()
    results: List[Dict[str, Any]] = []
    if not favorites:
        return results
    from tinkoff.invest.schemas import InstrumentIdType, GetInstrumentRequest
    with client() as c:
        for fav in favorites:
            uid = getattr(fav, 'instrument_uid', None)
            figi = getattr(fav, 'figi', None)
            if not uid:
                continue
            inst = None
            try:
                req = GetInstrumentRequest(id=uid, id_type=InstrumentIdType.INSTRUMENT_ID_TYPE_UID)
                inst_resp = c.instruments.get_instrument_by(request=req)
                inst = getattr(inst_resp, 'instrument', None)
            except Exception:
                inst = None
            # fallback share_by
            if inst is None:
                try:
                    share_resp = c.instruments.share_by(id=uid)
                    inst = getattr(share_resp, 'instrument', None)
                except Exception:
                    inst = None
            if inst is None:
                continue
            # Проверяем тип инструмента – берем только акции
            inst_type = getattr(inst, 'instrument_type', '').lower()
            if inst_type != 'share':
                continue
            ticker = getattr(inst, 'ticker', None)
            name = getattr(inst, 'name', None)
            class_code = getattr(inst, 'class_code', None)
            results.append({
                'uid': uid,
                'figi': figi,
                'ticker': ticker,
                'name': name,
                'classCode': class_code,
                'instrumentType': 'share',
                'is_favorite': True,
            })
    return results


def ensure_perspective_from_favorites() -> Dict[str, Any]:
    """Добавить отсутствующие избранные акции в таблицу perspective_shares.

    Не обновляет уже существующие строки; только вставка новых по UID.
    Возвращает статистику: processed, inserted, already.
    """
    db_layer.init_schema()
    shares = list_favorite_shares()
    processed = 0
    inserted = 0
    already = 0
    with db_layer.get_connection() as conn:
        for s in shares:
            processed += 1
            uid = s['uid']
            ticker = s.get('ticker')
            name = s.get('name')
            class_code = s.get('classCode')
            secid = None  # Можно позже попытаться сопоставить MOEX SECID
            isin = None
            figi = s.get('figi')
            inst_type = s.get('instrumentType')
            asset_uid = None
            # Проверяем существование UID
            cur = conn.execute("SELECT uid FROM perspective_shares WHERE uid = ?", (uid,))
            if cur.fetchone():
                already += 1
                continue
            conn.execute(
                "INSERT INTO perspective_shares(ticker, name, uid, secid, isin, figi, classCode, instrumentType, assetUid) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (ticker, name, uid, secid, isin, figi, class_code, inst_type, asset_uid)
            )
        if db_layer.BACKEND == 'sqlite':
            conn.commit()
    return {
        'processed': processed,
        'inserted': inserted,
        'already': already,
        'shares': shares,
    }

__all__ = [
    'list_favorites',
    'list_favorite_shares',
    'ensure_perspective_from_favorites',
]
