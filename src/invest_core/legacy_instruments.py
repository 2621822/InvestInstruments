"""legacy_instruments.py
Упрощённый модуль: PostApiHeaders, GetUidInstrument, FillingSharesData (REST версия).
"""
from __future__ import annotations
import os, logging, requests
from typing import Optional, Dict, Any, List
from . import db_mysql as db_layer

log = logging.getLogger(__name__)
BASE_HOST = "invest-public-api.tbank.ru"
FIND_PATH = "/rest/tinkoff.public.invest.api.contract.v1.InstrumentsService/FindInstrument"
FIND_URL = f"https://{BASE_HOST}{FIND_PATH}"
HARDCODE_TOKEN = "t.pdllofbQHnH9F0SyYtg1YZwMPM_eAbB-V51HqAI_AVS61ODDiS4O-mMc3YaGk25kEFN1k6_iq2rnedhoWlCRLQ"

def _load_token() -> str:
    t = os.getenv("INVEST_TINKOFF_TOKEN") or os.getenv("INVEST_TOKEN")
    if t: return t.strip()
    for name in ("tinkoff_token.txt","token.txt"):
        p = os.path.join(os.getcwd(), name)
        if os.path.exists(p):
            try:
                with open(p,"r",encoding="utf-8") as f: return f.read().strip()
            except Exception: pass
    return HARDCODE_TOKEN

def _ascii(v: str) -> str: return ''.join(c for c in v if ord(c) < 128)

def PostApiHeaders() -> Dict[str,str]:
    return {"Authorization": f"Bearer {_ascii(_load_token())}", "Content-Type": "application/json", "Accept": "application/json", "User-Agent": "invest-legacy/0.1"}

def _fetch_instruments(query: str) -> List[Dict[str,Any]]:
    payload = {"query": query, "instrumentKind": "INSTRUMENT_TYPE_SHARE", "apiTradeAvailableFlag": True}
    verify_ssl = os.getenv("INVEST_TINKOFF_VERIFY_SSL","1") != "0"
    force_ip = os.getenv("INVEST_TINKOFF_FORCE_IP")
    url = FIND_URL if not force_ip else f"https://{force_ip}{FIND_PATH}"
    headers = PostApiHeaders();
    if force_ip: headers["Host"] = BASE_HOST
    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=15, verify=verify_ssl)
    except Exception:
        log.exception("FindInstrument network error query=%s", query); return []
    if resp.status_code != 200:
        log.warning("FindInstrument status=%s body=%s", resp.status_code, resp.text[:200]); return []
    try: data = resp.json()
    except Exception: log.warning("Invalid JSON FindInstrument query=%s", query); return []
    instruments = data.get("instruments") or data.get("items") or []
    out=[]
    for item in instruments:
        inst = item.get("instrument") if isinstance(item, dict) else item
        if isinstance(inst, dict): out.append(inst)
    return out

def GetUidInstrument(query: str) -> Optional[str]:
    insts = _fetch_instruments(query)
    return (insts[0].get("uid") or insts[0].get("instrumentUid")) if insts else None

def _normalize(inst: Dict[str,Any]) -> Dict[str,Any]:
    return {"ticker": inst.get("ticker"), "name": inst.get("name"), "uid": inst.get("uid") or inst.get("instrumentUid"), "secid": inst.get("ticker"), "isin": inst.get("isin"), "figi": inst.get("figi"), "classCode": inst.get("classCode") or inst.get("class_code"), "instrumentType": inst.get("instrumentType") or inst.get("instrument_type"), "assetUid": inst.get("assetUid")}

def _find_by_uid(uid: str) -> Optional[Dict[str,Any]]:
    # Поиск по UID (не гарантированно): запросим FindInstrument и проверим точное совпадение.
    for inst in _fetch_instruments(uid):
        cand = inst.get("uid") or inst.get("instrumentUid")
        if cand == uid: return _normalize(inst)
    return None

def FillingSharesData(uid: str) -> Dict[str,Any]:
    inst = _find_by_uid(uid)
    if not inst: return {"status": "not-found", "uid": uid}
    db_layer.init_schema()
    with db_layer.get_connection() as conn:
        sql = ("UPDATE perspective_shares SET ticker=?, name=?, secid=?, isin=?, figi=?, classCode=?, instrumentType=?, assetUid=? WHERE uid=?")
        db_layer.exec_sql(conn, sql, (inst.get("ticker"), inst.get("name"), inst.get("secid"), inst.get("isin"), inst.get("figi"), inst.get("classCode"), inst.get("instrumentType"), inst.get("assetUid"), uid))
    return {"status": "updated", "uid": uid, **{k: inst.get(k) for k in ["ticker","name","isin","figi","classCode","instrumentType","assetUid"]}}

__all__ = ["PostApiHeaders", "GetUidInstrument", "FillingSharesData"]
