"""Minimal authorization + instrument UID search utilities.

This file defines:
- PostApiHeaders(): returns headers dict with Bearer auth for Tinkoff Invest public API.
- GetUidInstrument(query): POST search against InstrumentsService/FindInstrument to obtain first uid.

It uses a fixed Bearer token per task requirements. For production, move token to env INVEST_TINKOFF_TOKEN or tinkoff_token.txt.
"""
from __future__ import annotations
import os
import json
import logging
from typing import Optional
import requests
from requests.exceptions import SSLError
import socket


log = logging.getLogger(__name__)

BASE_HOST = "invest-public-api.tbank.ru"
FIND_PATH = "/rest/tinkoff.public.invest.api.contract.v1.InstrumentsService/FindInstrument"
FIND_URL = f"https://{BASE_HOST}{FIND_PATH}"
# Hard-coded token per user instruction. Prefer env or file fallback if present.
HARDCODE_TOKEN = "t.pdllofbQHnH9F0SyYtg1YZwMPM_eAbB-V51HqAI_AVS61ODDiS4O-mMc3YaGk25kEFN1k6_iq2rnedhoWlCRLQ"

def _load_token() -> str:
    # Allow environment override or file; else use hard-coded.
    t = os.getenv("INVEST_TINKOFF_TOKEN")
    if t:
        return t.strip()
    path = os.path.join(os.getcwd(), "tinkoff_token.txt")
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return f.read().strip()
        except Exception:
            pass
    return HARDCODE_TOKEN

def _sanitize_ascii(value: str) -> str:
    """Remove non-ASCII characters to satisfy http.client latin-1 header encoding."""
    return ''.join(ch for ch in value if ord(ch) < 128)


def PostApiHeaders() -> dict:
    token = _sanitize_ascii(_load_token())
    # Basic header set; add Accept for clarity.
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
        # Plain ASCII user-agent
        "User-Agent": "invest-core-minimal/0.1"
    }


def GetUidInstrument(query: str) -> Optional[str]:
    """Return first instrument uid for a search phrase.

    Sends POST to InstrumentsService/FindInstrument with required fields:
      query: search phrase
      instrumentKind: INSTRUMENT_TYPE_SHARE
      apiTradeAvailableFlag: True

    Returns uid (string) or None if not found / request failed.
    """
    payload = {
        "query": query,
        "instrumentKind": "INSTRUMENT_TYPE_SHARE",
        "apiTradeAvailableFlag": True
    }
    verify_ssl = os.getenv("INVEST_TINKOFF_VERIFY_SSL", "1") != "0"
    force_ip = os.getenv("INVEST_TINKOFF_FORCE_IP")  # e.g. 178.130.128.33
    url = FIND_URL if not force_ip else f"https://{force_ip}{FIND_PATH}"
    headers = PostApiHeaders()
    if force_ip:
        # ensure Host header preserved for virtual hosting / TLS SNI still uses IP (SNI mismatch may cause cert warning)
        headers["Host"] = BASE_HOST
    use_no_proxies = os.getenv("INVEST_TINKOFF_NO_PROXIES", "0") == "1"
    proxies = None if use_no_proxies else requests.utils.get_environ_proxies(url) if hasattr(requests.utils, 'get_environ_proxies') else None
    try:
        resp = requests.post(
            url,
            headers=headers,
            json=payload,
            timeout=15,
            verify=verify_ssl,
            proxies=proxies,
        )
    except SSLError:
        if verify_ssl and os.getenv("INVEST_TINKOFF_FALLBACK_NO_VERIFY", "0") == "1":
            log.warning("SSL verification failed; retrying with verify=False (fallback enabled)")
            try:
                resp = requests.post(
                    url,
                    headers=headers,
                    json=payload,
                    timeout=15,
                    verify=False,
                    proxies=proxies,
                )
            except Exception:
                log.exception("Network error after SSL fallback calling FindInstrument")
                return None
        else:
            log.exception("SSL error calling FindInstrument (set INVEST_TINKOFF_VERIFY_SSL=0 to disable verification)")
            return None
    except Exception:
        log.exception("Network error calling FindInstrument")
        return None
    # peer ip logging
    try:
        peer_ip = resp.raw._connection.sock.getpeername()[0]  # type: ignore[attr-defined]
        log.debug("FindInstrument peer_ip=%s status=%s", peer_ip, resp.status_code)
    except Exception:
        pass
    if resp.status_code != 200:
        # Log brief diagnostic
        snippet = resp.text[:400]
        log.warning("FindInstrument status=%s body=%s", resp.status_code, snippet)
        return None
    try:
        data = resp.json()
    except Exception:
        log.warning("Invalid JSON response for FindInstrument")
        return None
    instruments = data.get("instruments") or data.get("items") or []
    if not instruments:
        return None
    first = instruments[0]
    return first.get("uid") or first.get("instrumentUid")


def _demo():
    phrase = "ФосАгро"
    uid = GetUidInstrument(phrase)
    print(f"Search phrase: {phrase}\nUID: {uid}")
    if uid is None:
        print("(UID not found or request failed – check token validity / SSL settings)")
    # Дополнительная диагностика
    DiagnoseHost()
    ProbeFindInstrument()

def DiagnoseHost():
    try:
        ip = socket.gethostbyname("invest-public-api.tbank.ru")
        print(f"Resolved invest-public-api.tbank.ru -> {ip}")
    except Exception as ex:
        print(f"DNS resolution failed: {ex}")

def ProbeFindInstrument():
    print("-- Probing FindInstrument with HEAD/OPTIONS --")
    headers = PostApiHeaders()
    use_no_proxies = os.getenv("INVEST_TINKOFF_NO_PROXIES", "0") == "1"
    proxies = None if use_no_proxies else requests.utils.get_environ_proxies(FIND_URL) if hasattr(requests.utils, 'get_environ_proxies') else None
    try:
        h = requests.head(FIND_URL, headers=headers, timeout=10, verify=os.getenv("INVEST_TINKOFF_VERIFY_SSL","1")!="0", proxies=proxies)
        print(f"HEAD status={h.status_code} allow={h.headers.get('Allow')} content-type={h.headers.get('Content-Type')}")
    except Exception as ex:
        print(f"HEAD error: {ex}")
    try:
        o = requests.options(FIND_URL, headers=headers, timeout=10, verify=os.getenv("INVEST_TINKOFF_VERIFY_SSL","1")!="0", proxies=proxies)
        print(f"OPTIONS status={o.status_code} allow={o.headers.get('Allow')} content-type={o.headers.get('Content-Type')}")
        # If JSON body, show snippet
        if o.content:
            snippet = o.text[:300]
            print(f"OPTIONS body snippet: {snippet}")
    except Exception as ex:
        print(f"OPTIONS error: {ex}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    _demo()
