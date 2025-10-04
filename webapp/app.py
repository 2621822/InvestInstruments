from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, Request, Form, HTTPException, Depends
from fastapi.responses import RedirectResponse, HTMLResponse, JSONResponse, PlainTextResponse
from fastapi.responses import StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.security import HTTPBasic, HTTPBasicCredentials
import secrets
import aiohttp
import asyncio
import time

import sqlite3
import logging

from GorbunovInvestInstruments.main import (
    DB_PATH,
    AddShareData,
    DeleteShareData,
    EnsureForecastsForMissingShares,
    UpdateConsensusForecasts,
    current_limits,
    is_auto_fetch_enabled,
)
from GorbunovInvestInstruments.async_api import (
    GetUidInstrumentAsync,
    GetInstrumentByUidAsync,
    GetConsensusByUidAsync,
    FetchConsensusBatch,
)

security = HTTPBasic()

def _basic_auth(credentials: HTTPBasicCredentials = Depends(security)):
    user = os.getenv("APP_ADMIN_USER", "admin")
    pwd = os.getenv("APP_ADMIN_PASS", "admin")
    correct_username = secrets.compare_digest(credentials.username, user)
    correct_password = secrets.compare_digest(credentials.password, pwd)
    if not (correct_username and correct_password):
        raise HTTPException(status_code=401, detail="Unauthorized")
    return credentials.username

app = FastAPI(title="Invest Instruments Admin")


class BackgroundController:
    def __init__(self):
        self.task: asyncio.Task | None = None
        self.stop_event = asyncio.Event()
        self.interval = 1800  # 30 min default
        self.mode = "ensure"  # ensure|update|both

    async def runner(self):
        logging = __import__('logging')
        token = os.getenv("TINKOFF_INVEST_TOKEN", "")
        import sqlite3
        while not self.stop_event.is_set():
            iter_start = time.time()
            try:
                if not token:
                    logging.warning("[BG] токен не задан — пропуск итерации")
                else:
                    if self.mode in {"ensure", "both"}:
                        logging.info("[BG] ensure: проверка отсутствующих прогнозов")
                        EnsureForecastsForMissingShares(DB_PATH, token, prune=True)
                    if self.mode in {"update", "both"}:
                        logging.info("[BG] batch update consensus start")
                        # собрать uid
                        with sqlite3.connect(DB_PATH) as conn:
                            cur = conn.cursor()
                            cur.execute("SELECT uid FROM perspective_shares")
                            uids = [r[0] for r in cur.fetchall()]
                        # асинхронная пакетная загрузка
                        batch_results, metrics = await FetchConsensusBatch(uids, token, concurrency=10, timeout=API_TIMEOUT * 2 if 'API_TIMEOUT' in os.environ else None)  # type: ignore
                        from GorbunovInvestInstruments.main import AddConsensusForecasts, AddConsensusTargets, current_limits, PruneHistory
                        for uid, (cons, targets) in batch_results.items():
                            AddConsensusForecasts(DB_PATH, cons)
                            AddConsensusTargets(DB_PATH, targets)
                        limits = current_limits()
                        PruneHistory(DB_PATH, limits["max_consensus_per_uid"], limits["max_targets_per_analyst"], max_age_days=limits["max_history_days"])
                        logging.info("[BG] batch update done: %s", metrics)
            except Exception as e:  # noqa
                logging.exception("[BG] ошибка фоновой итерации: %s", e)
            elapsed = time.time() - iter_start
            wait_timeout = max(5, self.interval - elapsed)
            try:
                await asyncio.wait_for(self.stop_event.wait(), timeout=wait_timeout)
            except asyncio.TimeoutError:
                pass

    def start(self, interval: int | None = None, mode: str | None = None):
        if interval:
            self.interval = interval
        if mode:
            self.mode = mode
        if self.task and not self.task.done():
            return
        loop = asyncio.get_event_loop()
        self.stop_event.clear()
        self.task = loop.create_task(self.runner())

    def stop(self):
        if self.task and not self.task.done():
            self.stop_event.set()


bg = BackgroundController()

BASE_DIR = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

static_dir = BASE_DIR / "static"
static_dir.mkdir(parents=True, exist_ok=True)
app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")


def _get_token() -> str:
    return os.getenv("TINKOFF_INVEST_TOKEN", "")


def list_shares():
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        cur.execute("SELECT uid, ticker, name FROM perspective_shares ORDER BY COALESCE(ticker,name)")
        return [dict(uid=r[0], ticker=r[1], name=r[2]) for r in cur.fetchall()]


@app.get("/", response_class=HTMLResponse)
def index(request: Request, user: str = Depends(_basic_auth)):
    shares = list_shares()
    limits = current_limits()
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "shares": shares,
            "auto_fetch": is_auto_fetch_enabled(),
            "limits": limits,
            "token_set": bool(_get_token()),
            "user": user,
        },
    )


@app.post("/shares/add")
def add_share(query: str = Form(...), user: str = Depends(_basic_auth)):
    token = _get_token()
    if not token:
        raise HTTPException(status_code=400, detail="Токен не задан в переменной окружения TINKOFF_INVEST_TOKEN")
    AddShareData(DB_PATH, query, token)
    return RedirectResponse(url="/", status_code=303)


@app.post("/shares/delete")
def delete_share(uid: str = Form(...), delete_forecasts: Optional[bool] = Form(False), user: str = Depends(_basic_auth)):
    DeleteShareData(DB_PATH, uid, delete_forecasts=bool(delete_forecasts))
    return RedirectResponse(url="/", status_code=303)


@app.post("/settings/update")
def update_settings(
    auto_fetch: Optional[str] = Form(None),
    max_consensus_per_uid: Optional[int] = Form(None),
    max_targets_per_analyst: Optional[int] = Form(None),
    max_history_days: Optional[int] = Form(None),
    user: str = Depends(_basic_auth),
):
    # Меняем переменные окружения на лету (для текущего процесса)
    os.environ["CONSENSUS_AUTO_FETCH"] = "1" if auto_fetch == "on" else "0"
    if max_consensus_per_uid is not None:
        os.environ["CONSENSUS_MAX_PER_UID"] = str(max_consensus_per_uid)
    if max_targets_per_analyst is not None:
        os.environ["CONSENSUS_MAX_TARGETS_PER_ANALYST"] = str(max_targets_per_analyst)
    if max_history_days is not None:
        os.environ["CONSENSUS_MAX_HISTORY_DAYS"] = str(max_history_days)
    return RedirectResponse(url="/", status_code=303)


@app.post("/actions/ensure")
def ensure_forecasts(user: str = Depends(_basic_auth)):
    token = _get_token()
    if not token:
        raise HTTPException(status_code=400, detail="Токен не задан")
    EnsureForecastsForMissingShares(DB_PATH, token, prune=True)
    return RedirectResponse(url="/", status_code=303)


# Simple health endpoint
@app.get("/health")
def health():
    return {"status": "ok", "auto_fetch": is_auto_fetch_enabled(), **current_limits(), "bg_mode": bg.mode, "bg_running": bool(bg.task and not bg.task.done())}


# -------- REST API (JSON) ---------
def _share_dict(row):
    return {"uid": row[0], "ticker": row[1], "name": row[2]}


@app.get("/api/shares", response_class=JSONResponse)
def api_list_shares(user: str = Depends(_basic_auth)):
    return list_shares()


@app.post("/api/shares", response_class=JSONResponse)
def api_add_share(payload: dict, user: str = Depends(_basic_auth)):
    query = payload.get("query")
    if not query:
        raise HTTPException(status_code=422, detail="query is required")
    token = _get_token()
    if not token:
        raise HTTPException(status_code=400, detail="token not set")
    AddShareData(DB_PATH, query, token)
    return {"status": "ok"}


@app.delete("/api/shares/{uid}", response_class=JSONResponse)
def api_delete_share(uid: str, user: str = Depends(_basic_auth)):
    ok = DeleteShareData(DB_PATH, uid, delete_forecasts=True)
    if not ok:
        raise HTTPException(status_code=404, detail="not found")
    return {"status": "deleted"}


@app.post("/api/ensure", response_class=JSONResponse)
def api_ensure(user: str = Depends(_basic_auth)):
    token = _get_token()
    if not token:
        raise HTTPException(status_code=400, detail="token not set")
    EnsureForecastsForMissingShares(DB_PATH, token, prune=True)
    return {"status": "ok"}


@app.post("/api/update", response_class=JSONResponse)
def api_update(user: str = Depends(_basic_auth)):
    token = _get_token()
    if not token:
        raise HTTPException(status_code=400, detail="token not set")
    UpdateConsensusForecasts(DB_PATH, token)
    return {"status": "ok"}


@app.post("/api/update/batch", response_class=JSONResponse)
async def api_update_batch(concurrency: int = 5, user: str = Depends(_basic_auth)):
    token = _get_token()
    if not token:
        raise HTTPException(status_code=400, detail="token not set")
    import sqlite3
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        cur.execute("SELECT uid FROM perspective_shares")
        uids = [r[0] for r in cur.fetchall()]
    batch = await FetchConsensusBatch(uids, token, concurrency=concurrency)
    # сохранить результаты
    from GorbunovInvestInstruments.main import AddConsensusForecasts, AddConsensusTargets, current_limits, PruneHistory
    for uid, (cons, targets) in batch.items():
        AddConsensusForecasts(DB_PATH, cons)
        AddConsensusTargets(DB_PATH, targets)
    limits = current_limits()
    PruneHistory(DB_PATH, limits["max_consensus_per_uid"], limits["max_targets_per_analyst"], max_age_days=limits["max_history_days"])
    return {"status": "ok", "processed": len(batch)}


@app.post("/api/bg/start", response_class=JSONResponse)
def api_bg_start(interval: int = 1800, mode: str = "both", user: str = Depends(_basic_auth)):
    if mode not in {"ensure", "update", "both"}:
        raise HTTPException(status_code=422, detail="bad mode")
    bg.start(interval=interval, mode=mode)
    return {"status": "started", "interval": interval, "mode": mode}


@app.post("/api/bg/stop", response_class=JSONResponse)
def api_bg_stop(user: str = Depends(_basic_auth)):
    bg.stop()
    return {"status": "stopping"}


@app.get("/api/forecasts/{uid}", response_class=JSONResponse)
def api_forecasts(uid: str, user: str = Depends(_basic_auth)):
    import sqlite3
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        c.execute("SELECT recommendationDate, recommendation, priceConsensus, minTarget, maxTarget FROM consensus_forecasts WHERE uid=? ORDER BY recommendationDate DESC LIMIT 10", (uid,))
        forecasts = [
            {
                "recommendationDate": r[0],
                "recommendation": r[1],
                "priceConsensus": r[2],
                "minTarget": r[3],
                "maxTarget": r[4],
            }
            for r in c.fetchall()
        ]
        c.execute("SELECT recommendationDate, company, targetPrice, recommendation FROM consensus_targets WHERE uid=? ORDER BY recommendationDate DESC LIMIT 10", (uid,))
        targets = [
            {
                "recommendationDate": r[0],
                "company": r[1],
                "targetPrice": r[2],
                "recommendation": r[3],
            }
            for r in c.fetchall()
        ]
    return {"uid": uid, "forecasts": forecasts, "targets": targets}


# -------- Просмотр логов ---------
@app.get("/logs", response_class=PlainTextResponse)
def view_logs(lines: int = 300, user: str = Depends(_basic_auth)):
    log_file = os.getenv("APP_LOG_FILE", "app.log")
    try:
        with open(log_file, "r", encoding="utf-8") as f:
            content = f.readlines()[-lines:]
        return "".join(content)
    except FileNotFoundError:
        return "(лог файл не найден)"


# -------- Экспорт эндпоинты ---------
@app.get("/api/export/shares")
def api_export_shares(user: str = Depends(_basic_auth)):
    from GorbunovInvestInstruments.main import export_perspective_shares_to_excel
    filename = "perspective_shares.xlsx"
    export_perspective_shares_to_excel(DB_PATH, filename)
    try:
        file = open(filename, "rb")
    except FileNotFoundError:
        raise HTTPException(status_code=500, detail="export failed")
    return StreamingResponse(file, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", headers={"Content-Disposition": f"attachment; filename={filename}"})


@app.get("/api/export/consensus")
def api_export_consensus(user: str = Depends(_basic_auth)):
    from GorbunovInvestInstruments.main import export_consensus_to_excel
    filename = "consensus_data.xlsx"
    export_consensus_to_excel(DB_PATH, filename)
    try:
        file = open(filename, "rb")
    except FileNotFoundError:
        raise HTTPException(status_code=500, detail="export failed")
    return StreamingResponse(file, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", headers={"Content-Disposition": f"attachment; filename={filename}"})


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("webapp.app:app", host="0.0.0.0", port=int(os.getenv("PORT", 8000)), reload=False)
