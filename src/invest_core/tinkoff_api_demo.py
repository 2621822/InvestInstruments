import requests

def PostApiHeaders():
    """Возвращает headers для авторизации через Bearer-токен."""
    token = "t.pdllofbQHnH9F0SyYtg1YZwMPM_eAbB-V51HqAI_AVS61ODDiS4O-mMc3YaGk25kEFN1k6_iq2rnedhoWlCRLQ"
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": "invest-core-demo/0.1"
    }

def GetUidInstrument(query: str) -> str:
    """
    Поиск uid бумаги по поисковой фразе через POST FindInstrument.
    Возвращает первый найденный uid или пустую строку.
    """
    url = "https://invest-public-api.tbank.ru/rest/tinkoff.public.invest.api.contract.v1.InstrumentsService/FindInstrument"
    payload = {
        "query": query,
        "instrumentKind": "INSTRUMENT_TYPE_SHARE",
        "apiTradeAvailableFlag": True
    }
    headers = PostApiHeaders()
    try:
        resp = requests.post(url, json=payload, headers=headers, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        items = data.get("instruments") or data.get("items") or []
        if not items:
            print("Не найдено бумаг по запросу:", query)
            return ""
        uid = items[0].get("uid") or items[0].get("instrumentUid")
        print(f"UID для '{query}':", uid)
        return uid
    except Exception as ex:
        print("Ошибка запроса:", ex)
        return ""

if __name__ == "__main__":
    # Тест: поиск по фразе "ФосАгро"
    GetUidInstrument("ФосАгро")
