"""tinkoff_sdk.py (DEPRECATED)

Исторический вспомогательный модуль для работы с официальным SDK был заменён:
  * `sdk_client.py` – единая точка создания клиентов
  * `forecasts.py` – загрузка/сохранение консенсусов и таргетов

Этот файл сохранён как заглушка. Используйте новые модули. Любой импорт атрибутов
из него должен быть перенаправлён. Преднамеренно без функционала.
"""
from __future__ import annotations

def __getattr__(name: str):  # Любой доступ к имени вызывает ошибку
    raise AttributeError(
        f"tinkoff_sdk.{name} устарел. Используйте sdk_client / forecasts / instruments / potentials."
    )

__all__: list[str] = []