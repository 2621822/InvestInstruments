"""Минимальный пакет для ежедневного job.

Оставлены только необходимые модули:
	- db_mysql: слой доступа и инициализация схемы
	- normalization: утилиты нормализации чисел и дат
	- forecasts: загрузка и сохранение консенсусов/таргетов
	- moex_history: догрузка исторических цен
	- potentials: расчёт и обслуживание потенциалов

Дополнительные legacy/инструментальные модули удалены для упрощения окружения.
"""

from . import db_mysql, normalization, forecasts, moex_history, potentials  # re-export

__all__ = [
		'db_mysql', 'normalization', 'forecasts', 'moex_history', 'potentials'
]
