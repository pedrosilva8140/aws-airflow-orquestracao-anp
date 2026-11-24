"""Mês de referência de uma execução do DAG.

O DAG roda no dia 15 de cada mês e processa o mês anterior (a ANP publica no início
do mês seguinte). Com o `CronTriggerTimetable` do Airflow 3, a data lógica é o próprio
instante do disparo: 2025-02-15 processa jan/2025.
"""

from datetime import date, datetime


def mes_de_referencia(data_logica: date | datetime) -> tuple[int, int]:
    """(ano, mês) do mês anterior ao da data lógica."""
    if data_logica.month == 1:
        return data_logica.year - 1, 12
    return data_logica.year, data_logica.month - 1
