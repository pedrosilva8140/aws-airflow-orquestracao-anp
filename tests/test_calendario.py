from datetime import UTC, date, datetime

from anp.calendario import mes_de_referencia


def test_mes_anterior_no_mesmo_ano():
    assert mes_de_referencia(date(2025, 2, 15)) == (2025, 1)


def test_janeiro_volta_para_dezembro_do_ano_anterior():
    assert mes_de_referencia(date(2025, 1, 15)) == (2024, 12)


def test_aceita_datetime_com_fuso():
    assert mes_de_referencia(datetime(2026, 11, 15, 12, 0, tzinfo=UTC)) == (2026, 10)
