from datetime import UTC, datetime
from types import SimpleNamespace

from anp.alertas import montar_alerta, notificar_falha

TOPICO = "arn:aws:sns:us-east-2:000000000000:alertas-teste"


class SNSFalso:
    def __init__(self) -> None:
        self.publicacoes: list[dict] = []

    def publish(self, **kwargs: object) -> dict:
        self.publicacoes.append(kwargs)
        return {"MessageId": "1"}


def test_assunto_menos_de_100_caracteres():
    assunto, mensagem = montar_alerta("d" * 80, "t" * 80, "2026-11-15T00:00:00+00:00", "erro")
    assert len(assunto) == 99
    assert assunto.startswith("Falha no Airflow:")
    assert "d" * 80 in mensagem and "t" * 80 in mensagem and "erro" in mensagem


def test_assunto_ascii_sem_quebra():
    assunto, mensagem = montar_alerta("anp_precos_mensal", "publicação\nfinal", "2026", "não")
    assert assunto.isascii()
    assert "\n" not in assunto and "\r" not in assunto
    assert "publicacao final" in assunto
    assert "publicação\nfinal" in mensagem and "não" in mensagem


def test_notificar_publica_no_topico():
    sns = SNSFalso()
    contexto = {
        "task_instance": SimpleNamespace(dag_id="anp_precos_mensal", task_id="checar_qualidade"),
        "logical_date": datetime(2026, 11, 15, tzinfo=UTC),
        "exception": ValueError("poucas linhas: 500 (mínimo 10000)"),
    }
    notificar_falha(sns, TOPICO, contexto)

    assert len(sns.publicacoes) == 1
    publicacao = sns.publicacoes[0]
    assert publicacao["TopicArn"] == TOPICO
    assert publicacao["Subject"].startswith("Falha no Airflow: anp_precos_mensal.checar_qualidade")
    assert "2026-11-15T00:00:00+00:00" in publicacao["Message"]
    assert "ValueError: poucas linhas: 500 (mínimo 10000)" in publicacao["Message"]


def test_notificar_sem_data_logica_nem_excecao():
    sns = SNSFalso()
    contexto = {"task_instance": SimpleNamespace(dag_id="d", task_id="t"), "logical_date": None}
    notificar_falha(sns, TOPICO, contexto)
    mensagem = sns.publicacoes[0]["Message"]
    assert "sem data lógica" in mensagem and "erro não informado" in mensagem
