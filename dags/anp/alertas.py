"""Alertas de falha do DAG no tópico SNS do P2.

`notificar_falha` é usado como `on_failure_callback`: recebe o contexto do Airflow
como dicionário e não importa `airflow`.
"""

import unicodedata

# A documentação do SNS exige assunto com "menos de 100 caracteres".
LIMITE_ASSUNTO = 99
PREFIXO_ASSUNTO = "Falha no Airflow:"


def _assunto_seguro(texto: str) -> str:
    """O SNS recusa assunto com quebra de linha ou caractere de controle: ASCII, uma linha."""
    ascii_ = unicodedata.normalize("NFKD", texto).encode("ascii", "ignore").decode("ascii")
    return " ".join(ascii_.split())[:LIMITE_ASSUNTO]


def montar_alerta(dag_id: str, task_id: str, data_logica: str, erro: str) -> tuple[str, str]:
    """Devolve (assunto, mensagem). O assunto é curto; os detalhes vão na mensagem."""
    assunto = _assunto_seguro(f"{PREFIXO_ASSUNTO} {dag_id}.{task_id} {data_logica}")
    mensagem = (
        f"DAG: {dag_id}\n"
        f"Tarefa: {task_id}\n"
        f"Data lógica: {data_logica}\n"
        f"Erro: {erro}\n"
        "\n"
        "Consulte o docs/runbook.md do projeto para o procedimento.\n"
    )
    return assunto, mensagem


def notificar_falha(sns, topico_arn: str, contexto: dict) -> None:
    """Publica no SNS o alerta da tarefa que falhou, a partir do contexto do Airflow."""
    tarefa = contexto["task_instance"]
    data = contexto.get("logical_date")
    data_logica = data.isoformat() if data is not None else "sem data lógica"
    excecao = contexto.get("exception")
    erro = f"{type(excecao).__name__}: {excecao}" if excecao is not None else "erro não informado"

    assunto, mensagem = montar_alerta(tarefa.dag_id, tarefa.task_id, data_logica, erro)
    sns.publish(TopicArn=topico_arn, Subject=assunto, Message=mensagem)
