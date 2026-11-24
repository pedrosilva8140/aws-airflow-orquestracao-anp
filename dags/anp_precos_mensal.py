"""DAG mensal da ANP: descobre o arquivo do mês, alimenta o P2, espera o resultado,
aplica os portões de qualidade e publica a partição no Glue/Athena.

Este arquivo só encadeia as funções do pacote `anp` (testadas sem Airflow). A
configuração vem de variáveis de ambiente lidas na execução das tarefas, nunca no
parse do DAG: ANP_BUCKET, ANP_TOPICO_ALERTAS_ARN, ANP_GLUE_DATABASE, ANP_GLUE_TABELA,
ANP_ATHENA_WORKGROUP (e AWS_DEFAULT_REGION). Credenciais: só a role da instância.
"""

import json
import logging
import os
from datetime import datetime, timedelta

import boto3
import pendulum
import requests
from airflow.sdk import PokeReturnValue, dag, task
from airflow.sdk.exceptions import AirflowFailException

from anp.alertas import notificar_falha
from anp.calendario import mes_de_referencia
from anp.descoberta import MesAmbiguo, encontrar_url_mes
from anp.envio import chave_entrada, enviar
from anp.publicacao import executar_athena, sql_adicionar_particao
from anp.qualidade import LINHAS_MINIMAS, checar, ler_parquet
from anp.resultado import chave_parquet, listar_saidas, verificar_com_confirmacao

PAGINA_ANP = (
    "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/"
    "serie-historica-de-precos-de-combustiveis"
)
TIMEOUT_PAGINA_S = 60

logger = logging.getLogger(__name__)


def _alertar_falha(contexto: dict) -> None:
    notificar_falha(boto3.client("sns"), os.environ["ANP_TOPICO_ALERTAS_ARN"], contexto)


@dag(
    dag_id="anp_precos_mensal",
    description="ANP -> P2 -> qualidade -> Athena, um mês por execução",
    schedule="0 9 15 * *",
    start_date=pendulum.datetime(2025, 2, 1, tz="America/Sao_Paulo"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
        # Airflow 3.4: multiplicador (2.0 = o atraso dobra a cada retentativa: 5, 10, 20 min).
        "retry_exponential_backoff": 2.0,
        "execution_timeout": timedelta(minutes=10),
        "on_failure_callback": _alertar_falha,
    },
    params={"linhas_minimas": LINHAS_MINIMAS},
    tags=["anp", "p2", "athena"],
)
def anp_precos_mensal():
    @task
    def descobrir_url(**contexto) -> dict:
        ano, mes = mes_de_referencia(contexto["logical_date"])
        resposta = requests.get(PAGINA_ANP, timeout=TIMEOUT_PAGINA_S)
        resposta.raise_for_status()
        try:
            url = encontrar_url_mes(resposta.text, ano, mes)
        except MesAmbiguo as erro:
            # Republicação ou link novo que casa por engano: decidir à mão, não retentar.
            raise AirflowFailException(str(erro)) from erro
        # MesNaoPublicado sobe como erro comum: as retentativas tentam mais tarde.
        logger.info("Arquivo de %s-%02d: %s", ano, mes, url)
        return {"ano": ano, "mes": mes, "url": url}

    @task
    def enviar_para_p2(alvo: dict) -> dict:
        chave = chave_entrada(alvo["ano"], alvo["mes"])
        desde = enviar(boto3.client("s3"), os.environ["ANP_BUCKET"], alvo["url"], chave)
        logger.info("Enviado para s3://.../%s em %s", chave, desde.isoformat())
        return {**alvo, "desde": desde.isoformat()}

    @task.sensor(poke_interval=30, timeout=900, mode="reschedule")
    def aguardar_resultado(envio: dict) -> PokeReturnValue:
        ano, mes = envio["ano"], envio["mes"]
        resultado = verificar_com_confirmacao(
            boto3.client("s3"),
            os.environ["ANP_BUCKET"],
            ano,
            mes,
            datetime.fromisoformat(envio["desde"]),
        )
        if resultado.status == "rejeitado":
            raise AirflowFailException(
                f"O P2 rejeitou o arquivo de {ano}-{mes:02d}: " + "; ".join(resultado.motivos)
            )
        if resultado.status == "fora_do_mes":
            raise AirflowFailException(
                f"O P2 processou o arquivo de {ano}-{mes:02d}, mas nada foi gravado em "
                f"{chave_parquet(ano, mes)}; partições encontradas: " + ", ".join(resultado.saidas)
            )
        if resultado.status == "processado":
            return PokeReturnValue(is_done=True, xcom_value={**envio, "chave": resultado.chave})
        return PokeReturnValue(is_done=False)

    @task
    def checar_qualidade(processado: dict, **contexto) -> dict:
        s3 = boto3.client("s3")
        bucket = os.environ["ANP_BUCKET"]
        ano, mes = processado["ano"], processado["mes"]
        desde = datetime.fromisoformat(processado["desde"])
        saidas = listar_saidas(s3, bucket, ano, mes, desde)
        tabela = ler_parquet(s3, bucket, processado["chave"])
        linhas_minimas = int(contexto["params"]["linhas_minimas"])
        falhas = checar(tabela, ano, mes, saidas, linhas_minimas=linhas_minimas)
        logger.info(
            json.dumps(
                {
                    "evento": "qualidade",
                    "mes": f"{ano}-{mes:02d}",
                    "linhas": tabela.num_rows,
                    "linhas_minimas": linhas_minimas,
                    "saidas": saidas,
                    "aprovado": not falhas,
                    "falhas": falhas,
                },
                ensure_ascii=False,
            )
        )
        if falhas:
            raise AirflowFailException("Qualidade reprovada: " + "; ".join(falhas))
        return processado

    @task
    def publicar_particao(aprovado: dict) -> str:
        database = os.environ["ANP_GLUE_DATABASE"]
        sql = sql_adicionar_particao(
            database,
            os.environ["ANP_GLUE_TABELA"],
            os.environ["ANP_BUCKET"],
            aprovado["ano"],
            aprovado["mes"],
        )
        execucao = executar_athena(
            boto3.client("athena"), sql, os.environ["ANP_ATHENA_WORKGROUP"], database
        )
        logger.info("Partição %s-%02d publicada (%s)", aprovado["ano"], aprovado["mes"], execucao)
        return execucao

    publicar_particao(checar_qualidade(aguardar_resultado(enviar_para_p2(descobrir_url()))))


anp_precos_mensal()
