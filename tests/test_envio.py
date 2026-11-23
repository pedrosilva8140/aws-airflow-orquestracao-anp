import codecs
from datetime import UTC, datetime

import pytest
import requests
from conftest import BUCKET

from anp.envio import ArquivoInvalido, chave_entrada, enviar, validar_csv_basico

CABECALHO_ANP = (
    "Regiao - Sigla;Estado - Sigla;Municipio;Revenda;CNPJ da Revenda;Nome da Rua;"
    "Numero Rua;Complemento;Bairro;Cep;Produto;Data da Coleta;Valor de Venda;"
    "Valor de Compra;Unidade de Medida;Bandeira\n"
)
LINHA_ANP = (
    "SE;SP;SAO PAULO;POSTO TESTE LTDA;00.000.000/0001-00;RUA TESTE;100;;CENTRO;"
    "01000-000;GASOLINA;01/10/2026;6,19;;R$ / litro;BRANCA\n"
)
URL = "https://www.gov.br/anp/pt-br/x/shpc/dsan/2026/10-gasolina-etanol.csv"
HTML_ERRO = b"<!DOCTYPE html><html><body>Pagina em manutencao</body></html>" + b" " * 5000


def csv_anp(tamanho: int, bom: bool = False) -> bytes:
    linhas = CABECALHO_ANP
    while len(linhas.encode()) < tamanho:
        linhas += LINHA_ANP
    corpo = linhas.encode()[:tamanho]
    return codecs.BOM_UTF8 + corpo if bom else corpo


class RespostaFalsa:
    """Imita o `requests.Response` usado com `stream=True` dentro de `with`."""

    def __init__(self, conteudo: bytes, status: int = 200) -> None:
        self.conteudo = conteudo
        self.status = status

    def __enter__(self) -> "RespostaFalsa":
        return self

    def __exit__(self, *_: object) -> None:
        return None

    def raise_for_status(self) -> None:
        if self.status >= 400:
            raise requests.HTTPError(f"{self.status} Client Error")

    def iter_content(self, chunk_size: int = 1) -> list[bytes]:
        return [self.conteudo[i : i + chunk_size] for i in range(0, len(self.conteudo), chunk_size)]


def baixar_falso(conteudo: bytes, status: int = 200):
    chamadas: list[dict] = []

    def baixar(url: str, **kwargs: object) -> RespostaFalsa:
        chamadas.append({"url": url, **kwargs})
        return RespostaFalsa(conteudo, status)

    baixar.chamadas = chamadas
    return baixar


def test_chave_entrada_zero_a_esquerda():
    assert chave_entrada(2025, 1) == "entrada/precos-gasolina-etanol-2025-01.csv"


def test_rejeita_html():
    with pytest.raises(ArquivoInvalido):
        validar_csv_basico(HTML_ERRO[:4096], len(HTML_ERRO))


def test_rejeita_arquivo_pequeno():
    conteudo = csv_anp(200)
    with pytest.raises(ArquivoInvalido, match="200"):
        validar_csv_basico(conteudo, len(conteudo))


def test_aceita_csv_com_bom():
    conteudo = csv_anp(5000, bom=True)
    validar_csv_basico(conteudo[:4096], len(conteudo))


def test_enviar_sobe_e_retorna_last_modified(s3):
    conteudo = csv_anp(5000)
    chave = chave_entrada(2026, 10)
    baixar = baixar_falso(conteudo)

    retorno = enviar(s3, BUCKET, URL, chave, baixar=baixar)

    assert s3.objetos[(BUCKET, chave)] == conteudo
    assert retorno == s3.agora == datetime(2026, 11, 15, 12, 0, tzinfo=UTC)
    assert baixar.chamadas == [{"url": URL, "stream": True, "timeout": 60}]


def test_enviar_nao_sobe_se_invalido(s3):
    with pytest.raises(ArquivoInvalido):
        enviar(s3, BUCKET, URL, chave_entrada(2026, 10), baixar=baixar_falso(HTML_ERRO))
    assert s3.chaves() == []


def test_enviar_erro_http_nao_sobe(s3):
    baixar = baixar_falso(HTML_ERRO, status=404)
    with pytest.raises(requests.HTTPError):
        enviar(s3, BUCKET, URL, chave_entrada(2026, 10), baixar=baixar)
    assert s3.chaves() == []
