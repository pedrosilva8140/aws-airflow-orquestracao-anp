import io
from datetime import UTC, datetime
from typing import BinaryIO

import pytest
from botocore.exceptions import ClientError

BUCKET = "bucket-teste"


def erro_aws(codigo: str, operacao: str) -> ClientError:
    return ClientError({"Error": {"Code": codigo, "Message": codigo}}, operacao)


class S3Falso:
    """Imita só as chamadas do cliente S3 que o pacote `anp` usa, guardando tudo em memória.

    `agora` é o horário gravado como `LastModified` nas próximas escritas.
    """

    def __init__(self, agora: datetime | None = None) -> None:
        self.agora = agora or datetime(2026, 11, 15, 12, 0, tzinfo=UTC)
        self.objetos: dict[tuple[str, str], bytes] = {}
        self.modificados: dict[tuple[str, str], datetime] = {}
        self.negados: set[str] = set()
        self.tamanho_pagina = 1000
        self.listagens = 0

    def gravar(self, chave: str, corpo: bytes, quando: datetime, bucket: str = BUCKET) -> None:
        """Prepara um objeto com `LastModified` escolhido pelo teste."""
        self.objetos[(bucket, chave)] = corpo
        self.modificados[(bucket, chave)] = quando

    def _conferir_acesso(self, Key: str, operacao: str) -> None:
        if Key in self.negados:
            raise erro_aws("403", operacao)

    def upload_fileobj(self, Fileobj: BinaryIO, Bucket: str, Key: str, **_: object) -> None:
        self.gravar(Key, Fileobj.read(), self.agora, Bucket)

    def put_object(self, Bucket: str, Key: str, Body: bytes, **_: object) -> dict:
        self.gravar(Key, Body, self.agora, Bucket)
        return {}

    def head_object(self, Bucket: str, Key: str) -> dict:
        self._conferir_acesso(Key, "HeadObject")
        if (Bucket, Key) not in self.objetos:
            raise erro_aws("404", "HeadObject")
        return {
            "ContentLength": len(self.objetos[(Bucket, Key)]),
            "LastModified": self.modificados[(Bucket, Key)],
        }

    def get_object(self, Bucket: str, Key: str) -> dict:
        self._conferir_acesso(Key, "GetObject")
        if (Bucket, Key) not in self.objetos:
            raise erro_aws("NoSuchKey", "GetObject")
        return {
            "Body": io.BytesIO(self.objetos[(Bucket, Key)]),
            "LastModified": self.modificados[(Bucket, Key)],
        }

    def list_objects_v2(
        self, Bucket: str, Prefix: str = "", ContinuationToken: str | None = None, **_: object
    ) -> dict:
        """Paginação como a da AWS: no máximo `tamanho_pagina` itens por chamada."""
        self.listagens += 1
        chaves = sorted(c for b, c in self.objetos if b == Bucket and c.startswith(Prefix))
        inicio = int(ContinuationToken or 0)
        fim = inicio + self.tamanho_pagina
        resposta: dict = {
            "KeyCount": len(chaves[inicio:fim]),
            "IsTruncated": fim < len(chaves),
        }
        if chaves[inicio:fim]:
            resposta["Contents"] = [
                {"Key": c, "LastModified": self.modificados[(Bucket, c)]}
                for c in chaves[inicio:fim]
            ]
        if resposta["IsTruncated"]:
            resposta["NextContinuationToken"] = str(fim)
        return resposta

    def chaves(self) -> list[str]:
        return sorted(chave for _, chave in self.objetos)


@pytest.fixture
def s3() -> S3Falso:
    return S3Falso()
