# Runbook: DAG `anp_precos_mensal`

Procedimentos para os problemas esperados na operação. Cada seção segue a ordem **sintoma, como confirmar, o que fazer**.

Convenções usadas abaixo:
- `<instance-id>`: `terraform -chdir=infra output -raw instance_id`
- `<bucket-do-p2>`: output `bucket` do P2
- **Shell na instância:** `aws ssm start-session --target <instance-id>`, depois `sudo -i` e `cd /opt/airflow-anp/docker`. Os comandos `docker compose ...` abaixo partem dessa pasta.
- **Interface:** `.\scripts\abrir_airflow.ps1` e depois http://localhost:8080.

Princípio geral: o DAG foi feito para ser **reexecutado com segurança**. O nome no S3 é padronizado (reprocessar sobrescreve), o sensor só aceita saídas mais novas que o upload e a publicação usa `ADD IF NOT EXISTS`. Na dúvida, limpar e rodar de novo não duplica dados.

---

## 1. Mês ainda não publicado pela ANP

**Sintoma:** `descobrir_url` falha com `MesNaoPublicado: A ANP ainda não publicou o arquivo de gasolina e etanol de AAAA-MM.` As retentativas (5, 10 e 20 min) também falham.

**Como confirmar:** abra a página da série histórica da ANP (gov.br/anp, dados abertos, "Série histórica de preços de combustíveis") e procure o link do mês no ano correspondente.

**O que fazer:**
- É esperado quando a ANP atrasa. Não há nada quebrado.
- Quando o arquivo aparecer, limpe a execução (Clear na interface, com as tarefas seguintes). Ela recomeça do zero.
- Se o link existir na página e mesmo assim não for encontrado, o nome do arquivo saiu do padrão conhecido. Ajuste a regra em `dags/anp/descoberta.py` com um teste novo em `tests/test_descoberta.py` (com o nome real na fixture), faça um commit `fix:` e siga a seção 12.

## 2. Dois links para o mesmo mês (`MesAmbiguo`)

**Sintoma:** `descobrir_url` falha sem retentativa com `MesAmbiguo: Mais de um arquivo de gasolina e etanol para AAAA-MM: [...]`.

**Como confirmar:** a mensagem lista as duas URLs. Compare as duas na página da ANP (republicação, correção, ou outro produto que casou por engano).

**O que fazer:**
- O DAG nunca escolhe um link arbitrariamente, de propósito.
- Decida qual é o certo, ajuste a regra em `descoberta.py` com um teste que reproduza a ambiguidade, faça o commit `fix:`, atualize os DAGs (seção 12) e limpe a execução.

## 3. Arquivo rejeitado pelo P2

**Sintoma:** `aguardar_resultado` falha sem retentativa com `O P2 rejeitou o arquivo de AAAA-MM: <motivos>`. Chegam dois e-mails: o do P2 (rejeição) e o do Airflow (falha da tarefa).

**Como confirmar:**
```powershell
aws s3 cp s3://<bucket-do-p2>/rejeitado/precos-gasolina-etanol-AAAA-MM.json - --region us-east-2
```
O JSON tem `motivos` (lista), `arquivo_original` e `data_rejeicao`. O CSV rejeitado fica em `rejeitado/precos-gasolina-etanol-AAAA-MM.csv`.

**O que fazer:**
- Se for **dado ruim da ANP** (o P2 está certo): registre o mês e o motivo como evidência. Não force a publicação.
- Se for **bug no P2** (regra de validação errada): corrija no repositório do P2, reimplante e limpe a execução no Airflow. O upload novo gera um resultado novo, e o sensor ignora o JSON antigo, porque ele é anterior ao upload.

## 4. Qualidade reprovada

**Sintoma:** `checar_qualidade` falha sem retentativa com `Qualidade reprovada: <falhas>`. `publicar_particao` fica `upstream_failed`, e a partição **não** é publicada.

**Como confirmar:** no log da tarefa, procure a linha JSON `{"evento": "qualidade", ...}` com `linhas`, `linhas_minimas`, `saidas`, `aprovado` e `falhas`. As mensagens possíveis:

| Falha | Significado |
|---|---|
| `poucas linhas: N (mínimo M)` | arquivo truncado ou mês incompleto |
| `valor_venda fora da faixa [2.0, 15.0]` | preço impossível (erro de digitação na origem, ou mudança real de patamar) |
| `data_coleta fora de AAAA-MM` | linhas de outro mês no arquivo |
| `apenas N UF(s) (mínimo 27); faltando: ...` | UF sem coleta no mês |
| `produto(s) obrigatório(s) ausente(s)` | GASOLINA ou ETANOL ausente |
| `vazamento para outra partição: ...` | saídas do arquivo em outra partição (ver seção 9) |

**O que fazer:**
- **Dado ruim real:** não publique. Registre o mês e a falha.
- **Portão desatualizado** (por exemplo, o preço mudou de patamar ou uma UF deixou de ter coleta de forma legítima): decida explicitamente se muda a faixa em `dags/anp/qualidade.py`, com teste e commit, e limpe a execução.
- **Para rodar com outro mínimo de linhas sem mudar o código:** dispare uma execução manual com `conf {"linhas_minimas": N}`.

## 5. Evento na DLQ do P2

**Sintoma:** chega o e-mail do alarme da DLQ do P2. No Airflow, `aguardar_resultado` termina em timeout (15 min) sem achar `processado/` nem `rejeitado/`.

**Como confirmar:**
```powershell
aws sqs get-queue-attributes --queue-url <dlq_url do P2> --attribute-names ApproximateNumberOfMessages --region us-east-2
aws logs tail /aws/lambda/pipeline-eventos-anp-processador --since 1h --region us-east-2
```
O log da Lambda mostra a exceção. É uma falha de infraestrutura ou um bug no P2, não de dados.

**O que fazer:**
1. Corrija a causa no P2.
2. Reprocesse o arquivo sem reenviar: `aws s3 cp s3://<bucket-do-p2>/entrada/<nome>.csv s3://<bucket-do-p2>/entrada/<nome>.csv --metadata-directive REPLACE --region us-east-2`.
3. Limpe no Airflow `aguardar_resultado` e as tarefas seguintes. O Parquet novo é posterior ao upload original, então o sensor aceita.
4. Esvazie a DLQ depois de conferir as mensagens.

## 6. Instância desligada no meio de uma execução

**Sintoma:** o desligamento das 23:00 (ou um stop manual) pegou uma execução em andamento.

**Como confirmar:** `aws ec2 describe-instances --instance-ids <instance-id> --query "Reservations[0].Instances[0].StateTransitionReason"`. No CloudTrail, o `StopInstances` das 23:00 vem da role `aws-airflow-orquestracao-anp-desligamento`.

**O que fazer:**
- Religue com `.\scripts\abrir_airflow.ps1`. O Docker sobe com o sistema, e os containers voltam sozinhos (`restart: unless-stopped`), sem nenhum comando.
- A tarefa interrompida é marcada como falha pelo Airflow e entra na retentativa automática. O sensor em `reschedule` continua de onde parou.
- Se as retentativas tiverem se esgotado enquanto a máquina estava parada, limpe a execução.
- **Prevenção:** não dispare backfills longos perto das 23:00.

## 7. Disco cheio

**Sintoma:** tarefas falhando com `No space left on device`, ou containers reiniciando.

**Como confirmar** (no shell da instância):
```bash
df -h /
docker system df
docker compose exec scheduler du -sh /opt/airflow/logs
```

**O que fazer:**
- Imagens e cache de build antigos: `docker image prune -f` e `docker builder prune -f`.
- Logs de tarefas antigos: `docker compose exec scheduler find /opt/airflow/logs -type f -mtime +30 -delete`.
- Os logs dos containers já têm rotação (10 MB × 3 por serviço).
- O disco tem 30 GB. Aumentar exige mudar `volume_size` em `infra/ec2.tf` (plan e apply).

## 8. Senha do admin

**Como ler:**
```powershell
aws ssm get-parameter --name /airflow-orquestracao-anp/airflow-admin-senha --with-decryption --query Parameter.Value --output text --region us-east-2
```
O usuário é `admin`.

**Atenção:**
- O `airflow-init` cria o admin só na primeira subida, com a senha do SSM daquele momento. Mudar o parâmetro depois **não** muda a senha de um usuário que já existe.
- O parâmetro fica fora do prefixo `/aws...`, que é reservado pelo SSM.

## 9. Saída fora da partição do mês (`fora_do_mes`) ou vazamento

**Sintoma:**
- `aguardar_resultado` falha sem retentativa com `nada foi gravado em processado/ano=AAAA/mes=MM/...; partições encontradas: ...`; ou
- `checar_qualidade` reprova com `vazamento para outra partição`.

**Causa:** o P2 particiona pela `data_coleta` de cada linha, não pelo nome do arquivo. Linhas de outro mês viram um Parquet com o mesmo nome em outra partição.

**Como confirmar:** `aws s3 ls s3://<bucket-do-p2>/processado/ --recursive --region us-east-2 | findstr precos-gasolina-etanol-AAAA-MM`

**O que fazer:**
- O sensor já confirma uma segunda vez antes de falhar, porque o P2 grava as partições uma por vez. Então a falha é real.
- Verifique o arquivo da ANP: provavelmente ele traz datas erradas ou de outro mês.
- Não publique. Se a ANP corrigir o arquivo, apague as saídas indevidas em `processado/` e limpe a execução.

## 10. Erro 403 (permissão)

**Sintoma:** uma tarefa falha com `AccessDenied` ou `403` numa chamada S3, Glue, Athena ou SNS.

**Como confirmar:** o log mostra a operação (por exemplo, `HeadObject`) e a chave.

**O que fazer:**
- É problema de IAM, não de dados. O código não trata 403 como "pendente", de propósito.
- Confira a política em `infra/iam.tf`. Algumas operações têm requisitos indiretos: o `HeadObject` exige `s3:GetObject`, e um 404 em objeto inexistente exige `s3:ListBucket` (sem ele, a resposta vira 403).
- Corrija com plan e apply. Se ainda houver retentativas, a próxima passa sozinha. Se não, limpe a tarefa.

## 11. Reprocessar um mês

- **Uma execução:** na interface, Clear na execução (ou na tarefa a partir da qual quer refazer, com as seguintes). O upload sobrescreve o mesmo objeto, o P2 grava o mesmo Parquet e a publicação é idempotente.
- **Vários meses:**
  ```text
  airflow backfill create --dag-id anp_precos_mensal --from-date AAAA-MM-15 --to-date AAAA-MM-16 \
    --max-active-runs 1 --reprocess-behavior completed --dry-run
  ```
  - `none` cria só as execuções que faltam; `failed` refaz também as que falharam; `completed` refaz todas.
  - Use a data **do dia seguinte** no `--to-date`. Uma data sem hora vira 00:00 em America/Sao_Paulo e excluiria a execução das 09:00 daquele dia.
  - Confira a lista com `--dry-run` (só lê o banco) e depois rode sem ele.

## 12. Atualizar os DAGs

```powershell
.\scripts\atualizar_dags.ps1
```
- Faz `git pull --ff-only` em `/opt/airflow-anp` via SSM. Como `dags/` é montado somente leitura, o dag-processor lê a versão nova sozinho.
- Mudanças em `docker/` (imagem ou compose) exigem, no shell da instância, `docker compose up -d --build`.
- Confira os erros de import com `docker compose exec scheduler airflow dags list-import-errors`.

## 13. Ligar, desligar e acessar

- **Ligar e acessar:** `.\scripts\abrir_airflow.ps1`. O script liga a instância se estiver parada, espera o SSM ficar Online e abre o port forwarding. Ctrl+C encerra só o túnel.
- **Desligar:** `aws ec2 stop-instances --instance-ids <instance-id> --region us-east-2`. O agendamento já desliga todo dia às 23:00 (America/Sao_Paulo).
- **Saúde dos serviços:** `docker compose ps` (api-server, scheduler, dag-processor e postgres devem estar `healthy`; o airflow-init termina com código 0).
