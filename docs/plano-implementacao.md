# P4 - Orquestração com Airflow na AWS: plano de implementação

> **Como executar:** uma tarefa por vez no Claude Code do VS Code, seguindo o `CLAUDE.md`. Ao fim de cada tarefa: `ruff check .`, `pytest`, commit, marcar `[x]` e PARAR até o "ok" do Pedro. Os passos usam caixas `- [ ]` para acompanhamento.

**Objetivo:** um DAG mensal no Airflow (EC2) que alimenta o P2 com o arquivo da ANP, espera o resultado, aplica portões de qualidade, publica a partição no Glue/Athena e reprocessa jan/2025 a out/2026.

**Arquitetura:** a lógica fica em funções puras no pacote `dags/anp/` (sem import do Airflow, testáveis no Windows). O arquivo do DAG só encadeia essas funções. A infra (EC2 + SSM + Scheduler + Glue + Athena) é toda em Terraform e consome os outputs do P2.

**Stack:** Python 3.12, Apache Airflow 3.x (Docker Compose, LocalExecutor, Postgres), boto3, pyarrow, requests, Terraform 1.17.1, AWS (EC2, SSM, EventBridge Scheduler, Glue, Athena, S3/SNS do P2), pytest, ruff, GitHub Actions.

**Spec:** `docs/design.md` (o documento de design aprovado em 23/11/2026).

## Restrições globais

- Repositório: `aws-airflow-orquestracao-anp`, em `C:\Users\pedro\projetos\`. Região `us-east-2`.
- Commits só como Pedro Henrique; Conventional Commits em português; nenhuma menção a Claude/IA. `CLAUDE.md` e `.claude/` no `.gitignore`.
- Nunca `git push`, `terraform apply` ou `terraform destroy` sem autorização explícita. Sempre `plan -out=tfplan` + `apply tfplan`.
- **O Airflow não roda no Windows.** Tudo que importa `airflow` só é testado no CI (Linux) ou na EC2. O pacote `dags/anp/` nunca importa `airflow`.
- Instância: `m7i-flex.large`, Amazon Linux 2023 (AMI via parâmetro SSM `/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64`), disco gp3 30 GB, IMDSv2 obrigatório com `http_put_response_hop_limit = 2`, security group **sem regra de entrada**, porta 8080 publicada só em `127.0.0.1`.
- Desligamento automático: `cron(0 23 * * ? *)`, fuso `America/Sao_Paulo`.
- Athena: workgroup com `bytes_scanned_cutoff_per_query = 104857600` (100 MB); resultados em `s3://<bucket-P2>/athena-resultados/`.
- Glue: database `anp_combustiveis`, tabela `precos_gasolina_etanol`, Parquet, partições `ano` e `mes` (string).
- Interface com o P2 (conferir em `src/processador/transformacao.py` do P2 antes da Tarefa 3):
  - entrada: `entrada/precos-gasolina-etanol-{ano}-{mes:02d}.csv`
  - saída: `processado/ano={ano}/mes={mes:02d}/precos-gasolina-etanol-{ano}-{mes:02d}.parquet`
  - rejeição: `rejeitado/precos-gasolina-etanol-{ano}-{mes:02d}.json`, com a chave `motivos: list[str]`
- Variáveis de ambiente dos containers: `ANP_BUCKET`, `ANP_TOPICO_ALERTAS_ARN`, `ANP_GLUE_DATABASE`, `ANP_GLUE_TABELA`, `ANP_ATHENA_WORKGROUP`, `AWS_DEFAULT_REGION=us-east-2`. Nenhuma chave AWS: só a role da instância.
- Período do backfill: execuções de 2025-02-15 a 2026-11-15, que correspondem aos meses de referência jan/2025 a out/2026 (22 execuções).

## Pontos de atenção para a revisão

1. **Mês ainda não publicado pela ANP** (link ausente): `MesNaoPublicado` com mensagem clara, e as retentativas tentam mais tarde. Teste na Tarefa 1.
2. **Dois links para o mesmo mês** (republicação, ou um GLP que casa por engano): erro explícito `MesAmbiguo`, nunca escolher um arbitrariamente. Teste na Tarefa 1.
3. **A ANP responde 200 com HTML de erro ou arquivo vazio** em vez do CSV: `validar_csv_basico` barra antes do upload, para não poluir o `rejeitado/` do P2. Teste na Tarefa 2.
4. **Parquet antigo de uma execução anterior** presente durante o reprocessamento: o sensor só aceita objeto com `LastModified >= desde`. Teste na Tarefa 3.
5. **Instância desligada às 23:00 no meio de uma execução:** ao religar, o Airflow sobe sozinho e a tarefa é retentada. Verificação manual na Tarefa 8.

## Estrutura de arquivos

```
aws-airflow-orquestracao-anp/
  .github/workflows/ci.yml
  docker/   Dockerfile  docker-compose.yml  requirements.txt  .env.example
  dags/
    anp_precos_mensal.py          # só o DAG
    anp/__init__.py
    anp/descoberta.py             # URL do mês na página da ANP
    anp/envio.py                  # download, validação básica, upload para o P2
    anp/resultado.py              # decide processado/rejeitado/pendente
    anp/qualidade.py              # portões de qualidade
    anp/publicacao.py             # SQL da partição + execução no Athena
    anp/alertas.py                # mensagem e publicação no SNS
    anp/calendario.py             # mês de referência a partir da data lógica
  tests/  test_*.py  fixtures/pagina_anp.html
  infra/  providers.tf variables.tf outputs.tf ec2.tf iam.tf agendamento.tf catalogo.tf ssm.tf user_data.sh.tftpl
  scripts/ abrir_airflow.ps1  atualizar_dags.ps1
  docs/   design.md  plano-implementacao.md  runbook.md  img/
  pyproject.toml  requirements-dev.txt  .gitignore  README.md
```

---

### Tarefa 0: Reimplantar o P2 (pré-requisito, sem código)

- [ ] No repositório do P2: `python scripts/empacotar.py`; `cd infra`; `terraform init`; `terraform plan -out=tfplan` (esperado: 15 to add); `terraform apply tfplan` (com autorização).
- [ ] Confirmar a assinatura do e-mail do SNS.
- [ ] Esperar 5 minutos (propagação da notificação, lição do P2) e anotar os outputs `bucket` e `topico_alertas_arn`.

### Tarefa 1: Estrutura do repositório e descoberta da URL do mês

**Arquivos:** criar a estrutura acima (pastas vazias com `.gitkeep`), `pyproject.toml` (ruff line-length 100, py312, `select = ["E","F","I","B","UP"]`; pytest `pythonpath = ["dags"]`), `requirements-dev.txt` (pytest, ruff, boto3, pyarrow, requests), `.gitignore` (padrão do P2, mais `docker/.env`), `docs/design.md`, `docs/plano-implementacao.md`, `CLAUDE.md` (regras do P2 + contexto do P4); `dags/anp/descoberta.py`; `tests/test_descoberta.py`; `tests/fixtures/pagina_anp.html`.

**Interfaces (produz):**
- `class MesNaoPublicado(Exception)`, `class MesAmbiguo(Exception)`
- `extrair_links(html: str) -> list[str]` (todos os `href`, com o stdlib `html.parser`)
- `encontrar_url_mes(html: str, ano: int, mes: int, base: str = "https://www.gov.br") -> str` (URL absoluta)

- [ ] **Passo 1: fixture.** `pagina_anp.html` mínimo, com links `<a href>` reais (caminhos relativos e absolutos):
  `.../shpc/dsan/2026/10-gasolina-etanol.csv`, `.../2026/09-dados-abertos-precos-2026-09-gasolina-etanol.csv`, `.../2026/04-dados-abertos-precos-gasolina-etanol` (sem extensão), `.../2026/02-cados-abertos-preco-gasolina-etanol.csv`, `.../2025/precos-gasolina-etanol-12.csv`, `.../2025/precos-gasolina-etanol-01.csv`, mais um GLP de chamariz `.../2026/10-dados-abertos-precos-glp.csv`.
- [ ] **Passo 2: testes que falham:**
  - `test_outubro_2026` -> termina em `/2026/10-gasolina-etanol.csv`
  - `test_setembro_2026_com_ano_no_nome` -> `/2026/09-dados-abertos-precos-2026-09-gasolina-etanol.csv`
  - `test_abril_2026_sem_extensao` -> `/2026/04-dados-abertos-precos-gasolina-etanol`
  - `test_fevereiro_2026_com_erro_de_digitacao` -> `/2026/02-cados-abertos-preco-gasolina-etanol.csv`
  - `test_dezembro_2025_padrao_antigo` -> `/2025/precos-gasolina-etanol-12.csv`
  - `test_ignora_glp`: outubro/2026 não retorna o link de GLP
  - `test_mes_nao_publicado`: novembro/2026 -> `MesNaoPublicado` com "2026-11" na mensagem
  - `test_mes_ambiguo`: HTML com dois links de gasolina-etanol para 2026-10 -> `MesAmbiguo`
  - `test_url_relativa_vira_absoluta`: começa com `https://www.gov.br/`
- [ ] **Passo 3: rodar** `pytest tests/test_descoberta.py -v`. Esperado: FAIL (módulo inexistente).
- [ ] **Passo 4: implementar.** Regra: candidatos são os links com `/dsan/{ano}/` cujo nome do arquivo contém `gasolina` e `etanol`. O mês é o primeiro token de 2 dígitos entre 01 e 12, delimitado por início, fim, `-`, `_` ou `.`, desconsiderando o token de 4 dígitos do ano. Exatamente 1 candidato por mês, senão a exceção correspondente.
- [ ] **Passo 5: rodar** pytest e ruff. Esperado: PASS e lint limpo.
- [ ] **Passo 6: commit** `feat: adiciona descoberta da url mensal da anp com testes`.

### Tarefa 2: Envio para o P2

**Arquivos:** `dags/anp/envio.py`, `tests/test_envio.py`.

**Interfaces (produz):**
- `chave_entrada(ano: int, mes: int) -> str` -> `entrada/precos-gasolina-etanol-{ano}-{mes:02d}.csv`
- `class ArquivoInvalido(Exception)`
- `validar_csv_basico(inicio: bytes, tamanho_total: int) -> None`: exige `tamanho_total >= 1000`, primeira linha decodificável (`utf-8-sig`, senão `latin-1`), contendo `;` e `Data da Coleta`
- `enviar(s3, bucket: str, url: str, chave: str, baixar=requests.get) -> datetime`: baixa em streaming (timeout 60 s) para `tempfile.SpooledTemporaryFile`, valida, faz `upload_fileobj` e devolve o `LastModified` (UTC) do `head_object`. `baixar` é injetável para os testes.

- [ ] **Passo 1: testes que falham:** `test_chave_entrada_zero_a_esquerda` (2025, 1 -> `...-2025-01.csv`); `test_rejeita_html` (bytes `<!DOCTYPE html>...` -> `ArquivoInvalido`); `test_rejeita_arquivo_pequeno` (cabeçalho certo com 200 bytes -> `ArquivoInvalido`); `test_aceita_csv_com_bom` (BOM + cabeçalho real da ANP, 5000 bytes -> sem exceção); `test_enviar_sobe_e_retorna_last_modified` (S3 falso em memória + `baixar` falso -> objeto existe na chave e o retorno é igual ao `LastModified` do falso); `test_enviar_nao_sobe_se_invalido` (HTML -> `ArquivoInvalido` e nenhum objeto no S3 falso).
- [ ] **Passo 2:** rodar. Esperado: FAIL.
- [ ] **Passo 3:** implementar (reaproveitar o estilo do `S3Falso` do P2 em `tests/conftest.py`).
- [ ] **Passo 4:** pytest e ruff. Esperado: PASS.
- [ ] **Passo 5: commit** `feat: adiciona envio do arquivo da anp para o p2 com validacao basica`.

### Tarefa 3: Leitura do resultado do P2

**Arquivos:** `dags/anp/resultado.py`, `tests/test_resultado.py`.

**Interfaces (produz):**
- `chave_parquet(ano, mes) -> str` e `chave_rejeicao(ano, mes) -> str`, no formato das restrições globais
- `@dataclass(frozen=True) class Resultado: status: Literal["pendente", "processado", "rejeitado"]; chave: str | None = None; motivos: tuple[str, ...] = ()`
- `verificar_resultado(s3, bucket: str, ano: int, mes: int, desde: datetime) -> Resultado`. A rejeição tem prioridade se as duas existirem e forem recentes. Objetos com `LastModified < desde` são ignorados.

- [ ] **Passo 1: confirmar** o formato das chaves no `transformacao.py` e no `handler.py` do P2. Se diferir, ajustar as restrições globais deste plano antes de seguir.
- [ ] **Passo 2: testes que falham:** `test_pendente_sem_objetos`; `test_processado_recente`; `test_ignora_parquet_antigo` (LastModified 1 h antes de `desde` -> `pendente`); `test_rejeitado_com_motivos` (JSON com 2 motivos -> `motivos` com os 2 textos); `test_rejeicao_tem_prioridade`; `test_aceita_last_modified_igual_a_desde`.
- [ ] **Passos 3-5:** rodar (FAIL), implementar, rodar (PASS).
- [ ] **Passo 6: commit** `feat: adiciona leitura do resultado do p2 com protecao contra saida antiga`.

### Tarefa 4: Portões de qualidade

**Arquivos:** `dags/anp/qualidade.py`, `tests/test_qualidade.py`.

**Interfaces (produz):**
- Padrões: `LINHAS_MINIMAS = 10_000`, `PRECO_MIN = 2.0`, `PRECO_MAX = 15.0`, `UFS_MINIMAS = 27`, `PRODUTOS_OBRIGATORIOS = frozenset({"GASOLINA", "ETANOL"})`
- `checar(tabela: pyarrow.Table, ano: int, mes: int, linhas_minimas: int = LINHAS_MINIMAS) -> list[str]`: lista de falhas legíveis (vazia = aprovado). Colunas usadas: `valor_venda`, `data_coleta`, `uf`, `produto`.
- `ler_parquet(s3, bucket: str, chave: str) -> pyarrow.Table`

- [ ] **Passo 1: testes que falham** (tabelas montadas no teste): `test_aprova_tabela_boa`; `test_poucas_linhas`; `test_preco_fora_da_faixa` (um 0.5 e um 40.0 -> falha com os dois valores); `test_data_fora_do_mes`; `test_uf_faltando` (26 UFs); `test_produto_obrigatorio_faltando` (sem ETANOL); `test_linhas_minimas_sobrescrito` (`linhas_minimas=10_000_000` reprova uma tabela boa, usado na demonstração de falha).
- [ ] **Passos 2-4:** rodar (FAIL), implementar, rodar (PASS).
- [ ] **Passo 5: calibrar.** Rodar `checar` localmente sobre o Parquet real de out/2026 (gerado na Tarefa 0 ou baixado do P2). Esperado: lista vazia. Se não for, ajustar as faixas e registrar o motivo no `CLAUDE.md`.
- [ ] **Passo 6: commit** `feat: adiciona portoes de qualidade do parquet`.

### Tarefa 5: Publicação no Athena e alertas

**Arquivos:** `dags/anp/publicacao.py`, `dags/anp/alertas.py`, `tests/test_publicacao.py`, `tests/test_alertas.py`.

**Interfaces (produz):**
- `sql_adicionar_particao(database: str, tabela: str, bucket: str, ano: int, mes: int) -> str` -> `ALTER TABLE {database}.{tabela} ADD IF NOT EXISTS PARTITION (ano='{ano}', mes='{mes:02d}') LOCATION 's3://{bucket}/processado/ano={ano}/mes={mes:02d}/'`
- `executar_athena(athena, sql: str, workgroup: str, database: str, timeout_s: int = 120, espera_s: float = 2.0) -> str`: devolve o id da execução; `RuntimeError` com o `StateChangeReason` se der FAILED ou CANCELLED; `TimeoutError` se passar do tempo
- `montar_alerta(dag_id: str, task_id: str, data_logica: str, erro: str) -> tuple[str, str]`: assunto com no máximo 100 caracteres, começando por `Falha no Airflow:`
- `notificar_falha(sns, topico_arn: str, contexto: dict) -> None`: extrai `dag_id`, `task_id`, `logical_date` e a exceção do contexto do Airflow e publica

- [ ] **Passo 1: testes que falham:** `test_sql_particao_exato` (string exata para 2025-01); `test_athena_sucesso` (cliente falso: RUNNING -> SUCCEEDED); `test_athena_falha_com_motivo`; `test_athena_timeout` (`timeout_s=0`); `test_assunto_ate_100_caracteres`; `test_notificar_publica_no_topico`.
- [ ] **Passos 2-4:** rodar (FAIL), implementar, rodar (PASS).
- [ ] **Passo 5: commit** `feat: adiciona publicacao de particao no athena e alertas de falha`.

### Tarefa 6: O DAG, os testes de integridade e o CI

**Arquivos:** `dags/anp/calendario.py`, `dags/anp_precos_mensal.py`, `tests/test_calendario.py`, `tests/test_dag_integridade.py`, `.github/workflows/ci.yml`.

**Interfaces:**
- Produz `mes_de_referencia(data_logica: date | datetime) -> tuple[int, int]`: o mês anterior ao da data lógica (2025-02-15 -> (2025, 1); 2025-01-15 -> (2024, 12)).
- DAG `anp_precos_mensal`, com a API TaskFlow do Airflow 3 (`airflow.sdk`; conferir os imports na versão fixada): `schedule="0 9 15 * *"` (fuso `America/Sao_Paulo`), `start_date=2025-02-01`, `catchup=False` (o histórico é carregado por backfill explícito, não automaticamente ao ativar o DAG), `max_active_runs=1`, `default_args`: `retries=3`, `retry_exponential_backoff=True`, `retry_delay=5 min`, `execution_timeout=10 min`, `on_failure_callback` chamando `notificar_falha`. `params={"linhas_minimas": 10000}`.
- Tarefas, em sequência: `descobrir_url` -> `enviar_para_p2` (XCom: `desde` em ISO) -> `aguardar_resultado` (sensor, `mode="reschedule"`, `poke_interval=30`, `timeout=900`; falha sem retentativa (`AirflowFailException`) com os motivos se rejeitado) -> `checar_qualidade` (falha sem retentativa com a lista de falhas) -> `publicar_particao`.

- [ ] **Passo 1: testes que falham:** `test_calendario.py` com os dois casos acima. `test_dag_integridade.py` com `pytest.importorskip("airflow")`: DagBag sem erros de import; o DAG existe; 5 tarefas na ordem acima; `max_active_runs == 1`; `catchup is False`; retries == 3; schedule == `"0 9 15 * *"`.
- [ ] **Passo 2: rodar** no Windows. Esperado: calendário FAIL; integridade SKIPPED (sem Airflow, o esperado).
- [ ] **Passo 3:** implementar o calendário e o DAG.
- [ ] **Passo 4: CI** com os jobs `python` (instala `requirements-dev.txt` + `apache-airflow==<versão fixada>` com o arquivo de constraints oficial para Python 3.12; ruff check, ruff format --check, pytest -v **sem skips de integridade**), `compose` (`docker compose -f docker/docker-compose.yml config`, depois da Tarefa 7) e `terraform` (fmt -check, init -backend=false, validate, depois da Tarefa 8). `permissions: contents: read`, sem credenciais AWS, `hashicorp/setup-terraform@v4`.
- [ ] **Passo 5:** commit `feat: adiciona dag mensal da anp e ci`; push só com autorização; conferir o CI verde, com os testes de integridade rodando (e não pulados).

### Tarefa 7: O Airflow em containers

**Arquivos:** `docker/Dockerfile`, `docker/requirements.txt`, `docker/docker-compose.yml`, `docker/.env.example`.

**Decisões:** imagem `apache/airflow:<3.x fixada>-python3.12` estendida com `requirements.txt` (boto3, pyarrow, requests), instalados com as constraints. Serviços: `postgres` (16, volume nomeado), `airflow-init` (migração + usuário admin), `api-server` (porta `127.0.0.1:8080:8080`), `scheduler` e `dag-processor`, todos com `restart: unless-stopped`; sem Celery, Redis ou triggerer. `dags/` montado como volume somente leitura. Executor `LocalExecutor`. A senha do admin vem do `.env` (preenchido pelo user_data a partir do SSM). O mecanismo de autenticação fica a critério da versão (SimpleAuthManager ou FAB), documentado no `CLAUDE.md`. `.env.example` com as variáveis `ANP_*` das restrições globais e valores fictícios.

- [ ] **Passo 1:** escrever os arquivos.
- [ ] **Passo 2: validar** com o job `compose` do CI (`docker compose config` sem erro). O teste real é na Tarefa 8.
- [ ] **Passo 3: commit** `build: adiciona airflow em docker compose`.

### Tarefa 8: Infra do P4 em Terraform e acesso

**Arquivos:** `infra/*.tf`, `infra/user_data.sh.tftpl`, `scripts/abrir_airflow.ps1`, `scripts/atualizar_dags.ps1`.

**Decisões:** variáveis `bucket_p2` e `topico_alertas_arn_p2` (dos outputs do P2), `regiao = "us-east-2"`, `repo_url`. VPC e subnet padrão, IP público e nenhum ingresso. Role da instância: `AmazonSSMManagedInstanceCore`; S3 `PutObject` em `entrada/*`; `GetObject` em `processado/*` e `rejeitado/*`; `ListBucket` com prefixos `processado/` e `rejeitado/`; leitura e escrita em `athena-resultados/*`; Glue (`GetDatabase`, `GetTable`, `GetPartition*`, `BatchCreatePartition`, `CreatePartition`) restrito ao database/tabela; Athena (`StartQueryExecution`, `GetQueryExecution`) restrito ao workgroup; `sns:Publish` no tópico. `random_password` -> `aws_ssm_parameter` SecureString `/aws-airflow-orquestracao-anp/airflow-admin-senha`. Scheduler com role própria (`ec2:StopInstances` só nesta instância). `aws_glue_catalog_table` com as 15 colunas do Parquet do P2 (13 `string`, `data_coleta date`, `valor_venda` e `valor_compra double`), partições `ano` e `mes` string. `aws_athena_workgroup` com o limite de 100 MB e `force_destroy = true`. user_data: instala docker e o plugin compose, habilita o docker no boot, clona `repo_url` em `/opt/airflow-anp`, gera `docker/.env` (variáveis `ANP_*` + senha lida do SSM) e roda `docker compose up -d --build`. Outputs: `instance_id`, `comando_port_forward`.

Scripts: `abrir_airflow.ps1` (liga a instância se estiver parada, espera ela ficar `running` e o SSM `Online`, e abre o port forwarding `AWS-StartPortForwardingSession` 8080 -> 8080). `atualizar_dags.ps1` (`aws ssm send-command` com `git pull` em `/opt/airflow-anp`).

- [ ] **Passo 1:** pré-requisito no Windows: instalar o Session Manager plugin do AWS CLI e conferir com `session-manager-plugin --version`.
- [ ] **Passo 2:** push do repositório (com autorização), porque a instância clona o repositório público.
- [ ] **Passo 3:** `terraform fmt`, `validate` e `plan -out=tfplan`. Conferir com o Pedro (e colar no chat de revisão) antes do apply.
- [ ] **Passo 4:** `terraform apply tfplan` (com autorização).
- [ ] **Passo 5: verificar o acesso.** `abrir_airflow.ps1` -> `http://localhost:8080` abre o login; o DAG `anp_precos_mensal` aparece sem erro de import.
- [ ] **Passo 6: verificar o religamento** (ponto de atenção 5). Parar a instância, rodar `abrir_airflow.ps1` de novo: o Airflow volta sozinho, sem intervenção.
- [ ] **Passo 7: commit** `feat: adiciona infraestrutura do airflow em terraform`.

### Tarefa 9: Primeira execução ponta a ponta (out/2026)

- [ ] Disparar manualmente uma execução com data lógica 2026-11-15.
- [ ] Esperado: 5 tarefas verdes; Parquet novo no P2; partição `ano='2026', mes='10'` visível no Athena.
- [ ] Consulta de negócio no Athena (preço médio de venda por UF e produto em out/2026), salva em `docs/consultas/preco_medio_uf.sql`.
- [ ] Commit `docs: adiciona consulta de negocio no athena`.

### Tarefa 10: Backfill do histórico

- [ ] Rodar o backfill de 2025-02-15 a 2026-11-15 com no máximo 1 execução ativa (comando de backfill da versão fixada; conferir a sintaxe na documentação dela).
- [ ] Acompanhar. Para cada falha: ler o motivo, decidir com o Pedro se é um dado ruim real (fica registrado como evidência) ou um bug (corrigir com teste, commit `fix:`, rodar de novo só aquele mês).
- [ ] Esperado: 22 execuções concluídas; `SELECT ano, mes, count(*) ... GROUP BY 1,2` no Athena com até 22 partições.
- [ ] Registrar os números (tempo total, linhas por mês, falhas e motivos) no `CLAUDE.md`, para o README.

### Tarefa 11: Demonstrações de falha (evidência)

- [ ] **Portão de qualidade:** disparar out/2026 com `params={"linhas_minimas": 10000000}` -> `checar_qualidade` falha, `publicar_particao` não roda, chega o e-mail do alerta.
- [ ] **Rejeição pelo P2:** se o backfill já tiver produzido uma rejeição real, usar ela; senão, registrar que não houve, sem forçar.
- [ ] Prints e GIF (os listados na seção 8 do design).

### Tarefa 12: Runbook, README e publicação

- [ ] `docs/runbook.md`: mês não publicado; arquivo rejeitado pelo P2; qualidade reprovada; evento na DLQ do P2; instância desligada no meio da execução; disco cheio; senha do admin (como ler do SSM).
- [ ] README no padrão do P2 (topo com stack e destaques, Mermaid, decisões, evidências, problemas e lições, limitações, como rodar, deploy/destroy, outros projetos), incluindo a nota de infraestrutura destruída.
- [ ] Commit `docs: adiciona readme e runbook`; conferência pré-push (autores, busca por Co-Authored/Claude/Anthropic, `git ls-files`); push com autorização; CI verde.

### Tarefa 13: Destroy e conferência

- [ ] `terraform plan -destroy` e `terraform destroy` no **P4** (com autorização).
- [ ] Depois, `terraform destroy` no **P2** (com autorização).
- [ ] Conferir em us-east-2: nenhuma instância, volume, parâmetro SSM, schedule, workgroup, database do Glue, bucket, Lambda, fila, tópico ou alarme. Os dois states vazios.
- [ ] No dia seguinte: conferir os créditos em settings.aws.com -> Faturamento.
