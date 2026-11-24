# P4 - Orquestração com Airflow na AWS: documento de design

Status: rascunho para aprovação (23/11/2026)
Repositório proposto: `aws-airflow-orquestracao-anp`
Autor dos commits: Pedro Henrique (sem co-autoria, sem menção a IA)

---

## 1. Intenção e porquê

Quem abrir o P4 precisa concluir que o autor trabalha em **nível pleno/sênior**, e não só que sabe usar o Airflow. Isso aparece em:

- pipelines que **aguentam reprocessamento** (backfill sem duplicar dados);
- **dependências entre sistemas** tratadas explicitamente: o DAG espera, verifica e reage;
- **qualidade de dados como portão**: nada é publicado se não passar;
- **operação**: alertas, timeouts, runbook e controle de custo;
- **decisões documentadas com trade-offs**.

O P4 é a camada de orquestração sobre o que já existe. O P2 reage a arquivos; o P4 agenda, alimenta o P2, valida o resultado, publica para consulta e reprocessa o histórico.

## 2. Objetivos, não-objetivos e anti-objetivos

**Objetivos**
1. Um DAG mensal que, para cada mês: descobre a URL do arquivo da ANP, entrega em `entrada/` do P2, espera o resultado, roda checagens de qualidade e só então publica a partição no catálogo (Glue) para consulta no Athena.
2. Backfill de **jan/2025 a out/2026** (22 meses) executado pelo próprio Airflow, todos verdes ou com falhas explicadas.
3. Infra 100% em Terraform, com destroy completo no final.
4. Testes (pytest) e CI no GitHub Actions, sem credenciais AWS.
5. README com evidências: grade do Airflow com as 22 execuções, consulta de negócio no Athena, GIF do DAG, runbook e decisões.

**Não-objetivos**
- Airflow acessível publicamente ou ligado 24 horas.
- Alta disponibilidade, CeleryExecutor, Kubernetes ou MWAA.
- Dashboard (o P3 já cobre visualização).
- Séries anteriores a 2025 (só existem em ZIP semestral, com layout diferente).

**Anti-objetivos (falha mesmo que "funcione")**
- Interface do Airflow exposta à internet ou porta SSH aberta.
- Chaves de acesso AWS em arquivo, variável ou código: só role da instância.
- Gasto relevante de créditos por máquina esquecida ligada.
- Backfill que duplica dados ou aceita resultado antigo como se fosse novo.
- Tarefas "verdes" que publicam dados ruins.

## 3. Restrições

- **Conta:** projeto `portfolio-de-pedro` (nova experiência de projetos da AWS), plano gratuito, créditos (~US$ 140). Região **us-east-2**. SCP gerenciada nega quase tudo em us-east-1. EC2, SSM, Glue, Athena, S3, SNS, SQS, Lambda, EventBridge Scheduler e IAM estão liberados.
- **Tipos de instância elegíveis:** t3.micro, t3.small, t4g.micro, t4g.small, c7i-flex.large, m7i-flex.large.
- **Custo-alvo:** menos de US$ 10 dos créditos no projeto todo.
- **Ambiente local:** Windows 11, PowerShell 5.1, VS Code, Python 3.12, Terraform 1.17.1, AWS CLI 2 (`aws login`).
- **Regras de trabalho:** as mesmas do P2 (explicar antes, Conventional Commits em português, nada de push/apply/destroy sem autorização, `Set-Content -Encoding ascii`).
- **Dependência:** o P2 (`aws-pipeline-eventos-anp`) precisa estar implantado durante o P4.

## 4. Abordagens consideradas

**O que orquestrar**
| Abordagem | Trade-off | Decisão |
|---|---|---|
| A. Orquestrar o ecossistema (ANP -> P2 -> qualidade -> catálogo/Athena) + backfill | Mais peças móveis e dependência do P2; em troca, os sinais de sênior surgem do problema real e conta uma história de evolução | **Escolhida pelo Pedro** |
| B. Pipeline novo e independente dentro do Airflow | Mais simples; tende a parecer "mais um pipeline", com sinais de sênior forçados | Descartada |

**Onde rodar o Airflow** (escolha técnica, vetável)
| Abordagem | Trade-off | Decisão |
|---|---|---|
| EC2 + Docker Compose (LocalExecutor + Postgres) | Barato, controle total, mostra infra; operação manual | **Escolhida** |
| MWAA (Airflow gerenciado) | Zero operação; custo alto e contínuo, inviável nos créditos | Descartada |
| Airflow só local (Docker Desktop) | Custo zero; não demonstra infra nem integração real com AWS | Descartada |

**Tempo de máquina ligada**
| Abordagem | Custo estimado | Decisão |
|---|---|---|
| 24/7 | ~US$ 76/mês | Descartada |
| Só nas sessões de trabalho + desligamento noturno automático + backfill provando o agendamento | < US$ 10 no projeto | **Escolhida pelo Pedro** |

## 5. Arquitetura

```
 ANP (gov.br) ──► [Airflow na EC2] ──► s3://<bucket-P2>/entrada/precos-gasolina-etanol-AAAA-MM.csv
                        │                         │ (evento S3, P2)
                        │                         ▼
                        │                 Lambda P2 ──► processado/ano=AAAA/mes=MM/*.parquet
                        │                         └──► rejeitado/*.csv + *.json
                        │◄── sensor (espera processado/ ou rejeitado/, com LastModified > upload)
                        ├──► checagens de qualidade (lê o Parquet do S3)
                        ├──► publica partição (Athena: ALTER TABLE ADD IF NOT EXISTS PARTITION)
                        └──► alerta em falha (SNS do P2 -> e-mail)

 EventBridge Scheduler ──► StopInstances todo dia 23:00 (trava de custo)
 Acesso: SSM Session Manager + port forwarding (sem SSH, sem porta aberta)
```

### 5.1 Infra (Terraform do P4)
- **EC2** m7i-flex.large, Amazon Linux 2023, disco gp3 de 30 GB, na VPC padrão, subnet pública com IP público (necessário para saída à internet sem NAT).
- **Security group sem nenhuma regra de entrada**. Saída liberada.
- **IMDSv2 obrigatório** com `http_put_response_hop_limit = 2` (os containers do Airflow precisam alcançar a role da instância).
- **Instance profile** com: `AmazonSSMManagedInstanceCore`; S3 limitado ao bucket do P2 (PutObject em `entrada/*`, GetObject/ListBucket em `processado/*` e `rejeitado/*`, leitura/escrita no prefixo de resultados do Athena); Glue/Athena limitados ao database e workgroup do projeto; `sns:Publish` no tópico de alertas.
- **user_data**: instala Docker e o plugin Compose, clona o repositório (público) e sobe o Airflow.
- **EventBridge Scheduler** com role própria (`ec2:StopInstances` só nesta instância): para a máquina todo dia às 23:00 (America/Sao_Paulo).
- **Glue database** + **tabela externa** sobre `processado/` (Parquet, particionada por `ano`, `mes`).
- **Athena workgroup** com resultados em um prefixo do bucket e **limite de bytes por consulta** (trava de custo).
- **Senha do admin do Airflow** gerada e guardada no SSM Parameter Store (SecureString, chave gerenciada pela AWS).
- Logs locais da instância com rotação; nada de CloudWatch Agent (fora do escopo).

### 5.2 Airflow
- **Airflow 3.x** (última estável no momento da implementação, versão fixada), imagem oficial estendida com as dependências dos DAGs, instaladas com o arquivo de constraints oficial.
- **LocalExecutor + Postgres** via Docker Compose, adaptado do compose oficial, sem Celery/Redis.
- **Conexão AWS sem chaves**: `aws_default` usando a role da instância.
- **Deploy dos DAGs**: `git pull` na instância, disparado manualmente via SSM (`aws ssm send-command`). O CI não faz deploy (sem credenciais).

### 5.3 DAG `anp_precos_mensal`
- `schedule`: mensal, no dia 15 do mês seguinte (processa o mês anterior; a ANP publica no início do mês seguinte).
- `start_date` 2025-01-01, `catchup` ligado, `max_active_runs = 1` (backfill em ordem, sem sobrecarregar o P2).
- `retries` com backoff exponencial e `execution_timeout` em todas as tarefas.
- `on_failure_callback` publica no SNS do P2 (mesmo e-mail).

| Tarefa | O que faz | Detalhes de nível sênior |
|---|---|---|
| `descobrir_url` | Lê a página da série histórica da ANP e encontra a URL do mês | Os nomes da ANP são inconsistentes (erro de digitação "cados", arquivo sem extensão): mapear por mês/ano a partir dos links, nunca montar a URL por padrão. Se o mês ainda não foi publicado, falha com mensagem clara e as retentativas tentam mais tarde. |
| `enviar_para_p2` | Baixa em streaming, confere o básico (tamanho > 0, cabeçalho de CSV) e envia para `entrada/precos-gasolina-etanol-AAAA-MM.csv` | **Nome padronizado** = idempotência por nome do P2 funciona no backfill (reprocessar sobrescreve, não duplica). Guarda o horário do upload em XCom. |
| `aguardar_resultado` | Sensor em modo reschedule/deferrable: espera `processado/ano=AAAA/mes=MM/<nome>.parquet` **ou** `rejeitado/<nome>.json` | Só aceita objetos com `LastModified` **posterior ao upload** (senão um Parquet antigo de execução anterior seria aceito). Se cair em `rejeitado/`, falha mostrando os motivos do JSON. Timeout de ~15 min (cobre as retentativas do P2). |
| `checar_qualidade` | Lê o Parquet e valida | Ex.: linhas acima de um mínimo; `valor_venda` em faixa plausível; todas as `data_coleta` dentro do mês; 27 UFs presentes; produtos esperados. Resultado das checagens logado de forma estruturada. Falha **bloqueia** a publicação. |
| `publicar_particao` | `ALTER TABLE ... ADD IF NOT EXISTS PARTITION` via Athena | Idempotente. Só roda se a qualidade passou: consumidores no Athena nunca veem mês reprovado. |

### 5.4 Backfill
- Executado pelo próprio Airflow (CLI ou interface de backfill da versão escolhida) para jan/2025 a out/2026.
- Falhas reais de layout ou dados em meses antigos são **bem-vindas**: viram evidência de que os portões funcionam e vão para o README com a explicação.

## 6. Testes e CI

- **Unitários** (sem AWS): parser de URLs com HTML salvo da página da ANP como fixture; checagens de qualidade com Parquets pequenos; lógica do sensor (aceitar/rejeitar por `LastModified`, ler motivos do JSON) com S3 falso.
- **Integridade dos DAGs**: `DagBag` sem erros de import, sem ciclos, schedule/catchup/max_active_runs/retries conferidos por teste.
- **CI** (GitHub Actions, `permissions: contents: read`, sem credenciais AWS): ruff, ruff format --check, pytest (Airflow instalado com constraints) e terraform fmt/validate.

## 7. Operação e custo

- **Runbook** (`docs/runbook.md`): mês não publicado pela ANP; arquivo rejeitado pelo P2; qualidade reprovada; evento na DLQ do P2; instância parada no meio de uma execução (pelo agendamento noturno); disco cheio; senha do admin.
- **Travas de custo**: desligamento automático às 23:00; limite de bytes no workgroup do Athena; destroy no final (P4 e P2).
- **Ordem do destroy**: P4 primeiro (tabela/workgroup usam o bucket do P2), depois P2.

## 8. Evidências para o README

1. Grade do Airflow com as 22 execuções mensais.
2. Grafo do DAG com as tarefas.
3. Consulta de negócio no Athena (ex.: preço médio da gasolina por UF por mês) com o resultado.
4. GIF de uma execução, incluindo um portão de qualidade barrando um mês.
5. E-mail de alerta de falha do DAG.
6. CI verde.

## 9. Etapas sugeridas

1. Reimplantar o P2 (empacotar, plan, apply) e confirmar a assinatura de e-mail.
2. Infra do P4: EC2, SSM, SG, role, scheduler de desligamento. Validar acesso via SSM.
3. Airflow no ar via Compose; acesso à interface via port forwarding.
4. DAG + testes locais (TDD nas funções puras).
5. Uma execução ponta a ponta de um mês.
6. Glue + Athena + publicação de partição.
7. Backfill completo.
8. Demonstração de falhas (qualidade, rejeição) para evidência.
9. CI, README, runbook, prints.
10. Destroy do P4 e do P2; conferência de que nada sobrou.

## 10. Decidido x deixado para quem implementa

**Decidido pelo Pedro:** abordagem A; máquina ligada só nas sessões, com desligamento automático e backfill como prova do agendamento; foco em evidência no README.

**Escolhas técnicas (vetáveis):** EC2 m7i-flex.large + Docker Compose + LocalExecutor + Postgres; Airflow 3.x; SSM sem portas abertas; desligamento às 23:00; reaproveitar o SNS do P2 para alertas; deploy dos DAGs via `git pull` disparado por SSM; P2 implantado a partir do próprio repositório (não como módulo); nome do repositório `aws-airflow-orquestracao-anp`; dia 15 como dia de agendamento.

**Para quem implementa decidir:** versão exata do Airflow 3.x e do Postgres; faixas exatas das checagens de qualidade (calibrar com os dados reais); como provocar a falha de qualidade para a demonstração; ajustes no compose oficial para caber bem em 8 GB.
