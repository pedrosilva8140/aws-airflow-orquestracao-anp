<#
.SYNOPSIS
  Atualiza os DAGs na EC2: "git pull --ff-only" em /opt/airflow-anp via SSM.

.DESCRIPTION
  O dags/ e montado somente leitura nos containers: o dag-processor le a versao nova
  sozinho, sem reiniciar nada. Mudancas em docker/ (imagem, compose) exigem
  "docker compose up -d --build" na instancia.
  A instancia precisa estar ligada (use scripts/abrir_airflow.ps1 antes).
#>
param(
    [string]$Regiao = "us-east-2"
)

$ErrorActionPreference = "Stop"
$raiz = Split-Path -Parent $PSScriptRoot

$instancia = terraform "-chdir=$raiz\infra" output -raw instance_id
if ($LASTEXITCODE -ne 0 -or -not $instancia) { throw "Nao consegui ler o output instance_id do Terraform." }

# Parametros em arquivo JSON: evita os problemas de aspas do PowerShell 5.1 com o AWS CLI.
$parametros = Join-Path $env:TEMP "atualizar_dags_parametros.json"
'{"commands":["cd /opt/airflow-anp && git pull --ff-only && git log -1 --oneline"]}' |
    Set-Content -Encoding ascii -Path $parametros

try {
    $comando = aws ssm send-command --region $Regiao --instance-ids $instancia `
        --document-name AWS-RunShellScript `
        --comment "atualizar dags do P4" `
        --parameters "file://$parametros" `
        --query "Command.CommandId" --output text
    if ($LASTEXITCODE -ne 0) { throw "send-command falhou (a instancia esta ligada e Online no SSM?)." }
    Write-Host "Comando $comando enviado. Esperando..."

    aws ssm wait command-executed --region $Regiao --command-id $comando --instance-id $instancia
    $status = aws ssm get-command-invocation --region $Regiao --command-id $comando `
        --instance-id $instancia --query "Status" --output text
    aws ssm get-command-invocation --region $Regiao --command-id $comando `
        --instance-id $instancia --query "[StandardOutputContent, StandardErrorContent]" --output text
    if ($status -ne "Success") { throw "git pull terminou com status $status." }
    Write-Host "DAGs atualizados."
}
finally {
    Remove-Item -ErrorAction SilentlyContinue $parametros
}
