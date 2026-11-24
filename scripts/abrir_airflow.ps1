<#
.SYNOPSIS
  Liga a EC2 do Airflow (se estiver parada), espera o SSM ficar Online e abre o
  port forwarding: a interface fica em http://localhost:8080.

.DESCRIPTION
  Requer: AWS CLI v2 logado (aws login) e o Session Manager plugin instalado.
  O ID da instancia vem do output do Terraform (infra/). Nenhum IP e mostrado.
  Encerrar o port forwarding: Ctrl+C. A instancia continua ligada ate as 23:00
  (desligamento automatico) ou ate um "aws ec2 stop-instances".
#>
param(
    [string]$Regiao = "us-east-2",
    [int]$PortaLocal = 8080,
    [int]$EsperaMaximaSsmSegundos = 600
)

$ErrorActionPreference = "Stop"
$raiz = Split-Path -Parent $PSScriptRoot

$instancia = terraform "-chdir=$raiz\infra" output -raw instance_id
if ($LASTEXITCODE -ne 0 -or -not $instancia) { throw "Nao consegui ler o output instance_id do Terraform." }

$estado = aws ec2 describe-instances --region $Regiao --instance-ids $instancia `
    --query "Reservations[0].Instances[0].State.Name" --output text
Write-Host "Instancia $instancia esta '$estado'."

if ($estado -eq "stopping") {
    Write-Host "Esperando terminar de parar..."
    aws ec2 wait instance-stopped --region $Regiao --instance-ids $instancia
    $estado = "stopped"
}
if ($estado -eq "stopped") {
    Write-Host "Ligando..."
    aws ec2 start-instances --region $Regiao --instance-ids $instancia | Out-Null
}
if ($estado -ne "running") {
    aws ec2 wait instance-running --region $Regiao --instance-ids $instancia
    if ($LASTEXITCODE -ne 0) { throw "A instancia nao chegou a 'running'." }
    Write-Host "Instancia running."
}

Write-Host "Esperando o agente SSM ficar Online..."
$limite = (Get-Date).AddSeconds($EsperaMaximaSsmSegundos)
do {
    $ping = aws ssm describe-instance-information --region $Regiao `
        --filters "Key=InstanceIds,Values=$instancia" `
        --query "InstanceInformationList[0].PingStatus" --output text
    if ($ping -eq "Online") { break }
    if ((Get-Date) -gt $limite) { throw "O SSM nao ficou Online em $EsperaMaximaSsmSegundos s (ultimo status: $ping)." }
    Start-Sleep -Seconds 10
} while ($true)

Write-Host "SSM Online. Abrindo http://localhost:$PortaLocal (Ctrl+C para encerrar)."
Write-Host "Na primeira subida, o Airflow leva alguns minutos (build da imagem) ate responder."
aws ssm start-session --region $Regiao --target $instancia `
    --document-name AWS-StartPortForwardingSession `
    --parameters "portNumber=8080,localPortNumber=$PortaLocal"
