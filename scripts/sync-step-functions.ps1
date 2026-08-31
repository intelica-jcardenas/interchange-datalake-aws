# sync-step-functions.ps1
# Sincroniza definiciones ASL de Step Functions desde AWS al repo local.
#
# OJO -Environment: step-functions\{visa,mastercard}\asl.json y config.json NO
# distinguen de que ambiente vinieron -- sincronizar desde "prd" sobreescribe
# esos mismos archivos con datos de PRD sin dejar rastro del origen. Usar
# -Environment prd solo para inspeccion puntual, y volver a sincronizar desde
# DEV despues si hace falta restaurar el reflejo habitual del repo.
#
# Prerequisitos (segun -Environment):
#   dev: aws sso login --profile itx-dev  ; $env:AWS_PROFILE = "itx-dev"
#   prd: aws sso login --profile itx-prd  ; $env:AWS_PROFILE = "itx-prd"
#
# Uso:
#   .\scripts\sync-step-functions.ps1                       # sincroniza todos desde DEV (default)
#   .\scripts\sync-step-functions.ps1 -Environment prd      # sincroniza todos desde PRD (pide confirmacion extra)
#   .\scripts\sync-step-functions.ps1 -SF visa              # solo Visa
#   .\scripts\sync-step-functions.ps1 -SF mc                # solo Mastercard

param(
    [ValidateSet("all","visa","mc")]
    [string]$SF = "all",
    [ValidateSet("dev","prd")]
    [string]$Environment = "dev"
)

if ($Environment -ne "dev") {
    Write-Host "ADVERTENCIA: sincronizando desde '$Environment', no 'dev'." -ForegroundColor Red
    Write-Host "Esto sobreescribe asl.json/config.json locales (que normalmente reflejan DEV) con datos de '$Environment'." -ForegroundColor Red
    $EnvConfirm = Read-Host "Escribi CONFIRMAR para continuar"
    if ($EnvConfirm -ne "CONFIRMAR") {
        Write-Host "Cancelado." -ForegroundColor Yellow
        exit 0
    }
    Write-Host ""
}

$RepoRoot = Split-Path -Parent $PSScriptRoot
$Region   = "eu-south-2"

$AccountId = (aws sts get-caller-identity --query Account --output text 2>$null)
if (-not $AccountId) {
    Write-Host "ERROR: No se pudo obtener el Account ID. Verifica la sesion AWS." -ForegroundColor Red
    exit 1
}

$AllSFs = [ordered]@{
    "visa" = @{ Name="itl-0004-itx-$Environment-intchg-02-sfn-vi"; Dir="step-functions\visa" }
    "mc"   = @{ Name="itl-0004-itx-$Environment-intchg-02-sfn-mc"; Dir="step-functions\mastercard" }
}

$ToSync = [ordered]@{}
foreach ($Key in $AllSFs.Keys) {
    if ($SF -eq "all" -or $Key -eq $SF) {
        $ToSync[$Key] = $AllSFs[$Key]
    }
}

Write-Host "Step Functions a sincronizar: $($ToSync.Count)" -ForegroundColor White
$ToSync.Keys | ForEach-Object { Write-Host "  $($ToSync[$_].Name)" }
Write-Host ""

$Results = New-Object System.Collections.ArrayList

foreach ($Key in $ToSync.Keys) {
    $Meta     = $ToSync[$Key]
    $Arn      = "arn:aws:states:${Region}:${AccountId}:stateMachine:$($Meta.Name)"
    $LocalDir = Join-Path $RepoRoot $Meta.Dir

    Write-Host "[$Key - $($Meta.Name)]" -ForegroundColor Cyan

    $RawJson = aws stepfunctions describe-state-machine --state-machine-arn $Arn --output json 2>$null

    if (-not $RawJson) {
        Write-Host "  SKIP - State machine no encontrada en AWS" -ForegroundColor Yellow
        $null = $Results.Add([pscustomobject]@{ SF = $Key; Name = $Meta.Name; Status = "NOT FOUND" })
        Write-Host ""
        continue
    }

    $Data = $RawJson | ConvertFrom-Json

    if (-not (Test-Path $LocalDir)) {
        New-Item -ItemType Directory -Path $LocalDir -Force | Out-Null
    }

    # ASL definition -> asl.json (formateado)
    $AslPath = Join-Path $LocalDir "asl.json"
    $Data.definition | ConvertFrom-Json | ConvertTo-Json -Depth 50 | Out-File -FilePath $AslPath -Encoding utf8 -Force
    Write-Host "  asl.json actualizado"

    # Metadata (sin definition) -> config.json
    $ConfigPath = Join-Path $LocalDir "config.json"
    $Data | Select-Object -ExcludeProperty definition | ConvertTo-Json -Depth 10 | Out-File -FilePath $ConfigPath -Encoding utf8 -Force
    Write-Host "  config.json actualizado"

    $null = $Results.Add([pscustomobject]@{ SF = $Key; Name = $Meta.Name; Status = "OK" })
    Write-Host ""
}

Write-Host "========== Resumen ==========" -ForegroundColor White
$Results | Format-Table -AutoSize

$ok   = ($Results | Where-Object { $_.Status -eq "OK" }).Count
$skip = ($Results | Where-Object { $_.Status -ne "OK" }).Count
if ($skip -eq 0) {
    Write-Host "Completado: $ok OK" -ForegroundColor Green
} else {
    Write-Host "Completado: $ok OK, $skip con advertencias/errores" -ForegroundColor Yellow
}
