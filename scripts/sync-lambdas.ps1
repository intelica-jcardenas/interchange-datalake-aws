# sync-lambdas.ps1
# Sincroniza codigo fuente y configuracion de Lambdas desde AWS al repo local.
#
# OJO -Environment: los archivos locales (config.json, env-vars.json, src/) NO
# distinguen de que ambiente vinieron -- son "la ultima foto sincronizada", y
# el repo los usa en todos lados asumiendo que reflejan DEV. Sincronizar desde
# "prd" sobreescribe esos mismos archivos con datos de PRD sin dejar rastro de
# cual era el origen. Usar -Environment prd solo para inspeccion puntual, y
# volver a correr sin el parametro (o con -Environment dev) para restaurar el
# reflejo de DEV despues.
#
# Prerequisitos (segun -Environment):
#   dev: aws sso login --profile itx-dev  ; $env:AWS_PROFILE = "itx-dev"
#   prd: aws sso login --profile itx-prd  ; $env:AWS_PROFILE = "itx-prd"
#
# Uso:
#   .\scripts\sync-lambdas.ps1                          # sincroniza todos desde DEV (default)
#   .\scripts\sync-lambdas.ps1 -Environment prd         # sincroniza todos desde PRD (pide confirmacion extra)
#   .\scripts\sync-lambdas.ps1 -Group mc                # solo Mastercard
#   .\scripts\sync-lambdas.ps1 -Group vi                # solo Visa
#   .\scripts\sync-lambdas.ps1 -Group general           # solo generales (router, unzip, archive-file)
#   .\scripts\sync-lambdas.ps1 -Group rules-refresh      # solo rules-refresh
#   .\scripts\sync-lambdas.ps1 -Lambda mc-interpreter   # uno especifico

param(
    [ValidateSet("all","mc","vi","general","rules-refresh")]
    [string]$Group = "all",
    [string]$Lambda = "",
    [ValidateSet("dev","prd")]
    [string]$Environment = "dev"
)

if ($Environment -ne "dev") {
    Write-Host "ADVERTENCIA: sincronizando desde '$Environment', no 'dev'." -ForegroundColor Red
    Write-Host "Esto sobreescribe config.json/env-vars.json/src/ locales (que normalmente reflejan DEV) con datos de '$Environment'." -ForegroundColor Red
    $EnvConfirm = Read-Host "Escribi CONFIRMAR para continuar"
    if ($EnvConfirm -ne "CONFIRMAR") {
        Write-Host "Cancelado." -ForegroundColor Yellow
        exit 0
    }
    Write-Host ""
}

$RepoRoot = Split-Path -Parent $PSScriptRoot
$TempDir  = Join-Path $env:TEMP "lambda-sync"
$Prefix   = "itl-0004-itx-$Environment-intchg-02-lmbd"

# sufijo AWS => grupo, directorio local
# Formato: "sufijo" = @{ Group="grupo"; Dir="ruta\relativa" }
$AllLambdas = [ordered]@{
    # Generales
    "router"            = @{ Group="general"; Dir="lambdas\router" }
    "unzip"             = @{ Group="general"; Dir="lambdas\unzip" }
    "archive-file"      = @{ Group="general"; Dir="lambdas\archive-file" }
    # Visa
    "vi-transform"      = @{ Group="vi"; Dir="lambdas\visa\transform" }
    "vi-extract"        = @{ Group="vi"; Dir="lambdas\visa\extract" }
    "vi-clean"          = @{ Group="vi"; Dir="lambdas\visa\clean" }
    "vi-store"          = @{ Group="vi"; Dir="lambdas\visa\store" }
    "vi-ardef"          = @{ Group="vi"; Dir="lambdas\visa\ardef" }
    "vi-exchange-rates" = @{ Group="vi"; Dir="lambdas\visa\exchange-rates" }
    # Mastercard
    "mc-interpreter"    = @{ Group="mc"; Dir="lambdas\mastercard\interpreter" }
    "mc-transform"      = @{ Group="mc"; Dir="lambdas\mastercard\transform" }
    "mc-extract"        = @{ Group="mc"; Dir="lambdas\mastercard\extract" }
    "mc-clean"          = @{ Group="mc"; Dir="lambdas\mastercard\clean" }
    "mc-store"          = @{ Group="mc"; Dir="lambdas\mastercard\store" }
    "mc-iar"            = @{ Group="mc"; Dir="lambdas\mastercard\iar" }
    "mc-exchange-rates" = @{ Group="mc"; Dir="lambdas\mastercard\exchange-rates" }
    # Lambda definitivo (creado por el equipo de infra 2026-08-17,
    # reemplaza a itl-...-lmbd-test-1 -- ver lambdas\rules-refresh\README.md).
    # Sigue sobre el rol compartido itl-...-lmbd-vi-role (sin rol dedicado
    # todavia).
    "rules-refresh"     = @{ Group="rules-refresh"; Dir="lambdas\rules-refresh" }
}

# Validar -Lambda si se paso
if ($Lambda -ne "" -and -not $AllLambdas.Contains($Lambda)) {
    Write-Host "ERROR: Lambda '$Lambda' no reconocido. Opciones:" -ForegroundColor Red
    $AllLambdas.Keys | ForEach-Object { Write-Host "  $_" }
    exit 1
}

# Filtrar segun parametros
$ToSync = [ordered]@{}
foreach ($Suffix in $AllLambdas.Keys) {
    $Meta = $AllLambdas[$Suffix]
    if ($Lambda -ne "") {
        if ($Suffix -eq $Lambda) { $ToSync[$Suffix] = $Meta }
    } elseif ($Group -eq "all" -or $Meta.Group -eq $Group) {
        $ToSync[$Suffix] = $Meta
    }
}

if ($ToSync.Count -eq 0) {
    Write-Host "No hay Lambdas para sincronizar con los parametros indicados." -ForegroundColor Yellow
    exit 0
}

Write-Host "Lambdas a sincronizar: $($ToSync.Count)" -ForegroundColor White
$ToSync.Keys | ForEach-Object { Write-Host "  $_" }
Write-Host ""

if (Test-Path $TempDir) { Remove-Item $TempDir -Recurse -Force }
New-Item -ItemType Directory -Path $TempDir | Out-Null

$Results = New-Object System.Collections.ArrayList

foreach ($Suffix in $ToSync.Keys) {

    $Meta         = $ToSync[$Suffix]
    $FunctionName = "$Prefix-$Suffix"
    $LocalDir     = Join-Path $RepoRoot $Meta.Dir
    $LocalSrc     = Join-Path $LocalDir "src"
    $ZipPath      = Join-Path $TempDir "$Suffix.zip"
    $ExtractPath  = Join-Path $TempDir $Suffix

    Write-Host "[$Suffix]" -ForegroundColor Cyan

    # 1. Configuracion
    $ConfigJson = aws lambda get-function-configuration --function-name $FunctionName --output json 2>$null

    if (-not $ConfigJson) {
        Write-Host "  SKIP - Lambda no encontrado en AWS" -ForegroundColor Yellow
        $null = $Results.Add([pscustomobject]@{ Lambda = $Suffix; Group = $Meta.Group; Status = "NOT FOUND" })
        Write-Host ""
        continue
    }

    $Config = $ConfigJson | ConvertFrom-Json

    if (-not (Test-Path $LocalDir)) {
        New-Item -ItemType Directory -Path $LocalDir -Force | Out-Null
    }

    $ConfigJson | Out-File -FilePath (Join-Path $LocalDir "config.json") -Encoding utf8 -Force
    Write-Host "  config.json actualizado"

    $EnvVarsPath = Join-Path $LocalDir "env-vars.json"
    if ($Config.Environment -and $Config.Environment.Variables) {
        $VarCount = ($Config.Environment.Variables.PSObject.Properties | Measure-Object).Count
        @{ Variables = $Config.Environment.Variables } | ConvertTo-Json -Depth 5 | Out-File -FilePath $EnvVarsPath -Encoding utf8 -Force
        Write-Host "  env-vars.json actualizado ($VarCount vars)"
    } else {
        @{ Variables = @{} } | ConvertTo-Json | Out-File -FilePath $EnvVarsPath -Encoding utf8 -Force
        Write-Host "  env-vars.json creado (sin vars)"
    }

    # 2. Codigo fuente
    $Url = aws lambda get-function --function-name $FunctionName --query "Code.Location" --output text 2>$null

    if (-not $Url -or $Url -eq "None") {
        Write-Host "  WARN - No se pudo obtener URL del codigo" -ForegroundColor Yellow
        $null = $Results.Add([pscustomobject]@{ Lambda = $Suffix; Group = $Meta.Group; Status = "CONFIG OK / CODE FAILED" })
        Write-Host ""
        continue
    }

    Invoke-WebRequest -Uri $Url -OutFile $ZipPath -UseBasicParsing
    $SizeKB = [math]::Round((Get-Item $ZipPath).Length / 1KB, 1)
    Write-Host "  ZIP: $SizeKB KB"

    if (Test-Path $ExtractPath) { Remove-Item $ExtractPath -Recurse -Force }
    Expand-Archive -Path $ZipPath -DestinationPath $ExtractPath -Force

    if (-not (Test-Path $LocalSrc)) {
        New-Item -ItemType Directory -Path $LocalSrc -Force | Out-Null
    }

    Copy-Item -Path "$ExtractPath\*" -Destination $LocalSrc -Recurse -Force
    Write-Host "  src/ actualizado" -ForegroundColor Green
    $null = $Results.Add([pscustomobject]@{ Lambda = $Suffix; Group = $Meta.Group; Status = "OK" })
    Write-Host ""
}

Remove-Item $TempDir -Recurse -Force

Write-Host "========== Resumen ==========" -ForegroundColor White
$Results | Format-Table -AutoSize

$ok   = ($Results | Where-Object { $_.Status -eq "OK" }).Count
$skip = ($Results | Where-Object { $_.Status -ne "OK" }).Count
if ($skip -eq 0) {
    Write-Host "Completado: $ok OK" -ForegroundColor Green
} else {
    Write-Host "Completado: $ok OK, $skip con advertencias/errores" -ForegroundColor Yellow
}
