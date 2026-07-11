# push-glue.ps1
# Sincroniza el CODIGO (script .py) de Glue Jobs desde el repo local hacia AWS.
# Sentido inverso a sync-glue.ps1 -Resource jobs (AWS -> repo). Este script hace repo -> AWS.
#
# IMPORTANTE - Solo codigo, nunca configuracion:
#   Usa unicamente "aws s3 cp" para sobreescribir el script en su ScriptLocation actual.
#   No se llama a "glue update-job" -- DefaultArguments, workers, timeout, etc. quedan
#   intactos. La proxima vez que corras sync-glue.ps1, lo unico distinto que deberias
#   ver en config.json es LastModifiedOn (y el contenido descargado del script, que
#   coincidira con lo que ya tenias local) -- eso es esperado y correcto.
#
# De donde saca el ScriptLocation:
#   Lee el config.json local de cada job (el mismo que deja sync-glue.ps1) y usa
#   Job.Command.ScriptLocation como destino S3. Si no existe ese config.json, corre
#   sync-glue.ps1 primero para ese job.
#
# Prerequisitos:
#   aws sso login --profile itx-dev
#   $env:AWS_PROFILE = "itx-dev"
#
# Uso:
#   .\scripts\push-glue.ps1                          # sube todos los jobs
#   .\scripts\push-glue.ps1 -Group mc                # jobs Mastercard
#   .\scripts\push-glue.ps1 -Group vi                # jobs Visa
#   .\scripts\push-glue.ps1 -Group reports           # jobs de reportes y DQ
#   .\scripts\push-glue.ps1 -Job vi-calculate        # uno especifico
#   .\scripts\push-glue.ps1 -Job vi-calculate -WhatIf   # muestra que se subiria, no sube nada
#   .\scripts\push-glue.ps1 -Force                   # sin prompt de confirmacion (automatizacion)

param(
    [ValidateSet("all","vi","mc","reports")]
    [string]$Group = "all",
    [string]$Job = "",
    [switch]$WhatIf,
    [switch]$Force
)

$RepoRoot = Split-Path -Parent $PSScriptRoot

# Debe reflejar exactamente el mismo mapeo que sync-glue.ps1 ($AllJobs). Si agregas
# un job nuevo, actualiza los dos scripts.
$AllJobs = [ordered]@{
    "vi-calculate"     = @{ Group="vi";      Dir="glue\scripts\visa\calculate" }
    "vi-interchange"   = @{ Group="vi";      Dir="glue\scripts\visa\interchange" }
    "mc-calculate"     = @{ Group="mc";      Dir="glue\scripts\mastercard\calculate" }
    "mc-interchange"   = @{ Group="mc";      Dir="glue\scripts\mastercard\interchange" }
    # Reportes y Data Quality
    "get-transaction"  = @{ Group="reports"; Dir="glue\scripts\reports\get_transaction" }
    "exchange-rates"   = @{ Group="reports"; Dir="glue\scripts\reports\exchange_rates" }
    "vi-data-quality"  = @{ Group="reports"; Dir="glue\scripts\reports\vi_data_quality" }
    "mc-data-quality"  = @{ Group="reports"; Dir="glue\scripts\reports\mc_data_quality" }
    "scheme-fee"       = @{ Group="reports"; Dir="glue\scripts\reports\scheme_fee" }
}

# Validar -Job si se paso
if ($Job -ne "" -and -not $AllJobs.Contains($Job)) {
    Write-Host "ERROR: Job '$Job' no reconocido. Opciones:" -ForegroundColor Red
    $AllJobs.Keys | ForEach-Object { Write-Host "  $_" }
    exit 1
}

# Filtrar segun parametros
$ToSync = [ordered]@{}
foreach ($Suffix in $AllJobs.Keys) {
    $Meta = $AllJobs[$Suffix]
    if ($Job -ne "") {
        if ($Suffix -eq $Job) { $ToSync[$Suffix] = $Meta }
    } elseif ($Group -eq "all" -or $Meta.Group -eq $Group) {
        $ToSync[$Suffix] = $Meta
    }
}

if ($ToSync.Count -eq 0) {
    Write-Host "No hay jobs para sincronizar con los parametros indicados." -ForegroundColor Yellow
    exit 0
}

Write-Host "Glue Jobs a subir a AWS: $($ToSync.Count)" -ForegroundColor White
$ToSync.Keys | ForEach-Object { Write-Host "  $_" }
Write-Host ""

if (-not $WhatIf -and -not $Force) {
    Write-Host "Esto sobreescribe el script .py en S3 (ScriptLocation) de cada job listado." -ForegroundColor Yellow
    Write-Host "Afecta la PROXIMA ejecucion de cada job (no hay ejecuciones en curso afectadas)." -ForegroundColor Yellow
    $Confirm = Read-Host "Continuar? (s/N)"
    if ($Confirm -notin @("s", "S", "si", "SI", "y", "Y", "yes", "YES")) {
        Write-Host "Cancelado." -ForegroundColor Yellow
        exit 0
    }
    Write-Host ""
}

$Results = New-Object System.Collections.ArrayList

foreach ($Suffix in $ToSync.Keys) {

    $Meta       = $ToSync[$Suffix]
    $LocalDir   = Join-Path $RepoRoot $Meta.Dir
    $ConfigPath = Join-Path $LocalDir "config.json"

    Write-Host "[$Suffix]" -ForegroundColor Cyan

    if (-not (Test-Path $ConfigPath)) {
        Write-Host "  SKIP - No existe config.json (corre sync-glue.ps1 -Job $Suffix primero)" -ForegroundColor Yellow
        $null = $Results.Add([pscustomobject]@{ Job = $Suffix; Group = $Meta.Group; Status = "NO CONFIG" })
        Write-Host ""
        continue
    }

    $LocalConfig  = Get-Content $ConfigPath -Raw | ConvertFrom-Json
    $ScriptS3Path = $LocalConfig.Job.Command.ScriptLocation

    if (-not $ScriptS3Path) {
        Write-Host "  SKIP - config.json no tiene Job.Command.ScriptLocation" -ForegroundColor Yellow
        $null = $Results.Add([pscustomobject]@{ Job = $Suffix; Group = $Meta.Group; Status = "NO SCRIPT LOCATION" })
        Write-Host ""
        continue
    }

    $ScriptName  = Split-Path $ScriptS3Path -Leaf
    $ScriptLocal = Join-Path $LocalDir $ScriptName

    if (-not (Test-Path $ScriptLocal)) {
        Write-Host "  SKIP - No existe $ScriptLocal" -ForegroundColor Yellow
        $null = $Results.Add([pscustomobject]@{ Job = $Suffix; Group = $Meta.Group; Status = "NO LOCAL SCRIPT" })
        Write-Host ""
        continue
    }

    $SizeKB = [math]::Round((Get-Item $ScriptLocal).Length / 1KB, 1)
    Write-Host "  $ScriptName ($SizeKB KB) -> $ScriptS3Path"

    if ($WhatIf) {
        Write-Host "  WHATIF - no se sube" -ForegroundColor Yellow
        $null = $Results.Add([pscustomobject]@{ Job = $Suffix; Group = $Meta.Group; Status = "WHATIF" })
        Write-Host ""
        continue
    }

    aws s3 cp $ScriptLocal $ScriptS3Path 2>$null | Out-Null

    if ($LASTEXITCODE -ne 0) {
        Write-Host "  ERROR - aws s3 cp fallo" -ForegroundColor Red
        $null = $Results.Add([pscustomobject]@{ Job = $Suffix; Group = $Meta.Group; Status = "UPLOAD FAILED" })
        Write-Host ""
        continue
    }

    Write-Host "  Subido" -ForegroundColor Green
    $null = $Results.Add([pscustomobject]@{ Job = $Suffix; Group = $Meta.Group; Status = "OK" })
    Write-Host ""
}

Write-Host "========== Resumen ==========" -ForegroundColor White
$Results | Format-Table -AutoSize

$ok     = ($Results | Where-Object { $_.Status -eq "OK" }).Count
$skip   = ($Results | Where-Object { $_.Status -ne "OK" }).Count
$errors = ($Results | Where-Object { $_.Status -match "ERROR|FAILED" }).Count
if ($skip -eq 0) {
    Write-Host "Completado: $ok OK" -ForegroundColor Green
} else {
    Write-Host "Completado: $ok OK, $skip saltados/con error (ver tabla)" -ForegroundColor Yellow
}

# Codigo de salida explicito -- no depender de un $LASTEXITCODE residual.
if ($errors -gt 0) { exit 1 } else { exit 0 }
