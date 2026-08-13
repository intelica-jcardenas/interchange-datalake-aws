# push-lambdas.ps1
# Sincroniza el CODIGO FUENTE de Lambdas desde el repo local hacia AWS.
# Sentido inverso a sync-lambdas.ps1 (AWS -> repo). Este script hace repo -> AWS.
#
# IMPORTANTE - Solo codigo, nunca configuracion:
#   Usa unicamente "aws lambda update-function-code". No se toca memoria, timeout,
#   variables de entorno, VPC, layers, ni ningun otro campo de configuracion.
#   Por eso, la proxima vez que corras sync-lambdas.ps1, lo unico distinto que
#   deberias ver en config.json es CodeSize / CodeSha256 / LastModified / RevisionId
#   -- eso es esperado y correcto, es la huella de este mismo push.
#
# Que se sube:
#   El contenido de <lambda>\src\ tal cual esta en el repo, zippeado con handler.py
#   en la raiz del zip (igual estructura que game el ZIP real de Lambda). Se excluyen
#   __pycache__, *.pyc/*.pyo/*.pyd, .pytest_cache, .DS_Store, Thumbs.db, *.log, *.swp/*.swo
#   -- nunca deben llegar a AWS. Archivos gitignored pero presentes localmente (ej.
#   resources\proxy_settings.json en mc-exchange-rates) SI se incluyen: el gitignore
#   protege el repo, no el deploy -- ese archivo es necesario en runtime.
#
# Lambdas Docker (PackageType=Image, ej. vi-exchange-rates) se saltan automaticamente:
#   update-function-code con --zip-file no aplica a imagenes; gestionar esa Lambda
#   por su pipeline de imagen Docker aparte.
#
# Prerequisitos:
#   aws sso login --profile itx-dev
#   $env:AWS_PROFILE = "itx-dev"
#
# Uso:
#   .\scripts\push-lambdas.ps1                          # sube todas (Zip-based)
#   .\scripts\push-lambdas.ps1 -Group mc                # solo Mastercard
#   .\scripts\push-lambdas.ps1 -Group vi                # solo Visa
#   .\scripts\push-lambdas.ps1 -Group general           # solo generales (router, unzip, archive-file)
#   .\scripts\push-lambdas.ps1 -Group test               # infra prestada de prueba (hoy: test-1 = rules-refresh)
#   .\scripts\push-lambdas.ps1 -Lambda mc-interpreter    # una especifica
#   .\scripts\push-lambdas.ps1 -Lambda mc-interpreter -WhatIf   # arma el zip y muestra tamano, no sube nada
#   .\scripts\push-lambdas.ps1 -Force                    # sin prompt de confirmacion (automatizacion)

param(
    [ValidateSet("all","mc","vi","general","test")]
    [string]$Group = "all",
    [string]$Lambda = "",
    [switch]$WhatIf,
    [switch]$Force
)

$RepoRoot = Split-Path -Parent $PSScriptRoot
$TempDir  = Join-Path $env:TEMP "lambda-push"
$Prefix   = "itl-0004-itx-dev-intchg-02-lmbd"

# Debe reflejar exactamente el mismo mapeo que sync-lambdas.ps1. Si agregas una
# Lambda nueva, actualiza los dos scripts.
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
    # Infra prestada (temporal) -- ver lambdas\rules-refresh\README.md.
    # "test-1" es el nombre REAL en AWS hoy (Lambda huerfano de pruebas,
    # creado 2026-06-10, repurposed 2026-08-03 para prototipar
    # lmbd-rules-refresh) -- no confundir con el FunctionName que dice
    # lambdas\rules-refresh\config.json (itl-...-lmbd-rules-refresh), que
    # todavia no existe en AWS. Actualizar esta entrada (suffix real y
    # Dir) el dia que se cree el Lambda definitivo con su propio rol/layer.
    "test-1"            = @{ Group="test"; Dir="lambdas\rules-refresh" }
}

# Directorios y patrones que nunca deben llegar al ZIP subido a AWS.
$ExcludeDirs  = @("__pycache__", ".pytest_cache")
$ExcludeFiles = @("*.pyc", "*.pyo", "*.pyd", ".DS_Store", "Thumbs.db", "*.log", "*.swp", "*.swo")

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

Write-Host "Lambdas a subir a AWS: $($ToSync.Count)" -ForegroundColor White
$ToSync.Keys | ForEach-Object { Write-Host "  $_" }
Write-Host ""

if (-not $WhatIf -and -not $Force) {
    Write-Host "Esto sobreescribe el codigo `$LATEST de cada Lambda listada en AWS (cuenta dev)." -ForegroundColor Yellow
    $Confirm = Read-Host "Continuar? (s/N)"
    if ($Confirm -notin @("s", "S", "si", "SI", "y", "Y", "yes", "YES")) {
        Write-Host "Cancelado." -ForegroundColor Yellow
        exit 0
    }
    Write-Host ""
}

if (Test-Path $TempDir) { Remove-Item $TempDir -Recurse -Force }
New-Item -ItemType Directory -Path $TempDir | Out-Null

$Results = New-Object System.Collections.ArrayList

foreach ($Suffix in $ToSync.Keys) {

    $Meta         = $ToSync[$Suffix]
    $FunctionName = "$Prefix-$Suffix"
    $LocalDir     = Join-Path $RepoRoot $Meta.Dir
    $LocalSrc     = Join-Path $LocalDir "src"
    $ConfigPath   = Join-Path $LocalDir "config.json"
    $StageDir     = Join-Path $TempDir "$Suffix-stage"
    $ZipPath      = Join-Path $TempDir "$Suffix.zip"

    Write-Host "[$Suffix]" -ForegroundColor Cyan

    if (-not (Test-Path $LocalSrc)) {
        Write-Host "  SKIP - No existe $LocalSrc" -ForegroundColor Yellow
        $null = $Results.Add([pscustomobject]@{ Lambda = $Suffix; Group = $Meta.Group; Status = "NO SRC" })
        Write-Host ""
        continue
    }

    # Lambdas Docker (Image) no se actualizan via update-function-code con zip-file.
    if (Test-Path $ConfigPath) {
        $LocalConfig = Get-Content $ConfigPath -Raw | ConvertFrom-Json
        if ($LocalConfig.PackageType -eq "Image") {
            Write-Host "  SKIP - PackageType=Image (Docker), no se sube via ZIP" -ForegroundColor Yellow
            $null = $Results.Add([pscustomobject]@{ Lambda = $Suffix; Group = $Meta.Group; Status = "SKIP (Image)" })
            Write-Host ""
            continue
        }
    }

    # 1. Copiar src/ a un staging dir, excluyendo __pycache__ y basura de SO/editor.
    if (Test-Path $StageDir) { Remove-Item $StageDir -Recurse -Force }
    New-Item -ItemType Directory -Path $StageDir | Out-Null

    $RobocopyArgs = @($LocalSrc, $StageDir, "/E", "/XD") + $ExcludeDirs + @("/XF") + $ExcludeFiles + @("/NFL", "/NDL", "/NJH", "/NJS", "/NP")
    robocopy @RobocopyArgs | Out-Null
    if ($LASTEXITCODE -ge 8) {
        Write-Host "  ERROR - robocopy fallo copiando $LocalSrc (exit $LASTEXITCODE)" -ForegroundColor Red
        $null = $Results.Add([pscustomobject]@{ Lambda = $Suffix; Group = $Meta.Group; Status = "ROBOCOPY ERROR" })
        Write-Host ""
        continue
    }

    $StagedFiles = Get-ChildItem -Path $StageDir -Recurse -File
    if ($StagedFiles.Count -eq 0) {
        Write-Host "  SKIP - src/ esta vacio tras excluir __pycache__/basura" -ForegroundColor Yellow
        $null = $Results.Add([pscustomobject]@{ Lambda = $Suffix; Group = $Meta.Group; Status = "EMPTY SRC" })
        Write-Host ""
        continue
    }
    if (-not (Test-Path (Join-Path $StageDir "handler.py"))) {
        Write-Host "  WARN - no se encontro handler.py en la raiz de src/ (se sube igual)" -ForegroundColor Yellow
    }

    # 2. Zippear el CONTENIDO de staging (no la carpeta) para que handler.py quede en la raiz del zip.
    if (Test-Path $ZipPath) { Remove-Item $ZipPath -Force }
    Compress-Archive -Path (Join-Path $StageDir "*") -DestinationPath $ZipPath -CompressionLevel Optimal
    $SizeKB = [math]::Round((Get-Item $ZipPath).Length / 1KB, 1)
    Write-Host "  ZIP armado: $SizeKB KB ($($StagedFiles.Count) archivos)"

    if ($WhatIf) {
        Write-Host "  WHATIF - no se sube (zip queda en $ZipPath)" -ForegroundColor Yellow
        $null = $Results.Add([pscustomobject]@{ Lambda = $Suffix; Group = $Meta.Group; Status = "WHATIF" })
        Write-Host ""
        continue
    }

    # 3. Subir a AWS -- SOLO codigo. No se suprime stderr: si aws cli falla
    #    (credenciales expiradas, funcion no encontrada, etc.) el mensaje real
    #    debe verse en consola en vez de un "ERROR" generico sin causa.
    $UpdateJson = aws lambda update-function-code `
        --function-name $FunctionName `
        --zip-file "fileb://$ZipPath" `
        --output json

    if (-not $UpdateJson) {
        Write-Host "  ERROR - update-function-code fallo (ver mensaje de aws cli arriba)" -ForegroundColor Red
        $null = $Results.Add([pscustomobject]@{ Lambda = $Suffix; Group = $Meta.Group; Status = "UPDATE FAILED" })
        Write-Host ""
        continue
    }

    $UpdateResult = $UpdateJson | ConvertFrom-Json
    Write-Host "  Subido -> CodeSha256=$($UpdateResult.CodeSha256.Substring(0,12))... LastModified=$($UpdateResult.LastModified)" -ForegroundColor Green
    $null = $Results.Add([pscustomobject]@{ Lambda = $Suffix; Group = $Meta.Group; Status = "OK" })
    Write-Host ""
}

Remove-Item $TempDir -Recurse -Force

Write-Host "========== Resumen ==========" -ForegroundColor White
$Results | Format-Table -AutoSize

$ok     = @($Results | Where-Object { $_.Status -eq "OK" }).Count
$skip   = @($Results | Where-Object { $_.Status -ne "OK" }).Count
$errors = @($Results | Where-Object { $_.Status -match "ERROR|FAILED" }).Count
if ($skip -eq 0) {
    Write-Host "Completado: $ok OK" -ForegroundColor Green
} else {
    Write-Host "Completado: $ok OK, $skip saltadas/con error (ver tabla)" -ForegroundColor Yellow
}

# Codigo de salida explicito -- sin esto, un $LASTEXITCODE residual de robocopy
# (1 = "archivos copiados", exito) se filtraria como codigo de salida del script.
if ($errors -gt 0) { exit 1 } else { exit 0 }
