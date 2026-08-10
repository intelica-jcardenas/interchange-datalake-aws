# sync-glue.ps1
# Sincroniza Jobs, Databases y Crawlers de AWS Glue desde AWS al repo local.
#
# Prerequisitos:
#   aws sso login --profile itx-dev
#   $env:AWS_PROFILE = "itx-dev"
#
# Uso:
#   .\scripts\sync-glue.ps1                               # todo (jobs + databases + crawlers)
#   .\scripts\sync-glue.ps1 -Resource jobs                # solo jobs
#   .\scripts\sync-glue.ps1 -Resource databases           # solo databases
#   .\scripts\sync-glue.ps1 -Resource crawlers            # solo crawlers
#   .\scripts\sync-glue.ps1 -Resource jobs -Group mc      # jobs Mastercard
#   .\scripts\sync-glue.ps1 -Resource jobs -Group reports # jobs de reportes y DQ
#   .\scripts\sync-glue.ps1 -Job vi-calculate             # un job especifico
#   .\scripts\sync-glue.ps1 -Name operational_ebgr_visa   # un database/crawler especifico

param(
    [ValidateSet("all","jobs","databases","crawlers")]
    [string]$Resource = "all",
    [ValidateSet("all","vi","mc","reports")]
    [string]$Group = "all",
    [string]$Job  = "",
    [string]$Name = ""
)

$RepoRoot      = Split-Path -Parent $PSScriptRoot
$JobPrefix     = "itl-0004-itx-dev-intchg-02-glue"
$CatalogPrefix = "itl_0004_itx_dev_02_"
$DbPrefix      = "${CatalogPrefix}glue_database_"
$CrawlerPrefix = "${CatalogPrefix}glue_crawler_"

$AllJobs = [ordered]@{
    "vi-calculate"   = @{ Group="vi";      Dir="glue\scripts\visa\calculate" }
    "vi-interchange" = @{ Group="vi";      Dir="glue\scripts\visa\interchange" }
    "mc-calculate"   = @{ Group="mc";      Dir="glue\scripts\mastercard\calculate" }
    "mc-interchange" = @{ Group="mc";      Dir="glue\scripts\mastercard\interchange" }
    # Reportes y Data Quality
    "get-transaction" = @{ Group="reports"; Dir="glue\scripts\reports\get_transaction" }
    "exchange-rates"   = @{ Group="reports"; Dir="glue\scripts\reports\exchange_rates" }
    "vi-data-quality"  = @{ Group="reports"; Dir="glue\scripts\reports\vi_data_quality" }
    "mc-data-quality"  = @{ Group="reports"; Dir="glue\scripts\reports\mc_data_quality" }
    "scheme-fee"     = @{ Group="reports"; Dir="glue\scripts\reports\scheme_fee" }
    "ebgr-report"    = @{ Group="reports"; Dir="glue\scripts\reports\ebgr_report" }
}

# Invoca AWS CLI capturando stdout como UTF-8 via System.Diagnostics.Process.
# Sin esto, Python (el AWS CLI) usa el charmap del sistema (cp1252) y falla con
# UnicodeEncodeError al encontrar caracteres como -> (U+2192) en descriptions de Glue.
# Leer stdout y stderr de forma async evita el deadlock clasico de pipes.
function Invoke-AwsCli {
    param([string]$Arguments)
    $psi = New-Object System.Diagnostics.ProcessStartInfo
    $psi.FileName               = "cmd.exe"
    $psi.Arguments              = "/c aws $Arguments"
    $psi.RedirectStandardOutput = $true
    $psi.RedirectStandardError  = $true
    $psi.UseShellExecute        = $false
    $psi.StandardOutputEncoding = [System.Text.Encoding]::UTF8
    $psi.EnvironmentVariables["PYTHONUTF8"] = "1"
    $proc       = [System.Diagnostics.Process]::Start($psi)
    $stdoutTask = $proc.StandardOutput.ReadToEndAsync()
    $stderrTask = $proc.StandardError.ReadToEndAsync()
    $proc.WaitForExit()
    return $stdoutTask.Result
}

# ---------------------------------------------------------------------------
function Sync-Jobs {
    param([string]$FilterGroup, [string]$FilterJob)

    Write-Host "=== Jobs ===" -ForegroundColor Cyan

    $ToSync = [ordered]@{}
    foreach ($Suffix in $AllJobs.Keys) {
        $Meta = $AllJobs[$Suffix]
        if ($FilterJob -ne "") {
            if ($Suffix -eq $FilterJob) { $ToSync[$Suffix] = $Meta }
        } elseif ($FilterGroup -eq "all" -or $Meta.Group -eq $FilterGroup) {
            $ToSync[$Suffix] = $Meta
        }
    }

    if ($ToSync.Count -eq 0) {
        Write-Host "  Sin jobs para los filtros indicados" -ForegroundColor Yellow
        return @()
    }

    $Results = @()
    foreach ($Suffix in $ToSync.Keys) {
        $Meta     = $ToSync[$Suffix]
        $JobName  = "$JobPrefix-$Suffix"
        $LocalDir = Join-Path $RepoRoot $Meta.Dir

        Write-Host "  [$Suffix]" -ForegroundColor Cyan

        $ConfigJson = Invoke-AwsCli "glue get-job --job-name $JobName --output json"
        if (-not $ConfigJson) {
            Write-Host "    SKIP - Job no encontrado en AWS" -ForegroundColor Yellow
            $Results += [pscustomobject]@{ Tipo="Job"; Sufijo=$Suffix; Estado="NOT FOUND" }
            Write-Host ""
            continue
        }

        $Config = $ConfigJson | ConvertFrom-Json

        if (-not (Test-Path $LocalDir)) {
            New-Item -ItemType Directory -Path $LocalDir -Force | Out-Null
        }

        [System.IO.File]::WriteAllText((Join-Path $LocalDir "config.json"), $ConfigJson, [System.Text.Encoding]::UTF8)
        Write-Host "    config.json actualizado"

        if ($Config.Job.DefaultArguments) {
            $ArgCount = ($Config.Job.DefaultArguments.PSObject.Properties | Measure-Object).Count
            $ArgsJson = (@{ DefaultArguments = $Config.Job.DefaultArguments } | ConvertTo-Json -Depth 5) + "`r`n"
            [System.IO.File]::WriteAllText((Join-Path $LocalDir "args.json"), $ArgsJson, [System.Text.Encoding]::UTF8)
            Write-Host "    args.json actualizado ($ArgCount args)"
        } else {
            $ArgsJson = (@{ DefaultArguments = @{} } | ConvertTo-Json) + "`r`n"
            [System.IO.File]::WriteAllText((Join-Path $LocalDir "args.json"), $ArgsJson, [System.Text.Encoding]::UTF8)
            Write-Host "    args.json creado (sin args)"
        }

        $ScriptS3Path = $Config.Job.Command.ScriptLocation
        if (-not $ScriptS3Path) {
            Write-Host "    WARN - ScriptLocation no encontrado en la config" -ForegroundColor Yellow
            $Results += [pscustomobject]@{ Tipo="Job"; Sufijo=$Suffix; Estado="CONFIG OK / SCRIPT FAILED" }
            Write-Host ""
            continue
        }

        $ScriptName  = Split-Path $ScriptS3Path -Leaf
        $ScriptLocal = Join-Path $LocalDir $ScriptName

        Write-Host "    Descargando script desde S3..."
        aws s3 cp $ScriptS3Path $ScriptLocal 2>$null | Out-Null

        if (-not (Test-Path $ScriptLocal)) {
            Write-Host "    WARN - No se pudo descargar el script" -ForegroundColor Yellow
            $Results += [pscustomobject]@{ Tipo="Job"; Sufijo=$Suffix; Estado="CONFIG OK / SCRIPT FAILED" }
            Write-Host ""
            continue
        }

        $SizeKB = [math]::Round((Get-Item $ScriptLocal).Length / 1KB, 1)
        Write-Host "    $ScriptName descargado ($SizeKB KB)" -ForegroundColor Green
        $Results += [pscustomobject]@{ Tipo="Job"; Sufijo=$Suffix; Estado="OK" }
        Write-Host ""
    }
    return $Results
}

# ---------------------------------------------------------------------------
function Sync-Databases {
    param([string]$FilterName)

    Write-Host "=== Databases ===" -ForegroundColor Cyan

    $Raw = Invoke-AwsCli "glue get-databases --output json"
    if (-not $Raw) {
        Write-Host "  ERROR - No se pudo conectar a AWS Glue" -ForegroundColor Red
        return @()
    }

    $All = ($Raw | ConvertFrom-Json).DatabaseList |
           Where-Object { $_.Name.StartsWith($DbPrefix) }

    if ($FilterName -ne "") {
        $All = $All | Where-Object { $_.Name.EndsWith($FilterName) }
    }

    if (-not $All) {
        Write-Host "  Sin resultados para el filtro indicado" -ForegroundColor Yellow
        return @()
    }

    $Results = @()
    foreach ($Db in $All) {
        $DbName = $Db.Name
        $Suffix = $DbName.Substring($DbPrefix.Length)
        Write-Host "  [$Suffix]" -ForegroundColor White

        $Detail = Invoke-AwsCli "glue get-database --name `"$DbName`" --output json"
        if (-not $Detail) {
            Write-Host "    ERROR - No se pudo obtener el detalle" -ForegroundColor Red
            $Results += [pscustomobject]@{ Tipo="Database"; Sufijo=$Suffix; Estado="ERROR" }
            continue
        }

        $OutFile = Join-Path $RepoRoot "glue\databases\$Suffix.json"
        [System.IO.File]::WriteAllText($OutFile, $Detail, [System.Text.Encoding]::UTF8)
        Write-Host "    $Suffix.json actualizado" -ForegroundColor Green
        $Results += [pscustomobject]@{ Tipo="Database"; Sufijo=$Suffix; Estado="OK" }
    }
    return $Results
}

# ---------------------------------------------------------------------------
function Sync-Crawlers {
    param([string]$FilterName)

    Write-Host "=== Crawlers ===" -ForegroundColor Cyan

    $Raw = Invoke-AwsCli "glue get-crawlers --output json"
    if (-not $Raw) {
        Write-Host "  ERROR - No se pudo conectar a AWS Glue" -ForegroundColor Red
        return @()
    }

    $All = ($Raw | ConvertFrom-Json).Crawlers |
           Where-Object { $_.Name.StartsWith($CrawlerPrefix) }

    if ($FilterName -ne "") {
        $All = $All | Where-Object { $_.Name.EndsWith($FilterName) }
    }

    if (-not $All) {
        Write-Host "  Sin resultados para el filtro indicado" -ForegroundColor Yellow
        return @()
    }

    $Results = @()
    foreach ($Crawler in $All) {
        $CrawlerName = $Crawler.Name
        $Suffix      = $CrawlerName.Substring($CrawlerPrefix.Length)
        Write-Host "  [$Suffix]" -ForegroundColor White

        $Detail = Invoke-AwsCli "glue get-crawler --name `"$CrawlerName`" --output json"
        if (-not $Detail) {
            Write-Host "    ERROR - No se pudo obtener el detalle" -ForegroundColor Red
            $Results += [pscustomobject]@{ Tipo="Crawler"; Sufijo=$Suffix; Estado="ERROR" }
            continue
        }

        $OutFile = Join-Path $RepoRoot "glue\crawlers\$Suffix.json"
        [System.IO.File]::WriteAllText($OutFile, $Detail, [System.Text.Encoding]::UTF8)
        Write-Host "    $Suffix.json actualizado" -ForegroundColor Green
        $Results += [pscustomobject]@{ Tipo="Crawler"; Sufijo=$Suffix; Estado="OK" }
    }
    return $Results
}

# ---------------------------------------------------------------------------
# -Job implica -Resource jobs
if ($Job -ne "") { $Resource = "jobs" }

$AllResults = @()

if ($Resource -eq "all" -or $Resource -eq "jobs") {
    $AllResults += Sync-Jobs -FilterGroup $Group -FilterJob $Job
    Write-Host ""
}

if ($Resource -eq "all" -or $Resource -eq "databases") {
    $AllResults += Sync-Databases -FilterName $Name
    Write-Host ""
}

if ($Resource -eq "all" -or $Resource -eq "crawlers") {
    $AllResults += Sync-Crawlers -FilterName $Name
}

Write-Host ""
Write-Host "========== Resumen ==========" -ForegroundColor White
$AllResults | Format-Table -AutoSize

$ok   = ($AllResults | Where-Object { $_.Estado -eq "OK" }).Count
$fail = ($AllResults | Where-Object { $_.Estado -ne "OK" }).Count
if ($fail -eq 0) {
    Write-Host "Completado: $ok OK" -ForegroundColor Green
} else {
    Write-Host "Completado: $ok OK, $fail con advertencias/errores" -ForegroundColor Yellow
}
