# sync-dynamodb.ps1
# Sincroniza schemas (describe-table) e items de tablas DynamoDB desde AWS al repo local.
#
# Prerequisitos:
#   aws sso login --profile itx-dev
#   $env:AWS_PROFILE = "itx-dev"
#
# Uso:
#   .\scripts\sync-dynamodb.ps1                        # todo (schemas + items)
#   .\scripts\sync-dynamodb.ps1 -Resource schemas      # solo schemas
#   .\scripts\sync-dynamodb.ps1 -Resource items        # solo items
#   .\scripts\sync-dynamodb.ps1 -Table visa_fields     # tabla especifica (schemas + items)
#   .\scripts\sync-dynamodb.ps1 -Resource schemas -Table client

param(
    [ValidateSet("all","schemas","items")]
    [string]$Resource = "all",
    [string]$Table = ""
)

$RepoRoot    = Split-Path -Parent $PSScriptRoot
$TablePrefix = "itl-0004-itx-dev-dynamo-"
$TableSuffix = "-02"

# Tablas de configuracion cuyos items vale la pena versionar.
# file_control se excluye: datos operacionales que cambian con cada archivo procesado.
$ItemTables = @("client", "file_pattern", "visa_fields", "mastercard_fields")

# Invoca AWS CLI capturando stdout como UTF-8 via System.Diagnostics.Process.
# PYTHONUTF8=1 evita UnicodeEncodeError en campos con caracteres no-ASCII.
# Lectura async de stdout/stderr previene deadlock en pipes.
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

function Get-AllTables {
    $Raw = Invoke-AwsCli "dynamodb list-tables --output json"
    if (-not $Raw) { return @() }
    ($Raw | ConvertFrom-Json).TableNames |
        Where-Object { $_.StartsWith($TablePrefix) -and $_.EndsWith($TableSuffix) } |
        ForEach-Object {
            $suffix = $_.Substring($TablePrefix.Length, $_.Length - $TablePrefix.Length - $TableSuffix.Length)
            [pscustomobject]@{ Suffix = $suffix; FullName = $_ }
        }
}

# ---------------------------------------------------------------------------
function Sync-Schemas {
    param([string]$FilterTable)
    Write-Host "=== Schemas ===" -ForegroundColor Cyan

    $Tables = Get-AllTables
    if ($FilterTable -ne "") {
        $Tables = $Tables | Where-Object { $_.Suffix -eq $FilterTable }
    }

    if (-not $Tables) {
        Write-Host "  Sin tablas para el filtro indicado" -ForegroundColor Yellow
        return @()
    }

    $OutDir = Join-Path $RepoRoot "dynamodb\schemas"
    if (-not (Test-Path $OutDir)) { New-Item -ItemType Directory -Path $OutDir -Force | Out-Null }

    $Results = @()
    foreach ($t in $Tables) {
        Write-Host "  [$($t.Suffix)]" -ForegroundColor White
        $Raw = Invoke-AwsCli "dynamodb describe-table --table-name `"$($t.FullName)`" --output json"
        if (-not $Raw) {
            Write-Host "    ERROR - No se pudo obtener el schema" -ForegroundColor Red
            $Results += [pscustomobject]@{ Tipo="Schema"; Tabla=$t.Suffix; Estado="ERROR" }
            continue
        }
        $OutFile   = Join-Path $OutDir "$($t.Suffix).json"
        [System.IO.File]::WriteAllText($OutFile, $Raw, [System.Text.Encoding]::UTF8)
        $itemCount = ($Raw | ConvertFrom-Json).Table.ItemCount
        Write-Host "    $($t.Suffix).json actualizado ($itemCount items en tabla)" -ForegroundColor Green
        $Results += [pscustomobject]@{ Tipo="Schema"; Tabla=$t.Suffix; Estado="OK" }
    }
    return $Results
}

# ---------------------------------------------------------------------------
function Sync-Items {
    param([string]$FilterTable)
    Write-Host "=== Items ===" -ForegroundColor Cyan

    if ($FilterTable -ne "" -and $FilterTable -eq "file_control") {
        Write-Host "  [file_control] excluida de items (tabla operacional, cambia con cada archivo procesado)" -ForegroundColor Yellow
        return @()
    }

    $Tables = Get-AllTables | Where-Object { $ItemTables -contains $_.Suffix }
    if ($FilterTable -ne "") {
        $Tables = $Tables | Where-Object { $_.Suffix -eq $FilterTable }
    }

    if (-not $Tables) {
        Write-Host "  Sin tablas para el filtro indicado" -ForegroundColor Yellow
        return @()
    }

    $OutDir = Join-Path $RepoRoot "dynamodb\items"
    if (-not (Test-Path $OutDir)) { New-Item -ItemType Directory -Path $OutDir -Force | Out-Null }

    $Results = @()
    foreach ($t in $Tables) {
        Write-Host "  [$($t.Suffix)]" -ForegroundColor White

        # Obtener schema para conocer claves de ordenacion (diffs deterministas en git)
        $SchemaRaw = Invoke-AwsCli "dynamodb describe-table --table-name `"$($t.FullName)`" --output json"
        $KeySchema = ($SchemaRaw | ConvertFrom-Json).Table.KeySchema
        $HashKey   = ($KeySchema | Where-Object { $_.KeyType -eq "HASH"  }).AttributeName
        $RangeKey  = ($KeySchema | Where-Object { $_.KeyType -eq "RANGE" }).AttributeName

        Write-Host "    Escaneando items..."
        $Raw = Invoke-AwsCli "dynamodb scan --table-name `"$($t.FullName)`" --output json"
        if (-not $Raw) {
            Write-Host "    ERROR - No se pudo escanear la tabla" -ForegroundColor Red
            $Results += [pscustomobject]@{ Tipo="Items"; Tabla=$t.Suffix; Estado="ERROR" }
            continue
        }

        $Items = ($Raw | ConvertFrom-Json).Items

        # Ordenar por PK (+ SK si existe) para diffs reproducibles entre runs
        $Sorted = $Items | Sort-Object {
            $item  = $_
            $hProp = $item.$HashKey
            $hVal  = if ($hProp.S) { $hProp.S } elseif ($hProp.N) { $hProp.N } else { "" }
            if ($RangeKey) {
                $rProp = $item.$RangeKey
                $rVal  = if ($rProp.S) { $rProp.S } elseif ($rProp.N) { $rProp.N } else { "" }
                "$hVal`t$rVal"
            } else { $hVal }
        }

        $Output  = [ordered]@{ TableName = $t.FullName; Count = $Items.Count; Items = $Sorted }
        $Json    = ($Output | ConvertTo-Json -Depth 20) + "`r`n"
        $OutFile = Join-Path $OutDir "$($t.Suffix).json"
        [System.IO.File]::WriteAllText($OutFile, $Json, [System.Text.Encoding]::UTF8)
        Write-Host "    $($t.Suffix).json actualizado ($($Items.Count) items)" -ForegroundColor Green
        $Results += [pscustomobject]@{ Tipo="Items"; Tabla=$t.Suffix; Estado="OK" }
    }
    return $Results
}

# ---------------------------------------------------------------------------
$AllResults = @()

if ($Resource -eq "all" -or $Resource -eq "schemas") {
    $AllResults += Sync-Schemas -FilterTable $Table
    Write-Host ""
}

if ($Resource -eq "all" -or $Resource -eq "items") {
    $AllResults += Sync-Items -FilterTable $Table
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
