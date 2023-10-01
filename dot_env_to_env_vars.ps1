param(
    [string]$Path,
    [switch]$Verbose,
    [switch]$Remove,
    [switch]$RemoveQuotes
)

Write-Host "Collecting variables from conf file $Path"
$variables = Select-String -Path $Path -Pattern '^\s*[^\s=#]+=[^\s]+$' -Raw

foreach($var in $variables) {
    $keyVal = $var -split '=', 2
    $key = $keyVal[0].Trim()
    $val = $RemoveQuotes ? $keyVal[1].Trim("'").Trim('"') : $keyVal[1]
    [Environment]::SetEnvironmentVariable($key, $Remove ? '' : $val)
    if ($Verbose) {
        "$key=$([Environment]::GetEnvironmentVariable($key))"
    }
}