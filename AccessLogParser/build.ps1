$module = 'AccessLogParser'
    Push-Location $PSScriptroot

    dotnet build -o $PSScriptRoot\output\$module\bin
    Import-Module "$PSScriptRoot\Output\$module\bin\$module.dll"
    $VerbosePreference = $DebugPreference="continue"
    Write-Debug "$pid - $($PSVersionTable.PSVersion)"