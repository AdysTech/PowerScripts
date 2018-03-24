<#
.SYNOPSIS

Powershell based app server, to allow triggerring execution of long running powershell jobs over http. Original idea came from https://gallery.technet.microsoft.com/Simple-REST-api-for-b04489f1
It allows any external scripts to be executed in both in sync or async mode. Allows for additional arguments to be passed to those scripts from http payload (POST requests) 

.DESCRIPTION

Powershell based app server, to allow triggerring execution of long running powershell jobs over http. Supports  asynchronous execution, status tracking, and executing really long running powershell scripts on a server, 
triggered from a browser. It allows for additional arguments to be passed to those scripts from http payload (POST requests).
The access can be controlled for a specific AD groups, and the commands to be executed needs to be stored in same folder as this script, 
and the script name becomes the command variable in the URL. It logs the commands to an audit log, keeps the history of the jobs in a separate file for tracking purposes. 
A simple bootstrapped html frontend gives an easy UI.

Copyright: mvadu@adystech
license: MIT

#>

Param (
    #Port on which the server should be listening
    [Parameter()]
    [Int] $Port = 3355,

    #Base URL
    [Parameter()]
    [String] $Url = "",
    
    #AD Group, any user invoking the URLs in this server should be part of this group. 
    [Parameter()]
    [string] $primaryGroup = "PsJobSchedulers"
)

#region Variables and Initialize
$VerbosePreference = "Continue"

write-verbose "$([datetime]::now.tostring()) Starting $(split-path -Leaf $MyInvocation.MyCommand.Definition ) process"
 
$workingDir = split-path -parent $MyInvocation.MyCommand.Definition
   
[System.Net.AuthenticationSchemes] $Auth = [System.Net.AuthenticationSchemes]::IntegratedWindowsAuthentication
$jobWatcherTimer = new-object System.Timers.Timer(20000)
$listener = New-Object System.Net.HttpListener
$jobQueue = [System.Collections.Queue]::Synchronized((New-Object System.Collections.Queue))
$jobCollection = New-Object Collections.Generic.List[PSCustomObject]
[long] $requestCount = 0
[long] $errorCount = 0
$MaxActiveProcess = 2
$runspacePool = [runspacefactory]::CreateRunspacePool(1, $MaxActiveProcess)
#endregion
 
#region Functions

#
$jobWatchercallback = { 
    $modified = $false
    try {
        if ($jobQueue.Count -gt 0 -or $jobCollection.Count -gt 0) {
            $mtx = New-Object System.Threading.Mutex($false, "Global\PSJobMutex")
            if (-not $mtx.WaitOne(2000)) {
                return 
            }
            $path = $(Resolve-path ".\ProcessStatus.json")
            $statusEntries = Get-Content -Raw -Path $path | ConvertFrom-Json
            if ($jobQueue.Count -gt 0) {
                while ($jobQueue.Count -gt 0) {
                    $arguemnts = $jobQueue.Dequeue()

                    $PSinstance = [PowerShell]::Create()
                    $PSinstance.AddScript( {$VerbosePreference = "Continue"}, $false).Invoke()
                    $PSinstance.AddScript($ExecuteScriptAsync).AddArgument($arguemnts.WorkingDir).AddArgument($arguemnts.Script).AddArgument($arguemnts.CommandArgs).AddArgument($arguemnts.RunId)
                    $PSinstance.RunspacePool = $runspacePool
                    $jobCollection.Add([pscustomobject]@{
                            Runspace   = $PSinstance.BeginInvoke()
                            PowerShell = $PSinstance
                            RunID      = $arguemnts.RunId
                        })

                    $process = $statusEntries.Processes | Where-Object {$_.RunId -eq $arguemnts.RunId}
                    $process.Status = "Started"
                    $modified = $true
                }
            }
            elseif ($jobCollection.Count -gt 0) {
                foreach ($job in ($jobCollection | Where-Object {$_.Runspace.IsCompleted})) {                    
                    $process = $statusEntries.Processes | Where-Object {$_.RunID -eq $job.RunID}                    
                    $log = ".\Logs\$($job.RunID).log"
                    #read both verbose and output streams              
                    $entries = $job.PowerShell.Streams.Verbose.ReadAll()
                    $entries += $job.PowerShell.EndInvoke($job.Runspace)
                     
                    $entries | Out-File -FilePath $log -Append | out-null
                    $job.PowerShell.Dispose()
                    $jobCollection.Remove($job)
                }   
                $modified = $true
            }
            if ($modified) {
                $statusEntries | ConvertTo-Json -Depth 999 | Out-File $path
            }
            $mtx.ReleaseMutex()
        }
    }
    catch [Exception] { 
        write-error "Timer callback Error $($_.Exception.Message) at $($_.InvocationInfo.PositionMessage)" -ErrorAction Continue
    }
}

# This executes the passed script asynchronously. It updates the status entries as well
$ExecuteScriptAsync = {
    param (
        [ValidateNotNullOrEmpty()]
        [parameter(Mandatory = $true)]
        [string] $workingDir,            
        [ValidateNotNullOrEmpty()]
        [parameter(Mandatory = $true)]
        [string] $scriptFileName,
        [ValidateNotNullOrEmpty()]
        [parameter(Mandatory = $true)]
        [PSCustomObject] $arguments,
        [int] $runId
    )
            
    Set-Location $workingDir
    $path = $(Resolve-path ".\ProcessStatus.json")
    $mtx = New-Object System.Threading.Mutex($false, "Global\PSJobMutex")
    $retryCount = 0    
    while ($retryCount -lt 6) {
        if ($mtx.WaitOne(5000)) {
            break
        }
    }
    if ($retryCount -eq 6) {write-error "Unable to get lock on process file" -ErrorAction Stop; return}
    
    $statusEntries = Get-Content -Raw -Path $path | ConvertFrom-Json
    $process = $statusEntries.Processes | Where-Object {$_.RunId -eq $runId}
    $process.Status = "Processing"
    $statusEntries | ConvertTo-Json -Depth 999 | Out-File $path            
    $mtx.ReleaseMutex()
 
    $arguments | & $scriptFileName

    $log = ".\Logs\$runId.log"
    New-Item $log -Force -ItemType File | out-null
    #Set-Content $log $scriptresult

    $mtx = New-Object System.Threading.Mutex($false, "Global\PSJobMutex")
    if (-not $mtx.WaitOne(2000)) {
        return $false
    }
    $statusEntries = Get-Content -Raw -Path $path | ConvertFrom-Json
    $process = $statusEntries.Processes | Where-Object {$_.RunId -eq $runId}
    $process.EndTime = [DateTime]::Now
    $process.Status = "Completed"
    $statusEntries | ConvertTo-Json -Depth 999 | Out-File $path            
    $mtx.ReleaseMutex()
    return $true
}

# This executes the passed script synchronously and returns resutlts in an array, which can be serialized to json
Function ExecuteScript {
    param (
        [ValidateNotNullOrEmpty()]
        [parameter(Mandatory = $true)]
        [string] $scriptFileName,
        [ValidateNotNullOrEmpty()]
        [parameter(Mandatory = $true)]
        [PSCustomObject] $arguments
    )             
    $scriptresult = $arguments | & $scriptFileName
        
    return , $scriptresult
}

Function Log-AuditMessage {
    [CmdletBinding()]
    param (
        [string] $Message
    )
    $auditLog = "./AuditLogs/psserver-auditlog$([DateTime]::Today.ToString("yyyy-MM-dd")).log"
    if (!(Test-Path $auditLog)) { 
        New-Item $auditLog -Force -ItemType File | out-null
    }
    # Format Date for our Log File 
    $FormattedDate = Get-Date -Format "yyyy-MM-dd HH:mm:ss" 
    "$FormattedDate $Message" | Out-File -FilePath $auditLog -Append | out-null
}

Function GetProcessStatus {
    $mtx = New-Object System.Threading.Mutex($false, "Global\PSJobMutex")
    if (-not $mtx.WaitOne(2000)) {
        return $null
    }
    $statusEntries = Get-Content -Raw -Path $(Resolve-path "./ProcessStatus.json") | ConvertFrom-Json
    $mtx.ReleaseMutex()
    return $statusEntries
}

Function UpdateProcessStatus {
    [CmdletBinding()]
    param (
        [pscustomobject] $process
    )
    $mtx = New-Object System.Threading.Mutex($false, "Global\PSJobMutex")
    if (-not $mtx.WaitOne(2000)) {
        return $false
    }
    $path = $(Resolve-path ".\ProcessStatus.json")
    $statusEntries = Get-Content -Raw -Path $path | ConvertFrom-Json
    $statusEntries.Processes += $process
    $statusEntries | ConvertTo-Json -Depth 999 | Out-File $path
    $mtx.ReleaseMutex()|Out-Null
    return $true
}

Function ArchiveProcessEntries {
    [CmdletBinding()]
    param (
        [timespan] $TimeToLive
    )
    $mtx = New-Object System.Threading.Mutex($false, "Global\PSJobMutex")
    if (-not $mtx.WaitOne(2000)) {
        return $false
    }
    $path = $(Resolve-path ".\ProcessStatus.json")

        
    $statusEntries = Get-Content -Raw -Path $path | ConvertFrom-Json
    $now = [DateTime]::Now
    $archiveEntries = $statusEntries.Processes | Where-Object { $_.Status -eq "Completed" -and $_.StartTime + $TimeToLive -lt $now}
    $statusEntries.Processes = $statusEntries.Processes | Where-Object { $_.Status -ne "Completed" -or $_.StartTime + $TimeToLive -gt $now}
    $statusEntries | ConvertTo-Json -Depth 999 | Out-File $path

    $archive = ".\ProcessArchive-$($now.ToString('MMM')).json"
    if (-not(test-path $archive)) {
        New-Item $archive -Force -ItemType File | out-null
    }
    else {
        $archiveEntries += (Get-Content -Raw -Path $archive | ConvertFrom-Json).Processes
    }
    $archiveEntries | ConvertTo-Json -Depth 999 | Out-File $archive
    $mtx.ReleaseMutex()|Out-Null
    return $true
}

function Send-Response {
    [CmdletBinding()]
    param (
        [int] $statusCode,

        [ValidateNotNullOrEmpty()]
        [System.Net.HttpListenerResponse] $response,
   
        [ValidateSet('JSON', 'TEXT')]
        [ValidateNotNullOrEmpty()]
        [string]$format,

        [ValidateNotNullOrEmpty()]
        $responseData
    )
    #if response is already sent, return
    if ($response.ContentLength64 -ne 0 ) {
        return
    }
        
    $response.StatusCode = $statusCode
    switch ($format) {
        JSON {
            $response.ContentType = "application/json"
            $response.AddHeader("Expires", "29");
            $buffer = [System.Text.Encoding]::UTF8.GetBytes(($responseData | ConvertTo-JSON -depth 999))
            break
        }
        TEXT {
            $response.ContentType = "application/text"
            $response.AddHeader("Expires", "86400");
            $buffer = [System.Text.Encoding]::UTF8.GetBytes(($responseData))
            break
        }
    }
    $response.ContentLength64 = $buffer.Length
    $output = $response.OutputStream
    $output.Write($buffer, 0, $buffer.Length)
    $output.Close()
}

#endregion


$ErrorActionPreference = "Stop"
$CommandKeywords = "status;archivejobs;runoutput;"

$CurrentPrincipal = New-Object Security.Principal.WindowsPrincipal( [Security.Principal.WindowsIdentity]::GetCurrent())

if ( -not ($currentPrincipal.IsInRole( [Security.Principal.WindowsBuiltInRole]::Administrator ))) {
    write-error "This script must be executed from an elevated PowerShell session" -ErrorAction Stop 
}

if ($Url.Length -gt 0 -and -not $Url.EndsWith('/')) {
    $Url += "/"
}

$prefix = "http://*:$Port/$Url"
$listener.Prefixes.Add($prefix)
$listener.AuthenticationSchemes = $Auth 

#region main block
try {
    Register-ObjectEvent -InputObject $jobWatcherTimer -EventName Elapsed -SourceIdentifier  timerEvntId -Action $jobWatchercallback 
    $jobWatcherTimer.start()
    $runspacePool.Open()
    $listener.Start()

    while ($true) {
        try {
            $context = $listener.GetContext()
            $statusCode = 200
            $requestCount = $requestCount + 1
            $runId = [long] ([datetime]::UtcNow - [datetime]::FromBinary(630822816000000000)).TotalSeconds
            write-verbose "$requestCount  -  Received a request - $runID"
            
            $request = $context.Request

            if (!$request.IsAuthenticated) {
                $statusCode = 403
                $commandOutput = @{Success = $false; Error = "Unauthorized"; Message = "Rejected request as user was not authenticated"}
            }
            else {
                $user = $context.User.Identity.Name
                Log-AuditMessage "RunID:$runID User:$user  Request:$($request.QueryString)"                
                # only allow requests that are part of allowed group
                if (-not $context.User.IsInRole($primaryGroup)) {
                    $statusCode = 403
                    $commandOutput = @{Success = $false; Error = "Unauthorized"; Message = "Rejected request as user is not in allowed group"}
                }
                else {
                    if (-not $request.QueryString.HasKeys()) {
                        $commandOutput = @{Success = $false; Error = "InvalidInput"; Message = "No Input provided!! Refer to user guide"}
                    }
                    else {
                        $command = $request.QueryString.Item("command").ToLower()
                        if ($request.HttpMethod -eq "POST" -and $request.HasEntityBody) {
                            try {                            
                                $reader = New-Object System.IO.StreamReader($request.InputStream)
                                $arg = $reader.ReadToEnd()
                                $commandArgs = $arg | ConvertFrom-Json
                            }
                            catch {
                                $commandOutput = @{Success = $false; Error = "InvalidInput"; Message = "Invalid POST data!! Refer to user guide"}
                            }
                            finally {
                                $reader.Close()
                            }
                        }
                       
                        switch ($command) {
                            status {
                                $processes = $(GetProcessStatus).Processes
                                
                                if ( $request.QueryString["runid"] -ne $null) {                                
                                    $commandOutput = @{Success = $True; RunID = $runId; Process = ($Processes | Where-Object {$_.RunId -eq $request.QueryString["runid"]})}
                                }
                                else {
                                    $commandOutput = @{Success = $True; RunID = $runId; Processes = $processes}    
                                }
                                break
                            }
                            archivejobs {
                                ArchiveProcessEntries $([TimeSpan]::FromHours(24))
                                $processes = $(GetProcessStatus).Processes
                                $commandOutput = @{Success = $True; RunID = $runId; Processes = $processes}    
                                break
                            }
                            runoutput {
                                if ( $request.QueryString["runid"] -ne $null) {
                                    $log = ".\Logs\$($request.QueryString["runid"]).log"
                                    if ((Test-Path $log) -and ($content = $((Get-Content $log) -join "`n`n")) -and (-not([string]::IsNullOrEmpty($content)))) {
                                        try {
                                            Send-Response $statusCode $context.Response "TEXT" $((Get-Content $log) -join "`n")
                                        }
                                        catch {
                                            $statusCode = 500
                                            @{Success = $false; Error = "UnableToProcess"; Message = "Unable to Process request"}; 
                                        }
                                    }
                                    else {
                                        Send-Response 404 $context.Response "TEXT" "File Not Found"
                                    }
                                    continue
                                }
                                break
                            }
                            Default {
                                $script = "./Execute_$command.ps1" 
                                    
                                if (-not(Test-Path $script)) {
                                    $statusCode = 500
                                    @{Success = $false; Error = "InvalidInput"; Message = "Command $command is not known!!"}; 
                                }
                                else {
                                    $script = Resolve-Path $script

                                    if ( $request.QueryString["async"] -ne $null) {
                                        if ($commandArgs -eq $null) {
                                            $commandOutput = @{Success = $false; Error = "InvalidInput"; Message = "Invalid POST data!! Async requires POST parameters"}
                                        }
                                        else {
                                            $process = [PsCustomObject] @{
                                                StartTime = [DateTime]::Now
                                                User      = $user
                                                Command   = $command
                                                Status    = "Queued"
                                                RunID     = $runId
                                                EndTime   = $null
                                                Arguments = $commandArgs
                                            }

                                            if ( UpdateProcessStatus $process) {
                                                #ExecuteScriptAsync $workingDir $script $commandArgs $runId
                                                $jobQueue.Enqueue([pscustomobject]@{
                                                        WorkingDir  = $workingDir
                                                        Script      = $script 
                                                        CommandArgs = $commandArgs 
                                                        RunId       = $runId
                                                    })
                                                
                                                $commandOutput = @{Success = $True; RunID = $runId; Process = $process}
                                            }
                                            else {
                                                $commandOutput = @{Success = "False"; RunID = $runId; Process = $process}
                                            }
                                        }
                                        
                                    }
                                    else {
                                        try {
                                            $commandArgs = @{}
                                            $request.QueryString.Keys | ForEach-Object {$commandArgs.Add($_, $request.QueryString[$_])}
                                            $results = ExecuteScript -script $script -arguments $([pscustomobject]$commandArgs)
                                            $commandOutput = @{Success = $True; RunID = $runId; Results = $results }
                                        }
                                        catch {
                                            write-verbose "Error $($_.Exception.Message) at $($_.InvocationInfo.PositionMessage)"
                                            @{Success = $false; Error = "UnableToProcess"; Message = "Unable to Process request"}; 
                                            $statusCode = 500
                                        }
                                    }
                                }
                            }
                        }
                        
                    }
                }
            }

            
            if (!$commandOutput) {
                $commandOutput = " "
            }

            Log-AuditMessage "RunID:$runID  StatusCode:$statusCode  Success:$($commandOutput.Success)   Error:$($commandOutput.Error) Message:$($commandOutput.Message)"
            send-response $statusCode $context.Response "JSON" $commandOutput
        }
        catch [Exception] { 
            write-error "Error $($_.Exception.Message) at $($_.InvocationInfo.PositionMessage)" -ErrorAction Continue
            $errorCount = $errorCount + 1
        }
         
    }
}
catch [Exception] { 
    write-error "Error $($_.Exception.Message) at $($_.InvocationInfo.PositionMessage)" -ErrorAction Continue
}
finally {
    $listener.Stop()
    $jobWatcherTimer.stop() 
    Unregister-Event timerEvntId 
}
#endregion