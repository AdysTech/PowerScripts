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
    [string] $primaryGroup = "BUILTIN\Performance Log Users"
)

$DebugPreference = "Continue"
$VerbosePreference = "Continue"

write-verbose "$([datetime]::now.tostring()) Starting $(split-path -Leaf $MyInvocation.MyCommand.Definition ) process - PID : $pid"

$workingDir = split-path -parent $MyInvocation.MyCommand.Definition

#region declare functions
function ExecuteScriptAsync {
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
        $retryCount = $retryCount + 1
        if ($mtx.WaitOne(1000)) {           
            break
        }
    }
    if ($retryCount -eq 6) {
        write-verbose "Unable to hold lock to mark starting job"
        return
    }
    try {
     
        $statusEntries = Get-Content -Raw -Path $path | ConvertFrom-Json
        $process = $statusEntries.Processes | Where-Object {$_.RunId -eq $runId}
        $process.Status = "Processing"
        $statusEntries | ConvertTo-Json -Depth 100 | Out-File $path    
    }
    finally {        
        $mtx.ReleaseMutex()
    }
    #$arg = $arguments | Get-Member -MemberType Properties | ForEach-Object { "-$($_.Name) $($arguments.($_.Name))"}              
    #$scriptresult = Invoke-Expression "& $scriptFileName $arg"
            
    $arguments | & $scriptFileName

    #Set-Content $log $scriptresult
    while ($retryCount -lt 6) {
        $retryCount = $retryCount + 1
        if ($mtx.WaitOne(1000)) {           
            break
        }
    }
    if ($retryCount -eq 6) {
        write-verbose "Unable to hold lock to mark job complete"
        return $false
    }  
    
    try {
  
        $statusEntries = Get-Content -Raw -Path $path | ConvertFrom-Json
        $process = $statusEntries.Processes | Where-Object {$_.RunId -eq $runId}
        $process.EndTime = [DateTime]::Now
        $process.Status = "Completed"
        $statusEntries | ConvertTo-Json -Depth 100 | Out-File $path            
    }
    finally {
        $mtx.ReleaseMutex()
    }
    return $true
}

Function ExecuteScript {
    param (
        [ValidateNotNullOrEmpty()]
        [parameter(Mandatory = $true)]
        [string] $scriptFileName,
        [ValidateNotNullOrEmpty()]
        [parameter(Mandatory = $true)]
        [PSCustomObject] $arguments
    )
                
    #$arg = $arguments | Get-Member -MemberType Properties | ForEach-Object { "-$($_.Name) $($arguments.($_.Name))"}              
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
    #read only transaction.. may not get latest one in a race condition
    if (Test-Path "./ProcessStatus.json") {
        $statusEntries = Get-Content -Raw -Path $(Resolve-path "./ProcessStatus.json") | ConvertFrom-Json
        return $statusEntries
    }
    else {return $null}
}

Function UpdateProcessStatus {
    [CmdletBinding()]
    param (
        [pscustomobject] $process
    )
    $mtx = New-Object System.Threading.Mutex($false, "Global\PSJobMutex")
    
    if (-not $mtx.WaitOne(2000)) {
        write-verbose "Unable to hold lock to UpdateProcessStatus"
        return $false
    }
    try {
        $path = $(Resolve-path ".\ProcessStatus.json")
        $statusEntries = Get-Content -Raw -Path $path | ConvertFrom-Json
        if ($statusEntries -eq $null) {
            $statusEntries = @{Processes = @()}
        }
        else {
            if (-not ($statusEntries.Processes -is [array])) {
                $t = $statusEntries.Processes
                $statusEntries.Processes = @()
                $statusEntries.Processes += $t
            }
        }
        $statusEntries.Processes = @($statusEntries.Processes | Where-Object { $_.RunId -ne $process.RunId})
        $statusEntries.Processes += $process
        $statusEntries | ConvertTo-Json -Depth 100 | Out-File $path
    }
    finally {
        $mtx.ReleaseMutex()
    }
 
    return $true
}

Function ArchiveProcessEntries {
    [CmdletBinding()]
    param (
        [timespan] $TimeToLive
    )
    $mtx = New-Object System.Threading.Mutex($false, "Global\PSJobMutex")
    if (-not $mtx.WaitOne(2000)) {
        write-verbose "Unable to hold lock to ArchiveProcessStatus"
        return $false
    }
    try {
        $path = $(Resolve-path ".\ProcessStatus.json")

            
        $statusEntries = Get-Content -Raw -Path $path | ConvertFrom-Json
        $now = [DateTime]::Now
        write-verbose "Total entries - $($statusEntries.Processes.Count)"
        $archiveEntries = $statusEntries.Processes | Where-Object { $_.EndTime -ne $null -and $_.StartTime + $TimeToLive -lt $now}
        write-verbose "Entries that can be archived - $($archiveEntries.Count)"
        $statusEntries.Processes = $statusEntries.Processes | Where-Object { $_.EndTime -eq $null -or $_.StartTime + $TimeToLive -gt $now}       
        write-verbose "Total entries remaining - $($statusEntries.Processes.Count)"
        $statusEntries | ConvertTo-Json -Depth 100 | Out-File $path
        $archive = ".\ProcessArchive-$($now.ToString('MMM')).json"
        if (-not(test-path $archive)) {
            New-Item $archive -Force -ItemType File | out-null
        }
        else {
            $archiveEntries += (Get-Content -Raw -Path $archive | ConvertFrom-Json).Processes
        }
        $archiveEntries | ConvertTo-Json -Depth 100 | Out-File $archive
    }
    finally {
        $mtx.ReleaseMutex()
    }
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
            $buffer = [System.Text.Encoding]::UTF8.GetBytes(($responseData | ConvertTo-JSON -Depth 100))
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


$jobOutput = [hashtable]::Synchronized(@{})
$jobQueue = [System.Collections.Queue]::Synchronized((New-Object System.Collections.Queue))
$sessionstate = [System.Management.Automation.Runspaces.InitialSessionState]::CreateDefault()

#region setup InitialSessionState
#add all locally defined functions to runspace
$MyInvocation.MyCommand.ScriptBlock.Ast.EndBlock.Statements.Where( {$_ -is [Management.Automation.Language.FunctionDefinitionAst]}).foreach( {
        $funcName = $_.Name
        $sessionstate.Commands.Add((New-Object System.Management.Automation.Runspaces.SessionStateFunctionEntry($funcName, (Get-Content function:\$funcName))))
    })

$sessionstate.Variables.Add(
    (New-Object System.Management.Automation.Runspaces.SessionStateVariableEntry('jobOutput', $jobOutput, $null))
)

$sessionstate.Variables.Add(
    (New-Object System.Management.Automation.Runspaces.SessionStateVariableEntry('jobQueue', $jobQueue, $null))
)

$sessionstate.Variables.Add(
    (New-Object System.Management.Automation.Runspaces.SessionStateVariableEntry('DebugPreference', "Continue", $null))
)

$sessionstate.Variables.Add(
    (New-Object System.Management.Automation.Runspaces.SessionStateVariableEntry('VerbosePreference', "Continue", $null))
)
#endregion


$jobWatchercallback = {     
    $VerbosePreference = "Continue"
    $DebugPreference = "Continue"
    #Write-debug "$([datetime]::Now) Queue Length $($Event.MessageData.jobQueue.Count)"
    $modified = $false
    try {        
        while ($Event.MessageData.jobQueue.Count -gt 0) {                    
            $arguemnts = $Event.MessageData.jobQueue.Dequeue()
            try {                    
                $PSinstance = [PowerShell]::Create()                    
                $PSinstance.AddCommand("ExecuteScriptAsync").AddArgument($arguemnts.WorkingDir).AddArgument($arguemnts.Script).AddArgument($arguemnts.CommandArgs).AddArgument($arguemnts.RunId)
                $PSinstance.RunspacePool = $Event.MessageData.runspacePool
                $Event.MessageData.jobOutput.$("Job$($arguemnts.RunId)") = ([PSCustomObject]@{
                        Runspace   = $PSinstance.BeginInvoke()
                        PowerShell = $PSinstance
                        RunID      = $arguemnts.RunId
                    })
                Write-Verbose "$([datetime]::Now) Started async processing $($arguemnts.RunId) with $($arguemnts.CommandArgs)"
            }
            catch [Exception] { 
                write-verbose "Error starting the RunSpace $($_.Exception.Message) at $($_.InvocationInfo.PositionMessage)"
                $Event.MessageData.jobQueue.Enqueue($arguemnts)
            }                    
        }
        if ( $Event.MessageData.jobOutput.Count -gt 0) {        
            $Event.MessageData.jobOutput.GetEnumerator().Where( {$_.Value.Runspace.IsCompleted}).foreach( {
                    $job = $_.Value
                    Write-Verbose "$([datetime]::Now) Finalize async processing for $($job.RunId)"
                    $log = ".\Logs\$($job.RunID).log"
                    $job.PowerShell.Streams.Debug.ReadAll() | Write-Debug 
                    $entries = New-Object Collections.Generic.List[string]
                    $verb = $job.PowerShell.Streams.Verbose.ReadAll()
                    if ($verb -ne $null) {
                        $entries.Add("Details`n")
                        $entries.Add("----------------------------------------------`n")
                        $verb |foreach-Object {$entries.Add($_.ToString())}
                    }
                    $err = $job.PowerShell.Streams.Error.ReadAll()
                    if ($err -ne $null) {
                        $entries.Add("Errors`n")
                        $entries.Add( "----------------------------------------------`n")
                        $err |foreach-Object {$entries.Add($_.ToString())}
                    }
                    $entries.Add("Output`n")
                    $entries.Add( "-----------------------$($job.PowerShell.InvocationStateInfo.State)-----------------------`n")
                    
                    if ($job.PowerShell.InvocationStateInfo.State -eq 'Completed') {
                        $out = $job.PowerShell.EndInvoke($job.Runspace) 
                    }
                    elseif ($job.PowerShell.InvocationStateInfo.State -eq 'Stopped') {
                        $out = $job.PowerShell.EndStop($job.Runspace) 
                    }
                        
                    if ($out -ne $null) {
                        $out|foreach-Object {if ($_ -ne $null) {[string]$o = $_; $entries.Add($o)}
                        }
                    }
                    $entries | Out-File -FilePath $log -Append | out-null
                    $job.Runspace = $null
                    $job.PowerShell.Dispose()
                    $Event.MessageData.jobOutput.Remove("Job$($job.RunId)")
                })            
        }
    }
    catch [Exception] { 
        write-verbose "Timer callback Error $($_.Exception.Message) at $($_.InvocationInfo.PositionMessage)"
    }
    return $true
}

$listenerCallback = {
    param (
        [ValidateNotNullOrEmpty()]
        [parameter(Mandatory = $true)]
        [string] $workingDir,            
        [ValidateNotNullOrEmpty()]
        [parameter(Mandatory = $true)]
        [System.Net.HttpListener] $listener,
        [ValidateNotNullOrEmpty()]
        [parameter(Mandatory = $true)]
        [string] $primaryGroup
    )

    [long] $requestCount = 0
    [long] $errorCount = 0
    $CommandKeywords = "status;archivejobs;runoutput;canceljob;procstatus;"
    
    try {
        while ($listener.IsListening) {
            try {
                $context = $listener.GetContext()
                $statusCode = 200
                $requestCount = $requestCount + 1
                $runId = [long] ([datetime]::UtcNow - [datetime]::FromBinary(630822816000000000)).TotalSeconds
                #write-verbose "$requestCount  -  Received a request - $runID"
                
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
                        #Write-Verbose "QueryString.HasKeys $($request.QueryString.HasKeys())"
                        if (-not $request.QueryString.HasKeys()) {
                            $commandOutput = @{Success = $false; Error = "InvalidInput"; Message = "No Input provided!! Refer to user guide"}
                        }
                        else {
                            $command = $request.QueryString.Item("command").ToLower()
                            $postArgs = $null
                            $commandArgs = @{}

                            if ($request.HttpMethod -eq "POST" -and $request.HasEntityBody) {
                                try {                            
                                    $reader = New-Object System.IO.StreamReader($request.InputStream)
                                    $arg = $reader.ReadToEnd()
                                    $postArgs = $arg | ConvertFrom-Json
                                }
                                catch {
                                    $commandOutput = @{Success = $false; Error = "InvalidInput"; Message = "Invalid POST data!! Refer to user guide"}
                                }
                                finally {
                                    $reader.Close()
                                }
                            }
                            if (-not ($CommandKeywords.Contains("$command;"))) {
                                $commandOutput = @{Success = $false; Error = "InvalidInput"; Message = "Invalid command selected"}
                                $statusCode = 501;
                            }
                            else {
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
                                    canceljob {                                        
                                        if ( $request.QueryString["runid"] -ne $null) {
                                            Write-Debug "Trying to cancel job RunID: $($request.QueryString["runid"])"
                                            Write-Debug "Job count: $($jobOutput.Count)"
                                            if ( $jobOutput.Count -gt 0) {
                                                $job = $jobOutput.GetEnumerator().Where( {$_.Value.RunID -eq $request.QueryString["runid"]}) | Select-Object -First 1 
                                                $job = $job.Value
                                                if ($job -ne $null) {
                                                    Write-Debug "Got job : $job "
                                                    $job.Runspace = $job.PowerShell.BeginStop($null, $null)

                                                    $process = $(GetProcessStatus).Processes | Where-Object {$_.RunId -eq $request.QueryString["runid"]}
                                                    $process.Status = "Cancelled"
                                                    $process.EndTime = [DateTime]::Now
                                                    UpdateProcessStatus $process 
                                                    $commandOutput = @{Success = $True; RunID = $runId; Process = $process}
                                                }
                                                else {
                                                    $commandOutput = @{Success = $false; RunID = $runId; } 
                                                }
                                            }
                                            else {
                                                $commandOutput = @{Success = $false; RunID = $runId; } 
                                            }
                                        }
                                        else {
                                            $commandOutput = @{Success = $false; Error = "InvalidInput"; Message = "Invalid command selected"}
                                            $statusCode = 501;
                                        }
                                        break
                                    }
                                    archivejobs {
                                        $res = ArchiveProcessEntries $([TimeSpan]::FromHours(24))
                                        $processes = $(GetProcessStatus).Processes
                                        $commandOutput = @{Success = $res; RunID = $runId; Processes = $processes}    
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
                                                if ($postArgs -eq $null) {
                                                    $commandOutput = @{Success = $false; Error = "InvalidInput"; Message = "Invalid POST data!! Refer to user guide"}
                                                }
                                                else {
                                                
                                                    $process = [PsCustomObject] @{
                                                        StartTime = [DateTime]::Now
                                                        User      = $user
                                                        Command   = $command
                                                        Status    = "Queued"
                                                        RunID     = $runId
                                                        EndTime   = $null
                                                        Arguments = $postArgs
                                                    }
                                                
                                                    if ( UpdateProcessStatus $process) {
                                                    
                                                        $jobQueue.Enqueue([pscustomobject]@{
                                                                WorkingDir  = $workingDir
                                                                Script      = $script 
                                                                CommandArgs = $postArgs 
                                                                RunId       = $runId
                                                            })
                                                    
                                                        Write-Debug "$([datetime]::Now) Queued request $runId, Queue Length: $($jobQueue.Count)"
                                                    
                                                        $commandOutput = @{Success = $True; RunID = $runId; Process = $process}
                                                    }
                                                    else {
                                                        $commandOutput = @{Success = $False; RunID = $runId; Process = $process}
                                                    }
                                                }
                                            
                                            }
                                            else {
                                                try {
                                                    $commandArgs = @{}
                                                    $request.QueryString.Keys | ForEach-Object {$commandArgs.Add($_, $request.QueryString[$_])}
                                                    if ($postArgs -ne $null) {
                                                        $postArgs | Get-Member -MemberType NoteProperty |ForEach-Object {
                                                            if (-not $commandArgs.ContainsKey($_.Name)) {
                                                                $commandArgs.Add($_.Name, $postArgs.($_.Name))
                                                            }
                                                        }
                                                    
                                                    }
                                                
                                                    $results = ExecuteScript -script $script -arguments $([pscustomobject]$commandArgs)
                                                    $commandOutput = @{Success = $True; RunID = $runId; Results = @($results) }
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
    
                }
                if (!$commandOutput) {
                    $commandOutput = " "
                }
    
                Log-AuditMessage "RunID:$runID  StatusCode:$statusCode  Success:$($commandOutput.Success)   Error:$($commandOutput.Error) Message:$($commandOutput.Message)"
                send-response $statusCode $context.Response "JSON" $commandOutput
            }
            catch [Exception] { 
                write-verbose "Error $($_.Exception.Message) at $($_.InvocationInfo.PositionMessage)"
                $errorCount = $errorCount + 1
            }
        }    
        
    }
    catch [Exception] { 
        write-verbose "Error $($_.Exception.Message) at $($_.InvocationInfo.PositionMessage)"
    }
}

##clear old abondend jobs
if (Test-Path ".\ProcessStatus.json") {
    $path = $(Resolve-path ".\ProcessStatus.json")
    $statusEntries = Get-Content -Raw -Path $path | ConvertFrom-Json
    $statusEntries.Processes | Where-Object { $_.EndTime -eq $null} | ForEach-Object {$_.Status = 'Cancelled'; $_.EndTime = [DateTime]::Now}
    $statusEntries | ConvertTo-Json -Depth 100 | Out-File $path
}

$MaxActiveProcess = $env:NUMBER_OF_PROCESSORS - 2
$runspacePool = [runspacefactory]::CreateRunspacePool(1, $MaxActiveProcess, $sessionstate, $Host)


$jobWatcherTimer = new-object System.Timers.Timer
$jobWatcherTimer.Interval = 5000
$jobWatcherTimer.AutoReset = $true
$jobWatcherTimer.Enabled = $true
$timerState = [PSObject] @{JobQueue = $jobQueue; jobOutput = $jobOutput; RunspacePool = $runspacePool}
$timerEvent = Register-ObjectEvent -InputObject $jobWatcherTimer -EventName Elapsed -SourceIdentifier  timerEvntId -Action $jobWatchercallback -MessageData $timerState
$ErrorActionPreference = "Stop"

$CurrentPrincipal = New-Object Security.Principal.WindowsPrincipal( [Security.Principal.WindowsIdentity]::GetCurrent())

if ( -not ($currentPrincipal.IsInRole( [Security.Principal.WindowsBuiltInRole]::Administrator ))) {
    write-verbose "This script must be executed from an elevated PowerShell session" -ErrorAction Stop
}

if ($Url.Length -gt 0 -and -not $Url.EndsWith('/')) {
    $Url += "/"
}


[System.Net.AuthenticationSchemes] $Auth = [System.Net.AuthenticationSchemes]::IntegratedWindowsAuthentication
$listener = New-Object System.Net.HttpListener
$prefix = "http://*:$Port/$Url"
$listener.Prefixes.Add($prefix)
$listener.AuthenticationSchemes = $Auth 

try {
    $runspacePool.Open()
    $listener.Start()   
    #start the main listener thread
    $PSinstance = [PowerShell]::Create()    
    $PSinstance.AddScript($listenerCallback) | Out-Null
    $PSinstance.AddParameter("primaryGroup", $primaryGroup)| Out-Null
    $PSinstance.AddParameter("listener", $listener)| Out-Null
    $PSinstance.AddParameter("WorkingDir", $WorkingDir)| Out-Null
    $PSinstance.RunspacePool = $runspacePool
    $listenerLoop = $PSinstance.BeginInvoke()
    
    $jobWatcherTimer.start()
    
    while ($true) {
        Start-Sleep -Milliseconds 1000
        $PSinstance.Streams.Verbose.ReadAll() | Write-Verbose -Verbose
        $PSinstance.Streams.Debug.ReadAll() | Write-Debug 
        $PSinstance.Streams.Verbose.Clear()
        $PSinstance.Streams.Debug.Clear()
    }
}

catch [Exception] { 
    write-verbose "Error $($_.Exception.Message) at $($_.InvocationInfo.PositionMessage)"
}

finally {
    Write-debug "$([datetime]::Now) Shutting down"
    Get-EventSubscriber -Force | Unregister-Event -Force
    $jobWatcherTimer.stop()
    $listener.Stop()
    start-sleep -Milliseconds 100
    $PSinstance.EndInvoke($listenerLoop)    
    $PSinstance.Dispose()
    $runspacePool.Close()
    $runspacePool.Dispose()    
    
    Write-debug "$([datetime]::Now) Good bye.."
}
#endregion