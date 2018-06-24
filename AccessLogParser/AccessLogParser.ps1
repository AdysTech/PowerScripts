<#
.SYNOPSIS

Powershell script to process Access logs for usual web/app server platforms.

.DESCRIPTION

Purpose of this PowerShell script is to process Access logs for usual web/app server platforms. This script takes an access log, splits them into fields, then calculates aggregates on those fields.

Usually access logs capture information for each hit, and that is too much information for a long term analysis. The idea is to process the access log, and group the entries at a specific time slots (e.g. 5 min) and aggregate the metrics for long term analysis. It can generate additional metrics based on the aggregated data points (e.g. User concurrency) based on already captured data points.
Copyright: mvadu@adystech
license: MIT

.EXAMPLE

AccessLogParser.ps1 -HostName jboss_host123 -source ".\access_log.2017-05-08.log"  -target "./access_log_analysis.csv" -fields "3:Timestamp;7:Url;10:HttpCode;11:PageSize;12:TimeTaken" -filter "Url;jpg$" -group "Url;HttpCode" -aggregate "HttpCode:Count;PageSize:Sum;TimeTaken:Percentile_90;TimeTaken:Average" -calculate "Concurrency=(([HttpCode:Count]/300)*([TimeTaken:Average]/1000))" -time "[dd/MMM/yyyy:HH:mm:ss" -Interval 5 -split " "

#>

[CmdletBinding()]
param (

    #Source file to be processed. Mandatory parameter
    [parameter(Mandatory = $true)]
    [alias("input")]
    [alias("Path")]
    [string]$source,

    #Target filename to store the results. Mandatory parameter
    [parameter(Mandatory = $true)]
    [alias("out")]
    [alias("output")]
    [string]$target,

    #Regular expression to split individual log lines. Mandatory parameter.
    [parameter(Mandatory = $true)]
    [alias("splitter")]
    [alias("splitby")]
    [string]$split,

    #List of fields (after they are split) to process. Usage Pattern:<column position>:<Column Name>. Separate multiple fields by a ;. e.g. "3:Timestamp;7:Url;" . Mandatory parameter.
    #Should also contain Timestamp column.
    [parameter(Mandatory = $true)]
    [alias("fieldmap")]
    [string]$fields,

    #Hostname from where the log came from. Optional, but recommended 
    [string]$HostName,

    #List of fields used as group keys. Usage Pattern: <Column Name>. Separate multiple groups by a ;. e.g. "Url;HttpCode" . Mandatory parameter.
    [parameter(Mandatory = $true)]
    [alias("group")]
    [string]$GroupedColumns,

    #List of fields to aggregate for each groups. Usage Pattern: <Column Name>:<Aggregate Function>. Supports Sum, Average, Count and 90th Percentile. 
    #Separate multiple fields by a ;. e.g. "PageSize:Sum;TimeTaken:Percentile_90" . Mandatory parameter.
    #These can contain simple math as well, e.g. if the value is in bytes, and output needs to be in KB, you can specify PageSize:Sum/1000;
    [parameter(Mandatory = $true)]
    [alias("aggregate")]    
    [string]$AggregatedColumns,

    
    #Filters to apply on column values. Usage Pattern: <Column Name>:<reg ex>. Separate multiple filters by a ;. e.g. "Url;jpg$" . Optional parameter.
    [alias("filter")]
    [string]$Filters,

    #RegEx to extract only part of URL. Usefule if the URL has any session ID etc. By default any query parameters are moved e.g. default is "\?(.*)$|;(.*)$" . Optional parameter.
    [string]$UrlExtract = "\?(.*)$|;(.*)$",

    #List of calculated columns. Usage Pattern: <Column Name>:<Aggregate Function>. Supports Sum, Average, Count and 90th Percentile. Separate multiple fields by a ;. e.g. "Concurrency=(([HttpCode:Count]/300)*([TimeTaken:Average]/1000))" . Mandatory parameter.
    [alias("calculate")]    
    [string]$CalculatedColumns,
    
    #Time format used in the access log. e.g. "[dd/MMM/yyyy:HH:mm:ss" Mandatory parameter
    #If this is split across two columns i.e. contains the delimiter, -fields should contain Date column as well.
    [parameter(Mandatory = $true)]
    [alias("timeformat")]
    [string]$time,

    #Time interval in minutes to collapse the log entries. Default: 1 min.
    [alias("TimeInterval")]
    [int]$Interval = 1,

    #Time format used for the output file. e.g. "yyyy-MM-dd HH:mm"
    [alias("OutputTimeFormat")]
    [string]$targetTime = "yyyy-MM-dd HH:mm",
	

    #Skip number of lines before processing.
    [alias("offset")]
    [int]$skipLines = 0,

    #Minimum number of hits in a given interval, any interval with less number of total hits will be ignored. Default is 5. 
    [alias("threshold")]
    [int]$minHits = 5
) 



Begin {
    $startTime = Get-Date


    try {
        # Wrap in a try-catch in case we try to add this type twice.
        # Create a class to act on the arrays, PS cmdlets creates an object array, which needs to be coverted
        Add-Type -TypeDefinition @"
        using System;
        //using System.Linq.Dynamic;
        using System.Linq;        
        using System.Collections.Generic;
 
        public static class Aggregator 
        {

            public static double Percentile (double[] values, int percent)
            {
                Array.Sort(values);
                int nth = (percent * values.Length) / 100;
                return values[nth];
            }
            
            public static double StandardDeviation (double[] values)
            {
                var avg = values.Average();               
                //Perform the Sum of (value-avg)^2
                double sum = values.Sum(d => (d - avg) * (d - avg));
                //Put it all together
                return Math.Sqrt(sum / values.Length);
            }
            
            public static double Count (double[] values)
            {
                return values.Length;
            }
            
            public static double Sum (double[] values)
            {
                return values.Sum();
            }

            public static double Average (double[] values)
            {
                return values.Average();
            }
        }
"@
    }
    catch {}


    if (!(Test-Path $source)) { 
        throw "Error: The specified Source storage location does not exist at $source.  
		Please check the arguments and try again."                 
        Exit 
    }    
    
    $source = Resolve-Path $source

    if ($fields -notmatch "Timestamp") { 
        throw 'Error: The specified field map does not contain field "Timestamp" which is required'
        Exit 
    }      	
	
    $splitter = new-object System.Text.RegularExpressions.Regex ( $split, [System.Text.RegularExpressions.RegexOptions]::Compiled)


    $fieldsConf = @{}
    if (!$fields.EndsWith(";")) { $fields = $fields + ";" }
    foreach ($field in $($([regex] "(?<index>.*?):(?<column>.*?);").Matches($fields))) {
        $fieldsConf.Add($field.groups["index"].Value, $field.groups["column"].Value)
    }

    if (!$AggregatedColumns.EndsWith(";")) { $AggregatedColumns = $AggregatedColumns + ";" }
    $AggregatedColumnsConf = $([regex] "(?<column>.*?):(?<aggregate>.*?);").Matches($AggregatedColumns)


    if (!([String]::IsNullOrEmpty($Filters))) {
        if (!$Filters.EndsWith(";")) { $Filters = "$Filters;" }
        $FiltersConf = New-Object Collections.Generic.List[PSCustomObject]    
        foreach ($column in ([regex] "(?<column>.*?):(?<filter>.*?);").Matches($Filters)) {
            $columnName = $column.Groups["column"].value
            if ($fields -notmatch "`:$columnName") { 
                throw "Error: The column:$columnName is not defined as part of -fields parameter"
                Exit 
            }
        
            $index = [int] ($fieldsConf.keys | Where-Object {$fieldsConf[$_] -eq $columnName})
            try {
                $filter = new-object System.Text.RegularExpressions.Regex ($column.Groups["filter"].value, [System.Text.RegularExpressions.RegexOptions]::Compiled)
            }
            catch {
                throw "Error: The filter defined for column:$columnName is not not a valid regex"
                Exit 
            }

            $FiltersConf.Add([pscustomobject]@{	
                    Column = $columnName
                    Filter = $filter
                    Index  = [int] $index
                })
        }
    }

    #if date and time are split in two columns, adjust the time format, and build date format
    $timeParts = $splitter.split($time)
    if ($timeParts.count -gt 1) {
        $dateIndex = [int] ($fieldsConf.keys | Where-Object {$fieldsConf[$_] -eq "Date"})
        if ($dateIndex -eq $null) {
            throw 'Error: The specified field map does not contain field "Date" which is required when the -time format spread across two columns'
            Exit 
        }
        $timeIndex = [int] ($fieldsConf.keys | Where-Object {$fieldsConf[$_] -eq "Timestamp"})
        $min = if ($dateIndex -gt $timeIndex) { $timeIndex } else { $dateIndex }
        $dateIndex = $dateIndex - $min
        $timeIndex = $timeIndex - $min
        $time = $timeParts[$timeIndex]
        $date = $timeParts[$dateIndex]
    }

    if (!$CalculatedColumns.EndsWith(";")) { $CalculatedColumns = $CalculatedColumns + ";" }
    $CalculatedColumnsConf = New-Object Collections.Generic.List[PSCustomObject]    
    

    foreach ($column in ([regex] "(?<column>.*?)=(?<formula>.*?);").Matches($CalculatedColumns)) {
        $formula = $column.Groups["formula"].value
        $CalculatedColumn = $column.Groups["column"].value
        $formulaSplitter = [regex] "(?<Metric>.*?):(?<Aggregate>.*?)$"   
        foreach ($formulaParts in (([regex] "\[(?<column>.*?)\]").Matches($formula) | ForEach-Object {$formulaSplitter.Match($_.Groups["column"].Value)})) {
            $CalculatedColumnsConf.Add([pscustomobject]@{	
                    CalculatedColumn = $CalculatedColumn 
                    Formula          = $formula
                    Aggregate        = $formulaParts.Groups["Aggregate"]
                    Metric           = $formulaParts.Groups["Metric"]
                })
        }
    }
    
    if (!$GroupedColumns.EndsWith(";")) { $GroupedColumns = $GroupedColumns + ";" }
    $GroupedColumnsConf = New-Object Collections.Generic.List[string]
    foreach ($grp in $([regex] "(?<column>.*?);").Matches($GroupedColumns)) {
        $GroupedColumnsConf.Add($grp.groups["column"].Value)
    }
    
    $targetFile = New-Item $target -Force -ItemType File 
    $valueFilter = new-object System.Text.RegularExpressions.Regex ($UrlExtract, [System.Text.RegularExpressions.RegexOptions]::Compiled) 
    $NameSplitter = new-object System.Text.RegularExpressions.Regex (",", [System.Text.RegularExpressions.RegexOptions]::Compiled) 

    function ProcessValues {
        Param(
            [Parameter(
                Position = 0, 
                Mandatory = $true, 
                ValueFromPipeline = $true,
                ValueFromPipelineByPropertyName = $true)
            ]
            [double[]]$values,
            [Parameter(
                Position = 1, 
                Mandatory = $true )]
            [string] $formula
        )
        
        process {
            $aggregate = $formula
            switch ($aggregate) {
                {($_ -match 'count')} {
                    $Value = [Aggregator]::Count($values)
                    $formula = $formula.Replace('count', $Value)
                    break
                }
                {($_ -match 'sum')} {
                    $Value = [Aggregator]::Sum($values)
                    $formula = $formula.Replace('sum', $Value)
                    break
                }
                {($_ -match 'average')} {
                    $Value = [Aggregator]::Average($values)
                    $formula = $formula.Replace('average', $Value)
                    break
                }
                {($_ -match 'percentile')} {
                    $percent = [int] $_.Substring($_.IndexOf('percentile_') + 'percentile_'.Length, 2)
                    $Value = [Aggregator]::Percentile($values, $percent)
                    $formula = $formula.Replace("percentile_$percent", $Value)
                    break
                }
            }
            return (Invoke-Expression $formula)
        }
    }


    Function ProcessEntries {
        [CmdletBinding()]
        param (
            [Collections.Generic.List[PSCustomObject]] $entries
        )
        $aggregates = New-Object Collections.Generic.List[PSCustomObject]
        $entryGrps = $entries | Group-Object $GroupedColumnsConf       
        foreach ($group in $entryGrps) {  
            foreach ($aggr in $AggregatedColumnsConf) {
                if ($group.Count -ge $minHits) {
                    try {
                        $entry = [ordered]@{}
                        $entry.Add("StartTime", $entries[0].Timestamp.ToString("$targetTime"))
                    
                        if (![string]::IsNullOrEmpty($HostName )) {
                            $entry.Add("HostName", $HostName)
                        }

                        $keys = $NameSplitter.Split($group.Name)
                        for ($i = 0; $i -lt $GroupedColumnsConf.Count; $i++) {
                            $entry.Add( $GroupedColumnsConf[$i], $keys[$i])
                        }
                        $entry.Add("Metric", $aggr.groups["column"].Value)
                        $entry.Add("Aggregate", $aggr.groups["aggregate"].Value)
                        $value = , ($group.Group | Select-Object $aggr.groups["column"].Value | ForEach-Object {$($_.($aggr.groups["column"].Value)) -as [double]}) | ProcessValues -formula $($entry.Aggregate.ToLower())
                        $entry.Add("AggregateValue", $Value)
                        $aggregates.Add([PSCustomObject]$entry)
                    }
                    catch [Exception] { 
                        Write-Host "Error $($_.Exception.Message) at $($_.InvocationInfo.PositionMessage)"
                    }
                }
            }
                         
        }

        if ($CalculatedColumnsConf.Count -gt 0) {
            foreach ($grp in ($aggregates | Group-Object $GroupedColumnsConf)) {
                $values = $grp.Group
                foreach ($calcCol in ($CalculatedColumnsConf | Group-Object Formula)) {
                    try {
                        $entry = [ordered]@{}
                
                        $entry.Add("StartTime", $values[0].StartTime)
                    
                        if (![string]::IsNullOrEmpty($HostName )) {
                            $entry.Add("HostName", $HostName)
                        }

                        $keys = $NameSplitter.Split($grp.Name)
                        for ($i = 0; $i -lt $GroupedColumnsConf.Count; $i++) {
                            $entry.Add( $GroupedColumnsConf[$i], $keys[$i])
                        }
                        $entry.Add("Aggregate", "Formula")
                    
                        $formula = $calcCol.Name
                        foreach ($col in $calcCol.Group) {
                            $val = ($values | Where-Object {$_.Metric -eq $col.Metric -and $_.Aggregate -eq $col.Aggregate }).AggregateValue
                            $formula = $formula.Replace("[$($col.Metric):$($col.Aggregate)]", $val) 
                        }
                        $entry.Add("Metric", $col.CalculatedColumn)
                        $entry.Add("AggregateValue", (Invoke-Expression $formula))
                        $aggregates.Add([PSCustomObject]$entry)
                    }
                    catch [Exception] { 
                        Write-Host "Error $($_.Exception.Message) at $($_.InvocationInfo.PositionMessage)"
                    }
                }
            }
        }


        return $aggregates
    }
}

process {

    $entries = New-Object Collections.Generic.List[PSCustomObject]
    $summarylist = New-Object Collections.Generic.List[PSCustomObject]
    [datetime]$ts = New-Object DateTime
    [datetime]$dt = New-Object DateTime
    $lastWriteTs = $ts
    $linecount = 0
    $completedLines = 0
    foreach ($line in [System.IO.File]::ReadLines($source)) {
        $linecount = $linecount + 1
        if ($linecount -lt $skipLines) { continue }

        $hash = [ordered]@{}
        $parts = $splitter.split($line)

        if ($FiltersConf.Count -gt 0) {
            $filterResult = $FiltersConf | ForEach-Object { if ($_.Filter.Match($parts[$_.Index]).Success) {1} else {0}} | Measure-Object -sum | Select-Object Sum, Count
            
            if ($filterResult.Sum -ne $filterResult.Count) { continue }
        }

        $hash.add("Line", $linecount) 
            
        foreach ($field in $fieldsConf.Keys) {
            $hash.add($fieldsConf[$field], $parts[$field])
        }
                        			

        $out = [PSCustomObject]$hash
        $out.Url = $out.Url -replace $valueFilter

        #bucket the time stamp into time slots
        [DateTime]::TryParseExact($out.Timestamp, $time, [System.Globalization.CultureInfo]::InvariantCulture, [System.Globalization.DateTimeStyles]::None, [ref]$ts) | Out-Null
            
        if ($dateIndex -ne $null) {                
            [DateTime]::TryParseExact($out.Date, $date, [System.Globalization.CultureInfo]::InvariantCulture, [System.Globalization.DateTimeStyles]::None, [ref]$dt) | Out-Null
            $ts = $dt.AddTicks( $ts.Ticks - [DateTime]::Today.Ticks)

        }

        $out.Timestamp = $ts.AddMinutes( - $ts.Minute % $Interval).AddSeconds( - $ts.Second)
        $entries.Add($out) 

        if ( ($entries[$entries.Count - 1].Timestamp - $entries[0].Timestamp).TotalMinutes -gt $Interval) {
            $cutoff = $entries[0].Timestamp.AddMinutes($Interval)
            $entries | Where-Object {$_.Timestamp -le $cutoff} | Group-Object {$_.Timestamp} | ForEach-Object {
                try {
                    [Collections.Generic.List[PSCustomObject]]$out1 = ProcessEntries($_.Group)
                    if ($out1 -ne $null) { 
                        $summarylist.AddRange($out1) 
                    }
                }
                catch [Exception] { 
                    Write-Host "Error $($_.Exception.Message) at $($_.InvocationInfo.PositionMessage)" 
                }
            }
            $entries.RemoveAll( { param($e) $e.Timestamp -le $cutoff}) | Out-Null
            if ($summarylist.Count -gt 1024) {
                $summarylist | Export-Csv -NoTypeInformation -Path $target -Encoding ascii -Append
                $summarylist.Clear()
            }
            $completedLines = $entries[0].Line - 1
        }
    }

    $entries | Group-Object {$_.Timestamp} | ForEach-Object {
        try {
            [Collections.Generic.List[PSCustomObject]]$out1 = ProcessEntries($_.Group)
            if ($out1 -ne $null) { 
                $summarylist.AddRange($out1) 
            }
        }
        catch [Exception] { 
            Write-Host "Error $($_.Exception.Message) at $($_.InvocationInfo.PositionMessage)" 
        }
    }
    
    #get last entry from last but one time bucket, and if its too long ago use the last bucket line
    if (([datetime]::now - $entries[$entries.Count - 1].Timestamp).TotalMinutes -gt $Interval) {
        $completedLines = $entries[$entries.Count - 1].Line
    }
    else {
        $completedLines = ($entries | Where-Object {$_.Timestamp -le ($entries[$entries.Count - 1].Timestamp.AddMinutes( - $Interval))} | Select-Object -last 1).Line
    }

        

    $summarylist | Export-Csv -NoTypeInformation -Path $target -Encoding ascii -Append



}

end {
    $endTime = Get-Date
    $result = [pscustomobject]@{	
        StartTime    = $startTime
        Source       = $source
        Lines        = if ($completedLines -gt $skipLines) {$completedLines - $skipLines} else {0}
        EndTime      = $endTime
        TimeTaken    = $($endtime - $startTime).TotalSeconds
        SourceSizeKB = $(Get-Item $source).Length / 1KB
        targetSize   = $(Get-Item $target).Length / 1KB
    }
    return $result
}