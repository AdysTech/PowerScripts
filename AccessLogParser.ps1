<#
.SYNOPSIS

Powershell script to process Access logs for usual web/app server platforms.

.DESCRIPTION

Purpose of this PowerShell script is to process Access logs for usual web/app server platforms. This script takes an access log, splits them into fields, then calculates aggregates on those fields.

Usually access logs capture information for each hit, and that is too much information for a long term analysis. The idea is to process the access log, and group the entries at a specific time slots (e.g. 5 min) and aggregate the metrics for long term analysis. It can generate additional metrics based on the aggregated data points (e.g. User concurrency) based on already captured data points.

.EXAMPLE

AccessLogParser.ps1 -HostName jboss_host123 -source ".\access_log.2016-05-08.log"  -target "./access_log_analysis.csv" -fields "3:Timestamp;7:Url;10:HttpCode;11:PageSize;12:TimeTaken" -group "Url;HttpCode" -aggregate "HttpCode:Count;PageSize:Sum;TimeTaken:Percentile_90;TimeTaken:Average" -calculate "Concurrency=(([HttpCode:Count]/300)*([TimeTaken:Average]/1000))" -time "[dd/MMM/yyyy:HH:mm:ss" -Interval 5 -split " "

#>

[CmdletBinding()]
param (

    #Source file to be processed. Mandatory parameter
	[parameter(Mandatory=$true)]
    [alias("input")]
    [alias("Path")]
    [string]$source,

    #Target filename to store the results. Mandatory parameter
	[parameter(Mandatory=$true)]
    [alias("out")]
    [alias("output")]
    [string]$target,

    #Regular expression to split individual log lines. Mandatory parameter.
	[parameter(Mandatory=$true)]
    [alias("splitter")]
    [alias("splitby")]
    [string]$split,

    #List of fields (after they are split) to process. Usage Pattern:<column position>:<Column Name>. Seperate multiple fields by a ;. e.g. "3:Timestamp;7:Url;" . Mandatory parameter.
	[parameter(Mandatory=$true)]
    [alias("fieldmap")]
    [string]$fields,

    #Hostname from where the log came from. Optional, but recommended 
    [string]$HostName,

    #List of fields used as group keys. Usage Pattern: <Column Name>. Separate multiple groups by a ;. e.g. "Url;HttpCode" . Mandatory parameter.
    [parameter(Mandatory=$true)]
    [alias("group")]
    [string]$GroupedColumns,

    #List of fields to aggregate for each groups. Usage Pattern: <Column Name>:<Aggregate Function>. Supports Sum, Average, Count and 90th Percentile. Separate multiple fields by a ;. e.g. "PageSize:Sum;TimeTaken:Percentile_90" . Mandatory parameter.
    [parameter(Mandatory=$true)]
    [alias("aggregate")]    
    [string]$AggregatedColumns,

    #List of calculated columns. Usage Pattern: <Column Name>:<Aggregate Function>. Supports Sum, Average, Count and 90th Percentile. Separate multiple fields by a ;. e.g. "Concurrency=(([HttpCode:Count]/300)*([TimeTaken:Average]/1000))" . Mandatory parameter.
    [alias("calculate")]    
    [string]$CalculatedColumns,
    
    #Time format used in the access log. e.g. "[dd/MMM/yyyy:HH:mm:ss" Mandatory parameter
    [parameter(Mandatory=$true)]
    [alias("timeformat")]
    [string]$time,

    #Time interval in minutes to collapse the log entries. Default: 1 min.
    [alias("TimeInterval")]
    [int]$Interval = 1,

    #Time format used for the output file. e.g. "yyyy-MM-dd HH:mm"
    [alias("OutputTimeFormat")]
    [string]$targetTime = "yyyy-MM-dd HH:mm",
	
    #Useful for large access log files. Controls how frequently output buffer must be purged to output file
	[alias("batch")]
    [int]$batchSize = 100000,

    #Skip number of lines before processing.
    [alias("offset")]
    [int]$skipLines = 0,

    #Minimum number of hits in a given interval, any interval with less number of total hits will be ignored. Default is 5. 
    [alias("threshold")]
    [int]$minHits = 5
) 

Begin {
    $startTime = Get-Date


    try {   # Wrap in a try-catch in case we try to add this type twice.
        # Create a class to act on the arrays, PS cmdlets creates an object array, which needs to be coverted
        Add-Type -TypeDefinition @"
        using System;
        //using System.Linq.Dynamic;
        using System.Linq;        
        using System.Collections.Generic;
 
        public static class Aggregator 
        {
            public static List<double> GetValues (System.Object[] values)
            {
                var l = new List<double>();
                double d = 0;
                foreach(var v in values)
                {                    
                    d=0;
                    double.TryParse(v.ToString(),out d); 
                    l.Add(d);
                }
                return l;
            }

            public static double Percentile (System.Object[] rawvalues, int percent)
            {
                var values = GetValues(rawvalues);
                values.Sort();
                int nth = (percent * values.Count()) / 100;
                return values[nth];
            }
            
            public static double StandardDeviation (System.Object[] rawvalues)
            {
                var values = GetValues(rawvalues);
                var avg = values.Average();               
                //Perform the Sum of (value-avg)^2
                double sum = values.Sum(d => (d - avg) * (d - avg));
                //Put it all together
                return Math.Sqrt(sum / values.Count);
            }
            
            public static double Count (System.Object[] rawvalues)
            {
                var values = GetValues(rawvalues);
                return values.Count();
            }
            
            public static double Sum (System.Object[] rawvalues)
            {
                var values = GetValues(rawvalues);
                return values.Sum();
            }

            public static double Average (System.Object[] rawvalues)
            {
                var values = GetValues(rawvalues);
                return values.Average();
            }
        }
"@
    } catch {}


    if (!(Test-Path $source)) { 
		throw "Error: The specified Source storage location does not exist at $source.  
		Please check the arguments and try again."                 
		Exit 
	}    
    
    $source = Resolve-Path $source

    if ($fields -notmatch "Timestamp") { 
		throw 'Error: The specified field map does not contain field "startTime" which is required'
		Exit 
	}      	
	
    $splitter = [regex] $split
    if(!$fields.EndsWith(";")) { $fields = $fields + ";" }
    $fieldsConf = $([regex] "(?<index>.*?):(?<column>.*?);").Matches($fields)

    if(!$AggregatedColumns.EndsWith(";")) { $AggregatedColumns = $AggregatedColumns + ";" }
    $AggregatedColumnsConf = $([regex] "(?<column>.*?):(?<aggregate>.*?);").Matches($AggregatedColumns)


    if(!$CalculatedColumns.EndsWith(";")) { $CalculatedColumns = $CalculatedColumns + ";" }
    $CalculatedColumnsConf = New-Object Collections.Generic.List[PSCustomObject]    
    

    foreach($column in ([regex] "(?<column>.*?)=(?<formula>.*?);").Matches($CalculatedColumns)){
        $formula = $column.Groups["formula"].value
        $CalculatedColumn = $column.Groups["column"].value
        $formulaSplitter =  [regex] "(?<Metric>.*?):(?<Aggregate>.*?)$"   
        foreach($formulaParts in (([regex] "\[(?<column>.*?)\]").Matches($formula) | foreach {$formulaSplitter.Match($_.Groups["column"].Value)})){
            $CalculatedColumnsConf.Add([pscustomobject]@{	
		        CalculatedColumn =$CalculatedColumn 
                Formula = $formula
                Aggregate = $formulaParts.Groups["Aggregate"]
                Metric= $formulaParts.Groups["Metric"]
            })
        }
    }
    



    if(!$GroupedColumns.EndsWith(";")) { $GroupedColumns = $GroupedColumns + ";" }
    $GroupedColumnsConf = New-Object Collections.Generic.List[string]
    foreach($grp in $([regex] "(?<column>.*?);").Matches($GroupedColumns))
    {
        $GroupedColumnsConf.Add($grp.groups["column"].Value)
    }
    
    $targetFile = New-Item $target -Force -ItemType File 
    $valueFilter = [regex] "(.*?)\?|(.*?);"
    $NameSplitter = [regex]","

Function ProcessEntries {
    [CmdletBinding()]
    param (
        [Collections.Generic.List[PSCustomObject]] $entries
	)
    $aggregates = New-Object Collections.Generic.List[PSCustomObject]
    $entryGrps = $entries | Group-Object $GroupedColumnsConf       
    foreach($group in $entryGrps) {  
        foreach($aggr in $AggregatedColumnsConf){
            if($group.Count -ge $minHits){
                $entry = [ordered]@{}
                $entry.Add("StartTime",$entries[0].Timestamp.ToString("$targetTime"))
                    
                if(![string]::IsNullOrEmpty($HostName )) {
                    $entry.Add("HostName",$HostName)
                }

                $keys = $NameSplitter.Split($group.Name)
                for ($i = 0; $i -lt $GroupedColumnsConf.Count; $i++) {
                    $entry.Add( $GroupedColumnsConf[$i],$keys[$i])
                }
                $entry.Add("Metric",$aggr.groups["column"].Value)
                $entry.Add("Aggregate",$aggr.groups["aggregate"].Value)
                $values = $group.Group | Select-Object $aggr.groups["column"].Value | foreach {$_.($aggr.groups["column"].Value)}
                switch ($entry.Aggregate){
                    {($_ -eq 'count')} {
					    $Value =  [Aggregator]::Count($values)
					    break
				    }
                    {($_ -eq 'sum')} {
					    $Value =  [Aggregator]::Sum($values)
					    break
				    }
                    {($_ -eq 'average')} {
					    $Value =  [Aggregator]::Average($values)
					    break
				    }
                    {($_ -eq 'Percentile_90')} {
					    $Value =  [Aggregator]::Percentile($values,90)
					    break
				    }
                }
                $entry.Add("AggregateValue",$Value)
                $aggregates.Add([PSCustomObject]$entry)
            }
        }
                         
        }

        if($CalculatedColumnsConf.Count -gt 0){
            foreach($grp in ($aggregates | Group-Object $GroupedColumnsConf)) {
                $values = $grp.Group
                foreach($calcCol in ($CalculatedColumnsConf | Group-Object Formula)){
            
                    $entry = [ordered]@{}
                
                    $entry.Add("StartTime",$values[0].StartTime)
                    
                    if(![string]::IsNullOrEmpty($HostName )) {
                        $entry.Add("HostName",$HostName)
                    }

                    $keys = $NameSplitter.Split($grp.Name)
                    for ($i = 0; $i -lt $GroupedColumnsConf.Count; $i++) {
                        $entry.Add( $GroupedColumnsConf[$i],$keys[$i])
                    }
                    $entry.Add("Aggregate","Formula")
                    
                    $formula = $calcCol.Name
                    foreach($col in $calcCol.Group){
                        $val = ($values | where {$_.Metric -eq $col.Metric -and $_.Aggregate -eq $col.Aggregate }).AggregateValue
                        $formula = $formula.Replace("[$($col.Metric):$($col.Aggregate)]",$val) 
                    }
                    $entry.Add("Metric",$col.CalculatedColumn)
                    $entry.Add("AggregateValue",(Invoke-Expression $formula))
                    $aggregates.Add([PSCustomObject]$entry)
                }
            }
        }


        return $aggregates
    }
}

process{

    $entries = New-Object Collections.Generic.List[PSCustomObject]
    $summarylist = New-Object Collections.Generic.List[PSCustomObject]
    [datetime]$ts = New-Object DateTime
    $lastWriteTs = $ts
    $linecount = 0
    $completedLines = 0
	foreach ($line in [System.IO.File]::ReadLines($source)) {
        	$linecount = $linecount + 1
            if($linecount -lt $skipLines) { continue }

            $hash=[ordered]@{}
            $parts = $splitter.split($line)
            $hash.add("Line",$linecount)
            foreach($field in $fieldsConf)
            {
                [string]$value = $parts[$field.groups["index"].Value]
                #if($value -match $valueFilter ) {
                if($value.Contains(";") -or $value.Contains("?")){
                    $value = $valueFilter.Match($value).Captures[0].Value
                    $value = $value.TrimEnd("?",";")
                }
                $hash.add($field.groups["column"].Value, $value)
            }
            $out = [PSCustomObject]$hash
            #bucket the time stamp into time slots
            [DateTime]::TryParseExact($out.Timestamp,$time,[System.Globalization.CultureInfo]::InvariantCulture, [System.Globalization.DateTimeStyles]::None, [ref]$ts) | Out-Null
            
            $out.Timestamp = $ts.AddMinutes( - $ts.Minute % $Interval).AddSeconds( - $ts.Second)
            $entries.Add($out) 
            if($entries.Count -gt $batchSize) {
                $cutoff = $entries[$entries.Count-1].Timestamp.AddMinutes(-$Interval)
                $entries | where {$_.Timestamp -le $cutoff} | Group-Object {$_.Timestamp} | ForEach-Object {
                    [Collections.Generic.List[PSCustomObject]]$out1 = ProcessEntries($_.Group)
                    if($out1 -ne $null) { 
                        $summarylist.AddRange($out1) 
                    }
                }
                $entries.RemoveAll({ param($e) $e.Timestamp -le $cutoff}) | Out-Null
                $completedLines = $entries[0].Line - 1
            }
	}

    $entries | Group-Object {$_.Timestamp} | ForEach-Object {
        [Collections.Generic.List[PSCustomObject]]$out1 = ProcessEntries($_.Group)
        if($out1 -ne $null) { 
            $summarylist.AddRange($out1) 
        }
    }
    
    #get last entry from last but one time bucket, and if its too long ago use the last bucket line
    if (([datetime]::now -  $entries[$entries.Count-1].Timestamp).TotalMinutes -gt $Interval){
        $completedLines = $entries[$entries.Count-1].Line
    } else {
        $completedLines = ($entries | where {$_.Timestamp -le ($entries[$entries.Count-1].Timestamp.AddMinutes(-$Interval))} | select -last 1).Line
    }

        

    $summarylist | Export-Csv -NoTypeInformation -Path $target -Encoding ascii -Append



}

end{
	$endTime = Get-Date
	$result = [pscustomobject]@{	
		StartTime = $startTime
		Source = $source
		Lines = if ($completedLines -gt $skipLines) {$completedLines - $skipLines} else {0}
		EndTime = $endTime
		TimeTaken = $($endtime - $startTime).TotalSeconds
        SourceSizeKB = $(Get-Item $source).Length / 1KB
        targetSize = $(Get-Item $target).Length / 1KB
	}
	return $result
}