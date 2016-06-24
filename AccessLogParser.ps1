[CmdletBinding()]
param (
    [switch] $TestMode = $false,

    #File paths and extensions
	[parameter(Mandatory=$true)]
    [alias("input")]
    [alias("Path")]
    [string]$source,

	[parameter(Mandatory=$true)]
    [alias("out")]
    [alias("output")]
    [string]$target = "./aceess_log_analysis.csv",

	[parameter(Mandatory=$true)]
    [alias("splitter")]
    [alias("splitby")]
    [string]$split,

	[parameter(Mandatory=$true)]
    [alias("fieldmap")]
    [string]$fields,

    [string]$HostName,

    
    [parameter(Mandatory=$true)]
    [alias("group")]
    [string]$GroupedColumns,

    [parameter(Mandatory=$true)]
    [alias("aggregate")]    
    [string]$AggregatedColumns,


    [parameter(Mandatory=$true)]
    [alias("timeformat")]
    [string]$time,

    [alias("TimeInterval")]
    [int]$Interval = 1,

    [alias("OutputTimeFormat")]
    [string]$targetTime = "yyyy-MM-dd HH:mm",
	
	[alias("batch")]
    [int]$batchSize = 100000,

    [alias("offset")]
    [int]$skipLines = 0,

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