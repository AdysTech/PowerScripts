# Access Log Analysis
Purpose of this PowerShell script is to process Access logs for usual web/app server platforms. This script takes an access log, splits them into fields, then calculates aggregates on those fields.
Usually access logs capture information for each hit, and that is too much information for a long term analysis. The idea is to process the access log, and group the entries at a specific time slots (e.g. 5 min) and aggregate the metrics for long term analysis. It can generate additional metrics based on the aggregated data points (e.g. User concurrency) based on already captured data points.

### Usage

`AccessLogParser.ps1 -HostName jboss_host123 -source ".\access_log.2016-05-08.log"  -target "./access_log_analysis.csv" -fields "3:Timestamp;7:Url;10:HttpCode;11:PageSize;12:TimeTaken" -filter "Url;jpg$" -group "Url;HttpCode" -aggregate "HttpCode:Count;PageSize:Sum;TimeTaken:Percentile_90;TimeTaken:Average" -calculate "Concurrency=(([HttpCode:Count]/300)*([TimeTaken:Average]/1000))" -time "[dd/MMM/yyyy:HH:mm:ss" -Interval 5 -split " "`

### PARAMETERS
####    -source <String>
        Source file to be processed. Mandatory parameter

####    -target <String>
        Target filename to store the results. Mandatory parameter

####    -split <String>
        Regular expression to split individual log lines. Mandatory parameter.

####    -fields <String>
        List of fields (after they are split) to process. Usage Pattern:<column position>:<Column Name>. Separate
        multiple fields by a ;. e.g. "3:Timestamp;7:Url;" . Mandatory parameter. Should also contain Timestamp column.

####    -HostName <String>
        Hostname from where the log came from. Optional, but recommended

####    -GroupedColumns <String>
        List of fields used as group keys. Usage Pattern: <Column Name>. Separate multiple groups by a ;. e.g.
        "Url;HttpCode" . Mandatory parameter.

####    -AggregatedColumns <String>
        List of fields to aggregate for each groups. Usage Pattern: <Column Name>:<Aggregate Function>. Supports Sum,
        Average, Count and 90th Percentile. Separate multiple fields by a ;. e.g.
        "PageSize:Sum;TimeTaken:Percentile_90" . Mandatory parameter. 
	These can contain simple math as well, e.g. if the value is in bytes, and output needs to be in KB, 
	it could be expressed as `PageSize:Sum/1000`;

####    -Filters <String>
        Filters to apply on column values. Usage Pattern: <Column Name>:<reg ex>. Separate multiple filters by a ;.
        e.g. "Url;jpg$" . Optional parameter.

####    -CalculatedColumns <String>
        List of calculated columns. Usage Pattern: <Column Name>:<Aggregate Function>. Supports Sum, Average, Count
        and 90th Percentile. Separate multiple fields by a ;. e.g.
        "Concurrency=(([HttpCode:Count]/300)*([TimeTaken:Average]/1000))" . Mandatory parameter.

####    -time <String>
        Time format used in the access log. e.g. "[dd/MMM/yyyy:HH:mm:ss" Mandatory parameter. 
		If this is split across two columns i.e. contains the delimiter, -fields should contain Date column as well.

####    -Interval <Int32>
        Time interval in minutes to collapse the log entries. Default: 1 min.

####    -targetTime <String>
        Time format used for the output file. e.g. "yyyy-MM-dd HH:mm"

####    -batchSize <Int32>
        Useful for large access log files. Controls how frequently output buffer must be purged to output file

####    -skipLines <Int32>
        Skip number of lines before processing.

####    -minHits <Int32>
        Minimum number of hits in a given interval, any interval with less number of total hits will be ignored.
        Default is 5.

####    -UrlExtract <String>
        RegEx to extract only part of URL. Useful if the URL has any session ID etc. By default any query parameters are moved e.g. default is "\?(.*)$|;(.*)$" . Optional parameter. This RegExp is ap;lied on the URL and anything that matches will be excluded from the rest of the processing (e.g. grouping etc)