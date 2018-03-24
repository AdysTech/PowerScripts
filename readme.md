# Power Scripts
This repo is a collection PowerShell scripts to parse the various forms of plain text logs (e.g. apache access log, application error log) and run analytics to generate metrics.
The metrics are saved as csv files, which can be imported into InfluxDB and visualized in grafana. 

### Scripts

#### PS Job Server
This is based on [Powershell Team's basic server](https://gallery.technet.microsoft.com/Simple-REST-api-for-b04489f1). This version build on top that introduce asynchronous execution, status tracking, and executing really long running powershell scripts on a server, triggered from a browser. It allows for additional arguments to be passed to those scripts from http payload (POST requests). The access can be controlled for a specific AD groups, and the commands to be executed needs to be stored in same folder as this script, and the script name becomes the command variable in the URL. It logs the commands to an audit log, keeps the history of the jobs in a separate file for tracking purposes. A simple bootstrapped html frontend gives an easy UI.

##### Usage
####### GET /?command=status
Returns a json with all current jobs with their start time, end time and all input parameters, with optional runid parameter if only one job needs to be tracked

####### GET /?command=<command script name>&commandParameterName=param_val
Executes the script defined as command (i.e. command.ps1) passing the arguments synchronously and returns the result as a json object

####### POST /?command=<command script name>
Executes the script defined as command (i.e. command.ps1) passing the arguments asynchronously and returns new run id which can be tracked
 
####### GET /?command=archivejobs
Archives the olf jobs, so that they don't show up in status call


#### Access Log Analysis
Purpose of this PowerShell script is to process Access logs for usual web/app server platforms. This script takes an access log, splits them into fields, then calculates aggregates on those fields.
Usually access logs capture information for each hit, and that is too much information for a long term analysis. The idea is to process the access log, and group the entries at a specific time slots (e.g. 5 min) and aggregate the metrics for long term analysis. It can generate additional metrics based on the aggregated data points (e.g. User concurrency) based on already captured data points.

##### Usage

`AccessLogParser.ps1 -HostName jboss_host123 -source ".\access_log.2016-05-08.log"  -target "./access_log_analysis.csv" -fields "3:Timestamp;7:Url;10:HttpCode;11:PageSize;12:TimeTaken" -group "Url;HttpCode" -aggregate "HttpCode:Count;PageSize:Sum;TimeTaken:Percentile_90;TimeTaken:Average" -calculate "Concurrency=(([HttpCode:Count]/300)*([TimeTaken:Average]/1000))" -time "[dd/MMM/yyyy:HH:mm:ss" -Interval 5 -split " "`
