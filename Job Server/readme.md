# PS Job Server
This is based on [Powershell Team's basic server](https://gallery.technet.microsoft.com/Simple-REST-api-for-b04489f1). This version build on top that introduce asynchronous execution, status tracking, and executing really long running powershell scripts on a server, triggered from a browser. It allows for additional arguments to be passed to those scripts from http payload (POST requests). The access can be controlled for a specific AD groups, and the commands to be executed needs to be stored in same folder as this script, and the script name becomes the command variable in the URL. It logs the commands to an audit log, keeps the history of the jobs in a separate file for tracking purposes. A simple bootstrapped html frontend gives an easy UI.

This scripts uses PS run spaces, and timer to emulate a true multi threaded app. The main thread just sets up the run space, and common functions and objects, and goes into a simple wait loop. THe run space thread will take care of HTTP listening and executing additional sync/async processes. The async jobs gets queued up and taken out from a timer thread. 

The process ways to route the debug and verbose messages back to respective streams of master process. So a debug statement in an asynchronous process can be captured. 

### Usage
##### GET /?command=status
Returns a json with all current jobs with their start time, end time and all input parameters, with optional runid parameter if only one job needs to be tracked

##### GET /?command=<command script name>&commandParameterName=param_val
Executes the script defined as command (i.e. command.ps1) passing the arguments synchronously and returns the result as a json object

##### POST /?command=<command script name>
Executes the script defined as command (i.e. command.ps1) passing the arguments asynchronously and returns new run id which can be tracked
 
##### GET /?command=archivejobs
Archives the olf jobs, so that they don't show up in status call