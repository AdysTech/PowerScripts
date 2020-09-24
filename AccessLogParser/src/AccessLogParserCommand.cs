using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Management.Automation;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

namespace AccessLogParser
{
    /// <summary>
    /// <para type="description">Output of Access log parser, it shows total number of lines found, and lines processed</para>
    /// </summary>
    public class ParserOutput
    {
        /// full path of input file
        public string SourceFile { get; internal set; }
        ///full path of output file
        public string TargetFile { get; internal set; }
        ///when did the file parsing started
        public DateTime StartTime { get; internal set; }
        ///when did the parsing ended
        public DateTime EndTime { get; internal set; }
        ///total lines in the input file
        public long TotalLines { get; internal set; }
        ///total lines that parser was able to process
        public long ProcessedLines { get; internal set; }
        public long TotalPoints { get; internal set; }

    }

    /// <summary>
    /// <para type="synopsis">Powershell script to process Access logs for usual web/app server platforms.</para>
    /// <para type="description">Purpose of this PowerShell script is to process Access logs for usual web/app server platforms. This script takes an access log, splits them into fields, then calculates aggregates on those fields.</para>
    /// <para type="description">Usually access logs capture information for each hit, and that is too much information for a long term analysis. The idea is to process the access log, and group the entries at a specific time slots (e.g. 5 min) and aggregate the metrics for long term analysis. It can generate additional metrics based on the aggregated data points (e.g. User concurrency) based on already captured data points.</para>
    /// <para type="description">Copyright: mvadu@adystech</para>
    /// <para type="description">license: MIT</para>
    /// </summary>
    [Cmdlet(VerbsLifecycle.Invoke, "AccessLogParser", SupportsShouldProcess = true)]
    [OutputType(typeof(ParserOutput[]))]
    public class AccessLogParserCommand : PSCmdlet
    {
        #region parameters
        ///Source file to be processed. Mandatory parameter
        [Parameter(
            Mandatory = true,
            Position = 0,
            ValueFromPipeline = true,
            ValueFromPipelineByPropertyName = true)]
        [Alias("fullname")]
        public string Source { get; set; }

        ///Target filename to store the results. Mandatory Parameter
        [Parameter(Mandatory = true)]
        [Alias("output")]
        public string Target { get; set; }

        ///Regular expression to split individual log lines. Mandatory Parameter.
        [Parameter(Mandatory = true)]
        public string SplitBy { get; set; }

        ///List of fields (after they are split) to process. Usage Pattern:<column position>:<Column Name>. Separate multiple fields by a ;. e.g. "3:Timestamp;7:Url;" . Mandatory Parameter.
        ///Should also contain Timestamp column.
        [Parameter(Mandatory = true)]
        [Alias("fieldmap")]
        public string Fields { get; set; }

        ///Hostname from where the log came from.Mandatory
        [Parameter(Mandatory = true)]
        public string HostName { get; set; }

        ///List of fields used as group keys. Usage Pattern: <Column Name>. Separate multiple groups by a ;. e.g. "Url;HttpCode" . Mandatory Parameter.
        [Parameter(Mandatory = true)]
        [Alias("group")]
        public string GroupedColumns { get; set; }

        ///List of fields to aggregate for each groups. Usage Pattern: <Column Name>:<Aggregate Function>. Supports Sum, Average, Count and 90th Percentile. 
        ///Separate multiple fields by a ;. e.g. "PageSize:Sum;TimeTaken:Percentile_90" . Mandatory Parameter.
        ///These can contain simple math as well, e.g. if the value is in bytes, and output needs to be in KB, you can specify PageSize:Sum/1000;
        [Parameter(Mandatory = true)]
        [Alias("aggregate")]
        public string AggregatedColumns { get; set; }


        ///Filters to apply on column values. Usage Pattern: <Column Name>:<reg ex>. Separate multiple filters by a ;. e.g. "Url;jpg$" . Optional Parameter.
        [Parameter(Mandatory = false)]
        [Alias("filter")]
        public string FilterBy { get; set; }

        ///RegEx to extract only part of column values into final result. Usage Pattern: <Column Name>:<reg ex>. Separate multiple filters by a ;;. All capture groups will be concatinated. If all matching entries to be removed sufix with !!. e.g. "TimeTaken:([\d.]+)". Optional Parameter.
        [Parameter(Mandatory = false)]
        public string ColumnExtract { get; set; }


        ///List of calculated columns. Usage Pattern: <Column Name>:<Aggregate Function>. Supports Sum, Average, Count and 90th Percentile. Separate multiple fields by a ;. e.g. "Concurrency=(([HttpCode:Count]/300)*([TimeTaken:Average]/1000))" . Mandatory Parameter.
        [Alias("calculate")]
        [Parameter(Mandatory = false)]
        public string CalculatedColumns { get; set; }

        ///List of sub columns. Usage Pattern: <Original Column Name>:<RegEx with capture groups for each of the sub columns>. Separate multiple fields by a ;;. e.g. "Url:(?<method>[A-Za-z]+) (?<Url>\S*)". If subcolumn name is same as main column its valus is replaced with captured value
        [Parameter(Mandatory = false)]
        public string SubColumns { get; set; }

        ///Time format used in the access log. e.g. "[dd/MMM/yyyy:HH:mm:ss" Mandatory Parameter
        ///If this is split across two columns i.e. contains the delimiter, -fields should contain Date column as well.
        [Parameter(Mandatory = true)]
        [Alias("time")]
        public string InputTimeFormat { get; set; }

        ///Time interval in minutes to collapse the log entries. Default: 1 min.
        [Parameter(Mandatory = false)]
        [Alias("TimeInterval")]
        public int Interval { get; set; }

        ///Time format used for the output file. e.g. "yyyy-MM-dd HH:mm"
        [Parameter(Mandatory = false)]
        [Alias("OutputTimeFormat")]
        public string TargetTimeFormat { get; set; }


        ///Skip number of lines before processing.
        [Parameter(Mandatory = false)]
        [Alias("offset")]
        public int SkipLines { get; set; }


        ///Skip lines matching this regex. Default is any lines starting with /// which is W3C comment lines    
        [Parameter(Mandatory = false)]
        public string SkipLinePattern { get; set; }


        ///Minimum number of hits in a given interval, any interval with less number of total hits will be ignored. Default is 5. 
        [Parameter(Mandatory = false)]
        [Alias("threshold")]
        public int MinHits { get; set; }

        [Parameter(Mandatory = false)]
        public SwitchParameter LogTimeInUTC { get; set; }

        [Parameter(Mandatory = false)]
        public SwitchParameter OutTimeInUTC { get; set; }
        #endregion
        #region constants
        private const string defaultColumnExtract = "Url:!!\\?~(.*)$|;(.*)$";
        private const string defaultTimeFormat = "yyyy-MM-dd HH:mm";
        private const string defaultSkipLinePattern = "^///\\S";
        private const int defaultMinHits = 5;
        private const int defaultInterval = 1;
        private const int flushThreshold = 5000;
        #endregion
        #region fields
        private Regex lineSplitter;
        private Dictionary<string, int> fieldMap;
        private Dictionary<string, List<AggregatedColumn>> aggregateMap;
        private List<ColumnFilterConfiguration> columnFilters;
        private List<CalculatedColumn> calculatedColumns;
        private string inputDateFormat;
        private List<SubColumnsConf> subColumnCollection;
        private List<string> groupByColumns;
        private List<ColumnFilterConfiguration> columnExtracts;
        private Dictionary<string, Task<ParserOutput>> fileProcessors;
        private ConcurrentQueue<LogEntry> logEntries;
        private Regex skipPattern;
        private ConcurrentQueue<OutputRecord> resultEntries;
        #endregion
        public AccessLogParserCommand()
        {
            this.ColumnExtract = defaultColumnExtract;
            this.TargetTimeFormat = defaultTimeFormat;
            this.SkipLinePattern = defaultSkipLinePattern;
            this.MinHits = defaultMinHits;
            this.Interval = defaultInterval;
            fieldMap = new Dictionary<string, int>();
            aggregateMap = new Dictionary<string, List<AggregatedColumn>>();
            columnFilters = new List<ColumnFilterConfiguration>();
            calculatedColumns = new List<CalculatedColumn>();
            subColumnCollection = new List<SubColumnsConf>();
            groupByColumns = new List<string>();
            columnExtracts = new List<ColumnFilterConfiguration>();
            fileProcessors = new Dictionary<string, Task<ParserOutput>>();
            logEntries = new ConcurrentQueue<LogEntry>();
            resultEntries = new ConcurrentQueue<OutputRecord>();
        }


        /// This method gets called once for each cmdlet in the pipeline when the pipeline starts executing
        protected override void BeginProcessing()
        {
            skipPattern = new Regex(SkipLinePattern, RegexOptions.Compiled);
            if (!Fields.Contains("Timestamp"))
            {
                throw new ArgumentException("The specified field map does not contain field 'Timestamp' which is required");
            }
            this.lineSplitter = new Regex(SplitBy, RegexOptions.Compiled);

            if (!Fields.EndsWith(";")) Fields = $"{Fields};";
            foreach (Match field in Regex.Matches(Fields, "(?<index>.*?):(?<column>.*?);"))
            {
                fieldMap.Add(field.Groups["column"].Value, Int32.Parse(field.Groups["index"].Value));
            }
            if (!String.IsNullOrEmpty(SubColumns))
            {
                if (!SubColumns.EndsWith(";;")) SubColumns = $"{SubColumns};;";
                var subcol = new Regex("(?<column>.*?):(?<subcolumns>.*?);;");
                if (!subcol.IsMatch(SubColumns))
                    throw new ArgumentException("Subcolumn definition is incorrect, refer to help for proper format");
                foreach (Match col in subcol.Matches(SubColumns))
                {
                    if (!fieldMap.ContainsKey(col.Groups["column"].Value))
                        throw new ArgumentException($"Error - {col.Groups["column"].Value} can't be used in subcolumns without it being in fieldsmap");
                    var subcolumn = new SubColumnsConf(col.Groups["column"].Value, col.Groups["subcolumns"].Value);
                    foreach (var c in subcolumn.SubColumns)
                    {
                        if (fieldMap.ContainsKey(c) && fieldMap[c] < 0)
                            throw new ArgumentException($"Error - SubColumn {c} is defined more than once!");
                        else if (!fieldMap.ContainsKey(c))
                            fieldMap.Add(c, -1);
                    }
                    subColumnCollection.Add(subcolumn);

                }
            }

            if (!AggregatedColumns.EndsWith(";")) AggregatedColumns = $"{AggregatedColumns};";
            foreach (Match clmn in Regex.Matches(AggregatedColumns, "(?<column>.*?):(?<aggregate>.*?);"))
            {
                if (!fieldMap.ContainsKey(clmn.Groups["column"].Value))
                    throw new ArgumentException($"Error - {clmn.Groups["column"].Value} can't be used in aggregate without it being in fieldsmap");
                if (!aggregateMap.ContainsKey(clmn.Groups["column"].Value))
                    aggregateMap.Add(clmn.Groups["column"].Value, null);
                if (aggregateMap[clmn.Groups["column"].Value] == null)
                {
                    aggregateMap[clmn.Groups["column"].Value] = new List<AggregatedColumn>();
                }
                aggregateMap[clmn.Groups["column"].Value].Add(new AggregatedColumn(clmn.Groups["column"].Value, clmn.Groups["aggregate"].Value));
            }

            if (!String.IsNullOrEmpty(FilterBy))
            {
                if (!FilterBy.EndsWith(";")) FilterBy = $"{FilterBy};";
                foreach (Match clmn in Regex.Matches(FilterBy, "(?<column>.*?):(?<filter>.*?);"))
                {
                    if (!fieldMap.ContainsKey(clmn.Groups["column"].Value))
                        throw new ArgumentException($"Error - {clmn.Groups["column"].Value} can't be used in filter without it being in fieldsmap");
                    columnFilters.Add(new ColumnFilterConfiguration(
                        clmn.Groups["column"].Value,
                        clmn.Groups["filter"].Value,
                        fieldMap[clmn.Groups["column"].Value]));
                }
            }

            var timeparts = lineSplitter.Split(InputTimeFormat);
            if (timeparts.Count() > 1)
            {
                if (!fieldMap.ContainsKey("Date"))
                    throw new ArgumentException("The specified field map does not contain field 'Date' which is required when the -inputTimeFormat spread across two columns");
                var min = Math.Min(fieldMap["Timestamp"], fieldMap["Date"]);
                InputTimeFormat = timeparts[fieldMap["Timestamp"] - min];
                inputDateFormat = timeparts[fieldMap["Date"] - min];
            }
            if (!String.IsNullOrEmpty(CalculatedColumns))
            {
                if (!CalculatedColumns.EndsWith(";")) CalculatedColumns = $"{CalculatedColumns};";
                foreach (Match clmn in Regex.Matches(CalculatedColumns, "(?<column>.*?)=(?<formula>.*?);"))
                {
                    calculatedColumns.Add(new CalculatedColumn(
                        clmn.Groups["column"].Value,
                        clmn.Groups["formula"].Value));
                }
            }


            if (!String.IsNullOrEmpty(GroupedColumns))
            {
                if (!GroupedColumns.EndsWith(";")) GroupedColumns = $"{GroupedColumns};";

                foreach (Match clmn in Regex.Matches(GroupedColumns, "(?<column>.*?);"))
                {
                    if (!fieldMap.ContainsKey(clmn.Groups["column"].Value))
                        throw new ArgumentException($"Error - {clmn.Groups["column"].Value} can't be used in GroupBy without it being in fieldsmap");
                    groupByColumns.Add(clmn.Groups["column"].Value);
                }

            }

            if (!String.IsNullOrEmpty(ColumnExtract))
            {
                if (!ColumnExtract.EndsWith(";;")) ColumnExtract = $"{ColumnExtract};;";

                foreach (Match clmn in Regex.Matches(ColumnExtract, "(?<column>.*?):(?<filter>.*?);;"))
                {
                    if (!fieldMap.ContainsKey(clmn.Groups["column"].Value))
                        throw new ArgumentException($"Error - {clmn.Groups["column"].Value} can't be used in ColumnExtract without it being in fieldsmap");
                    columnExtracts.Add(new ColumnFilterConfiguration(
                        clmn.Groups["column"].Value,
                        clmn.Groups["filter"].Value,
                        fieldMap[clmn.Groups["column"].Value]));
                }
            }
        }
        /// This method will be called for each input received from the pipeline to this cmdlet; if no input is received, this method is not called
        protected override void ProcessRecord()
        {
            if (ShouldProcess("Check if Source Exists"))
            {
                Source = Path.GetFullPath(Source);
                if (!File.Exists(Source))
                {
                    throw new FileNotFoundException($"{Source} not found!!");
                }
            }
            if (ShouldProcess("Parse the Access Log and generate output"))
            {
                if (!fileProcessors.ContainsKey(Source))
                {
                    string filename = Source;
                    WriteVerbose($"Queued {filename}");
                    fileProcessors.Add(filename, Task.Run(() => ParseFileAsync(filename)));
                }
            }
        }

        /// This method will be called once at the end of pipeline execution; if no input is received, this method is not called
        protected override void EndProcessing()
        {
            var headerWritten = false;
            if (fileProcessors.Count() > 0)
            {
                while (fileProcessors.Values.Any(t => !t.IsCompleted))
                {
                    Thread.Sleep(100);
                    LogEntry entry;
                    while (logEntries.TryDequeue(out entry))
                    {
                        switch (entry.Type)
                        {
                            case LogEntrytype.Debug: WriteDebug(entry.Message); break;
                            case LogEntrytype.Verbose: WriteVerbose(entry.Message); break;
                            case LogEntrytype.Information: WriteInformation(entry.Message, null); break;
                            case LogEntrytype.Warning: WriteWarning(entry.Message); break;
                            case LogEntrytype.Error: WriteError(new ErrorRecord(entry.Exception, entry.Message, ErrorCategory.ParserError, null)); break;
                            case LogEntrytype.Progress: WriteProgress(new ProgressRecord(0, "LogParser", entry.Message) { PercentComplete = entry.PercentComplete }); break;
                        }
                    }
                    if (resultEntries.Count > flushThreshold)
                    {
                        WriteResults(!headerWritten, false).Wait();
                        headerWritten = true;
                    }
                }


                foreach (var task in fileProcessors.Values)
                {
                    if (task.IsFaulted)
                    {
                        task.Exception.Handle((x) =>
                            {
                                WriteError(new ErrorRecord(x, "ParserError", ErrorCategory.ParserError, null));
                                return true;
                            });
                    }
                    else if (task.IsCompleted)
                    {
                        var item = task.Result;
                        item.TargetFile = Target;
                        WriteObject(item);
                    }
                }

                WriteResults(!headerWritten, true).Wait();
            }
            WriteVerbose("End!");
        }


        private async Task<ParserOutput> ParseFileAsync(string sourceFile)
        {
            var linecount = SkipLines;
            int processedLine = SkipLines;
            var filename = Path.GetFileName(sourceFile);
            List<Dictionary<string, string>> entries = null;
            DateTime lineTime, lineDate, bucketTime, lastProcessedtime = DateTime.Now;
            var output = new ParserOutput
            {
                SourceFile = sourceFile,
                StartTime = DateTime.Now
            };
            long totalLines = 0;
            try
            {
                totalLines = await CountLines(sourceFile);
            }
            catch (Exception e)
            {
                logEntries.Enqueue(LogEntry.WriteError($"{filename} - Unable to find line count", e));
            }
            logEntries.Enqueue(LogEntry.WriteProgress($"{filename} - Found {totalLines} lines..", 0));
            if (totalLines < SkipLines)
            {
                logEntries.Enqueue(LogEntry.WriteWarning($"{filename} - Found {totalLines} lines, can't skip{SkipLines}"));
                output.EndTime = DateTime.Now;
                return output;
            }
            if (SkipLines > 0)
            {
                logEntries.Enqueue(LogEntry.WriteProgress($"{filename} - skipping {SkipLines} lines..", (int)((double)SkipLines / totalLines * 100)));
            }
            var maxCol = fieldMap.Max(f => f.Value);
            var progressMarker = (int)(totalLines * 0.25);
            foreach (var line in File.ReadLines(sourceFile).Skip(SkipLines))
            {
                linecount++;
                if (skipPattern.IsMatch(line)) continue;
                if (linecount % progressMarker == 0)
                {
                    var completePercent = (int)((double)linecount / totalLines * 100);
                    logEntries.Enqueue(LogEntry.WriteProgress($"{filename} - Parsed {linecount} lines {completePercent}%",completePercent));
                }

                var fields = new Dictionary<string, string>();
                var parts = lineSplitter.Split(line);
                try
                {
                    if (parts.Count() >= maxCol)
                    {
                        foreach (var field in fieldMap)
                        {
                            if (field.Value > 0)
                                fields.Add(field.Key, parts[field.Value]);
                        }
                    }
                    else
                    {
                        continue;
                    }
                }
                catch { continue; }

                if (columnFilters?.Count > 0 &&
                    columnFilters.Any(col =>
                    {
                        //this block skips over the lines that are to be filtered out. 
                        //So a line with negative match gets skipped out, as does no match on non -ve filter.
                        if (col.IsNegativeFilter)
                            return col.Filter.IsMatch(fields[col.ColumnName]);
                        else
                            return !col.Filter.IsMatch(fields[col.ColumnName]);
                    })
                ) continue;
                if (columnExtracts != null)
                {
                    try
                    {
                        foreach (var col in columnExtracts)
                        {
                            //#If the Extract RegEx is to remove all matches then its easy as a simple replace will do the job. 
                            //If the RegEx is to keep the matches, its little intensive.            
                            if (col.IsNegativeFilter)
                            {
                                fields[col.ColumnName] = col.Filter.Replace(fields[col.ColumnName], "").Trim();
                            }
                            else
                            {
                                fields[col.ColumnName] = String.Join("", col.Filter.Matches(fields[col.ColumnName]).Cast<Match>().SelectMany(v => v.Captures.Cast<Capture>().Select(g => g.Value))).Trim();
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        logEntries.Enqueue(LogEntry.WriteWarning($"{filename} -{linecount} - error while filtering {e.Message}"));
                    }
                    //sometimes data may be eligible to be filtered after the extract logic
                    if (columnFilters?.Count > 0 &&
                        columnFilters.Any(col =>
                        {
                            //this block skips over the lines that are to be filtered out. 
                            //So a line with negative match gets skipped out, as does no match on non -ve filter.
                            if (col.IsNegativeFilter)
                                return col.Filter.IsMatch(fields[col.ColumnName]);
                            else
                                return !col.Filter.IsMatch(fields[col.ColumnName]);
                        })
                   ) continue;
                }
                fields.Add("Line", linecount.ToString());
                foreach (var subcol in subColumnCollection)
                {
                    var subs = subcol.columnSplitter.Match(fields[subcol.ColumnName]);
                    foreach (var scol in subcol.SubColumns)
                    {
                        fields[scol] = subs.Groups[scol].Value;
                    }
                }
                if (groupByColumns.Any(col => String.IsNullOrWhiteSpace(fields[col]))) continue;

                if (!DateTime.TryParseExact(fields["Timestamp"], InputTimeFormat, CultureInfo.InvariantCulture, DateTimeStyles.None, out lineTime))
                {
                    logEntries.Enqueue(LogEntry.WriteWarning($"{filename} - {linecount} - count not parse {fields["Timestamp"]} using format {InputTimeFormat}"));
                    continue;
                }

                if (!String.IsNullOrEmpty(inputDateFormat))
                {
                    if (DateTime.TryParseExact(fields["Timestamp"], inputDateFormat, CultureInfo.InvariantCulture, DateTimeStyles.None, out lineDate))
                    {
                        lineTime = lineDate.AddTicks(lineTime.Ticks - DateTime.Today.Ticks);
                    }
                    else
                    {
                        logEntries.Enqueue(LogEntry.WriteWarning($"{filename} - {linecount} - count not parse {fields["Timestamp"]} using format {inputDateFormat}"));
                        continue;
                    }
                }

                //bucket the time stamp into time slots, truncates seconds, set minutes to closest next end slot
                bucketTime = lineTime.AddTicks(-lineTime.Ticks % TimeSpan.TicksPerMinute).AddMinutes(-lineTime.Minute % Interval).AddMinutes(Interval);
                if (LogTimeInUTC.ToBool())
                    bucketTime = DateTime.SpecifyKind(bucketTime, DateTimeKind.Utc);
                else
                    bucketTime = DateTime.SpecifyKind(bucketTime, DateTimeKind.Local);

                fields.Add("Time", bucketTime.ToBinary().ToString());
                if (entries == null)
                {
                    entries = new List<Dictionary<string, string>>();
                    lastProcessedtime = bucketTime;
                }
                if ((bucketTime - lastProcessedtime).TotalMinutes >= Interval)
                {
                    lastProcessedtime = bucketTime;
                    processedLine = linecount - 1;
                    try
                    {
                        output.TotalPoints += await Task.Run(() => ProcessEntries(entries));
                    }
                    catch (Exception e)
                    {
                        logEntries.Enqueue(LogEntry.WriteError($"{filename} - {linecount} - count process entries", e));
                    }
                    entries?.Clear();
                }
                entries.Add(fields);
            }

            logEntries.Enqueue(LogEntry.WriteProgress($"{filename} - Parsed {linecount} lines..", 100));

            try
            {
                output.TotalPoints += await Task.Run(() => ProcessEntries(entries));
            }
            catch (Exception e)
            {
                logEntries.Enqueue(LogEntry.WriteError($"{filename} - {linecount} - count process entries", e));
            }
            entries?.Clear();
            output.TotalLines = totalLines;
            output.ProcessedLines = processedLine;
            output.EndTime = DateTime.Now;
            return output;
        }

        private async Task<long> CountLines(string inputFileName)
        {
            var lineCount = 0L;
            var byteBuffer = new byte[1024 * 1024];
            char eolChar = '\0', currentChar;
            int bytesRead;
            using (var stream = new FileStream(inputFileName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, 1024 * 1024, FileOptions.SequentialScan))
            {
                while ((bytesRead = await stream.ReadAsync(byteBuffer, 0, byteBuffer.Length)) > 0)
                {
                    for (var i = 0; i < bytesRead; i++)
                    {
                        currentChar = (char)byteBuffer[i];
                        if (eolChar != '\0')
                        {
                            if (currentChar == eolChar)
                            {
                                lineCount++;
                            }
                            continue;
                        }
                        else if (currentChar == '\n' || currentChar == '\r')
                        {
                            eolChar = currentChar;
                            lineCount++;
                        }
                    }
                }
            }
            return lineCount;
        }

        private int ProcessEntries(List<Dictionary<string, string>> entries)
        {
            var points = 0;
            if (entries == null) return 0;
            foreach (var grp in entries?.GroupBy(entry => String.Join(";", groupByColumns.Select(col => $"{col}={entry[col]}"))))
            {
                if (grp.Count() >= MinHits)
                {
                    var entry = grp.FirstOrDefault();
                    var result = new OutputRecord();
                    result.Time = DateTime.FromBinary(long.Parse(entry["Time"]));
                    if (OutTimeInUTC.ToBool())
                        result.Time = result.Time.ToUniversalTime();
                    result.HostName = HostName;
                    foreach (var col in groupByColumns)
                    {
                        result.GroupedColumns.Add(col, entry[col]);
                    }

                    foreach (var metric in aggregateMap)
                    {
                        foreach (var aggr in metric.Value)
                        {
                            result.AggregatedColumns.Add(aggr, ProcessValues(grp.Select(g => g[aggr.ColumnName]), aggr));
                        }
                    }
                    foreach (var metric in calculatedColumns)
                    {
                        var formula = metric.Formula.ToLower();
                        foreach (var aggr in result.AggregatedColumns)
                        {
                            formula = formula.Replace($"[{aggr.Key.ColumnName.ToLower()}:{aggr.Key.Formula}]", aggr.Value.ToString());
                        }
                        result.CalculatedColumns.Add(metric, EvaluateFormula(formula));
                    }
                    resultEntries.Enqueue(result);
                    points++;
                }
            }
            return points;
        }

        private double ProcessValues(IEnumerable<string> values, AggregatedColumn aggr)
        {
            double value; string formula = null;
            switch (aggr.Aggregate)
            {
                case Aggregate.Count:
                    value = values.Count();
                    if (aggr.HasFormula)
                        formula = aggr.Formula.Replace("count", value.ToString());
                    break;
                case Aggregate.Max:
                    value = values.Select(e => { double val = 0; double.TryParse(e, out val); return val; }).Max();
                    if (aggr.HasFormula)
                        formula = aggr.Formula.Replace("max", value.ToString());
                    break;
                case Aggregate.Min:
                    value = values.Select(e => { double val = 0; double.TryParse(e, out val); return val; }).Min();
                    if (aggr.HasFormula)
                        formula = aggr.Formula.Replace("min", value.ToString());
                    break;
                case Aggregate.Average:
                    value = values.Select(e => { double val = 0; double.TryParse(e, out val); return val; }).Average();
                    if (aggr.HasFormula)
                        formula = aggr.Formula.Replace("average", value.ToString());
                    break;
                case Aggregate.Sum:
                    value = values.Select(e => { double val = 0; double.TryParse(e, out val); return val; }).Sum();
                    if (aggr.HasFormula)
                        formula = aggr.Formula.Replace("average", value.ToString());
                    break;
                case Aggregate.Percentile:
                    value = values.Select(e => { double val = 0; double.TryParse(e, out val); return val; }).OrderBy(x => x).Skip(values.Count() * aggr.AggregateVariant / 100).FirstOrDefault();
                    if (aggr.HasFormula)
                        formula = aggr.Formula.Replace($"percentile_{aggr.AggregateVariant}", value.ToString());
                    break;
                default: return double.NaN;
            }
            if (aggr.HasFormula)
                return EvaluateFormula(formula);
            else
                return value;
        }

        private double EvaluateFormula(string expression)
        {
            // DataTable table = new DataTable();
            // table.Columns.Add("expression", typeof(string), expression);
            // DataRow row = table.NewRow();
            // table.Rows.Add(row);
            // return double.Parse((string)row["expression"]);
            return Convert.ToDouble(new DataTable().Compute(expression, null));
        }

        private string GetGroupedValues(Dictionary<string, string> entry)
        {
            var grouped = new StringBuilder();
            foreach (var col in groupByColumns)
            {
                grouped.Append($"{col}={entry[col]};");
            }
            return grouped.ToString();
        }

        private async Task<bool> WriteResults(bool writeHeader = false, bool writeAll = false)
        {

            using (StreamWriter outfile = (writeHeader ? new StreamWriter(Path.GetFullPath(Target), append: false) : new StreamWriter(Path.GetFullPath(Target), append: true)))
            {
                while (resultEntries.Count > (writeAll ? 0 : flushThreshold))
                {
                    OutputRecord item;
                    if (resultEntries.TryDequeue(out item))
                    {
                        if (writeHeader)
                        {
                            await outfile.WriteAsync($"\"EndTime\",\"HostName\",");
                            foreach (var col in item.GroupedColumns)
                            {
                                await outfile.WriteAsync($"{col.Key},");
                            }
                            await outfile.WriteLineAsync("\"Metric\",\"Aggregate\",\"AggregateValue\"");
                            writeHeader = false;
                        }
                        foreach (var col in item.AggregatedColumns)
                        {
                            await outfile.WriteAsync($"\"{item.Time.ToString(TargetTimeFormat)}\",\"{item.HostName}\",");
                            foreach (var grp in item.GroupedColumns)
                            {
                                await outfile.WriteAsync($"\"{grp.Value}\",");
                            }
                            await outfile.WriteLineAsync($"\"{col.Key.ColumnName}\",\"{col.Key.Aggregate}\",{col.Value}");
                        }
                        foreach (var col in item.CalculatedColumns)
                        {
                            await outfile.WriteAsync($"\"{item.Time.ToString(TargetTimeFormat)}\",\"{item.HostName}\",");
                            foreach (var grp in item.GroupedColumns)
                            {
                                await outfile.WriteAsync($"\"{grp.Value}\",");
                            }
                            await outfile.WriteLineAsync($"\"{col.Key.ColumnName}\",\"Formula\",{col.Value}");
                        }
                    }
                }
            }
            return true;
        }
    }
}
