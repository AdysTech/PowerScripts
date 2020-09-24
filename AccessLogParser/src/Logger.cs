using System;

namespace AccessLogParser
{
    internal class LogEntry
    {
        private string message;

        internal LogEntrytype Type { get; private set; }
        internal string Message { get { return $"{Timestamp.ToShortTimeString()}: {message}"; } }

        internal Exception Exception { get; private set; }
        internal DateTime Timestamp { get; private set; }
        internal int PercentComplete { get; set; }
        public static LogEntry WriteDebug(string message)
        {
            return new LogEntry
            {
                Timestamp = DateTime.Now,
                Type = LogEntrytype.Debug,
                message = message
            };
        }
        public static LogEntry WriteVerbose(string message)
        {
            return new LogEntry
            {
                Timestamp = DateTime.Now,
                Type = LogEntrytype.Verbose,
                message = message
            };
        }

        public static LogEntry WriteInformation(string message)
        {
            return new LogEntry
            {
                Timestamp = DateTime.Now,
                Type = LogEntrytype.Information,
                message = message
            };
        }

        public static LogEntry WriteWarning(string message)
        {
            return new LogEntry
            {
                Timestamp = DateTime.Now,
                Type = LogEntrytype.Warning,
                message = message
            };
        }

        public static LogEntry WriteError(string message)
        {
            return new LogEntry
            {
                Timestamp = DateTime.Now,
                Type = LogEntrytype.Error,
                message = message,
                Exception = new Exception(message)
            };
        }

        public static LogEntry WriteError(string message, Exception exception)
        {
            return new LogEntry
            {
                Timestamp = DateTime.Now,
                Type = LogEntrytype.Error,
                message = message,
                Exception = exception
            };
        }

        public static LogEntry WriteProgress(string message)
        {
            return new LogEntry
            {
                Timestamp = DateTime.Now,
                Type = LogEntrytype.Progress,
                message = message
            };
        }
        public static LogEntry WriteProgress(string message, int percent)
        {
            return new LogEntry
            {
                Timestamp = DateTime.Now,
                Type = LogEntrytype.Progress,
                message = message,
                PercentComplete = percent
            };
        }
        private LogEntry() { }
    }
    internal enum LogEntrytype
    {
        Debug,
        Verbose,
        Information,
        Warning,
        Error,
        Progress
    }
}