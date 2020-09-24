using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace AccessLogParser
{
    internal class ColumnFilterConfiguration
    {
        public string ColumnName { get; private set; }
        public Regex Filter { get; private set; }
        public int ColumnIndex { get; private set; }
        public bool IsNegativeFilter { get; private set; }
        public ColumnFilterConfiguration(string Column, string filter, int index)
        {
            this.ColumnName = Column;

            this.ColumnIndex = index;
            if (filter.StartsWith("!!"))
            {
                this.IsNegativeFilter = true;
                this.Filter = new Regex(filter.Substring(2), RegexOptions.Compiled);
            }
            else
            {
                this.IsNegativeFilter = false;
                this.Filter = new Regex(filter, RegexOptions.Compiled);
            }
        }
    }

    internal class SubColumnsConf
    {
        public string ColumnName { get; private set; }
        public Regex columnSplitter { get; private set; }
        public ReadOnlyCollection<string> SubColumns { get { return _subcolumns.AsReadOnly(); } }
        private List<string> _subcolumns;
        public SubColumnsConf(string Column, string subcolumndef)
        {
            this.ColumnName = Column;
            this.columnSplitter = new Regex(subcolumndef, RegexOptions.Compiled);
            this._subcolumns = new List<string>();
            this._subcolumns.AddRange(this.columnSplitter.GetGroupNames().Skip(1));
            if (this._subcolumns.Count == 0)
                throw new ArgumentException($"Regex defined for column:{ColumnName} is not generating any sub columns. Refer to help");
        }
    }
    internal class CalculatedColumn
    {
        public string ColumnName { get; private set; }
        public string Formula { get; private set; }
        public string Aggregate { get; private set; }
        public string Metric { get; private set; }

        public CalculatedColumn(string Column, string formula)
        {
            this.ColumnName = Column;
            this.Formula = formula;
        }
    }
    internal enum Aggregate
    {
        Count,
        Min,
        Max,
        Sum,
        Average,
        Percentile
    }
    internal class AggregatedColumn
    {
        public string ColumnName { get; private set; }
        public Aggregate Aggregate { get; private set; }
        public int AggregateVariant { get; private set; }
        public string Formula { get; private set; }
        public bool HasFormula { get; private set; }
        public AggregatedColumn(string Column, string aggrt)
        {
            this.ColumnName = Column;
            Aggregate aggregate;
            var a = Regex.Match(aggrt, "(?<aggregate>[A-Za-z_0-9]+)").Groups["aggregate"].Value;

            if (!Enum.TryParse(a, true, out aggregate))
            {
                if (aggrt.IndexOf("Percentile_", StringComparison.OrdinalIgnoreCase) >= 0)
                {
                    this.AggregateVariant = Int32.Parse(a.Substring(a.Length - 2));
                    this.Aggregate = Aggregate.Percentile;
                }
                else
                {
                    throw new ArgumentException($"Aggregate {a} is not supported");
                }
            }
            else
            {
                this.Aggregate = aggregate;
            }
            this.Formula = aggrt.ToLower();
            if (String.Compare(a, aggrt, true) != 0)
            {
                this.HasFormula = true;
            }
            else
            {
                this.HasFormula = false;
            }
        }
    }
    internal class OutputRecord
    {
        public DateTime Time { get; set; }
        public string HostName { get; set; }
        public Dictionary<string, string> GroupedColumns { get; set; }
        public Dictionary<AggregatedColumn, double> AggregatedColumns { get; set; }
        public Dictionary<CalculatedColumn, double> CalculatedColumns { get; set; }
        public OutputRecord()
        {
            this.GroupedColumns = new Dictionary<string, string>();
            this.AggregatedColumns = new Dictionary<AggregatedColumn, double>();
            this.CalculatedColumns = new Dictionary<CalculatedColumn, double>();
        }
    }
   
}
    