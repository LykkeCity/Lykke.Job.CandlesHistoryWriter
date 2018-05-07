using System;
using System.Collections.Concurrent;
using System.Collections.Generic;

namespace Lykke.Job.CandlesHistoryWriter.Core.Domain.HistoryMigration
{
    public class TradesMigrationHealthReport
    {
        public int SqlQueryBatchSize { get; }
        public bool PreliminaryRemoval { get; }
        public DateTime? RemoveByDate { get; }

        private TradesMigrationState _state;
        public TradesMigrationState State
        {
            get => _state;
            set
            {
                if (value != TradesMigrationState.InProgress)
                    FinishTime = DateTime.UtcNow;
                _state = value;
            }
        }
        public DateTime StartTime { get; }
        public DateTime? FinishTime { get; set; }

        public TimeSpan? Duration => FinishTime == null
            ? DateTime.UtcNow - StartTime
            : FinishTime - StartTime;

        public IDictionary<string, TradesMigrationHealthReportItem> AssetReportItems { get; }

        public TradesMigrationHealthReport(int sqlQueryBatchSize, bool preliminaryRemoval, DateTime? removeByDate)
        {
            SqlQueryBatchSize = sqlQueryBatchSize;
            PreliminaryRemoval = preliminaryRemoval;
            RemoveByDate = removeByDate;
            State = TradesMigrationState.InProgress;
            StartTime = DateTime.UtcNow;
            FinishTime = null;
            AssetReportItems = new ConcurrentDictionary<string, TradesMigrationHealthReportItem>();
        }
    }

    public class TradesMigrationHealthReportItem
    {
        public int SummaryFetchedTrades { get; set; }
        public int SummarySavedCandles { get; set; }
    }

    public enum TradesMigrationState
    {
        InProgress,
        Finished,
        Error
    }
}
