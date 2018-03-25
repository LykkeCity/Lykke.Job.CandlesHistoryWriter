using System;
using System.Collections.Generic;

namespace Lykke.Job.CandlesHistoryWriter.Core.Domain.HistoryMigration.HistoryProviders.TradesSQLHistory
{
    public class TradesMigrationHealthReport
    {
        public int SqlQueryBatchSize { get; }

        private TradesMigrationState _state;
        public TradesMigrationState State
        {
            get => _state;
            set
            {
                if (value == TradesMigrationState.Finished)
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

        public TradesMigrationHealthReport(int sqlQueryBatchSize)
        {
            SqlQueryBatchSize = sqlQueryBatchSize;
            State = TradesMigrationState.InProgress;
            StartTime = DateTime.UtcNow;
            FinishTime = null;
            AssetReportItems = new Dictionary<string, TradesMigrationHealthReportItem>();
        }
    }

    public class TradesMigrationHealthReportItem
    {
        public int StartingOffset { get; }
        public int SummaryFetchedTrades { get; set; }
        public int SummarySavedCandles { get; set; }

        public TradesMigrationHealthReportItem(int startingOffset)
        {
            StartingOffset = startingOffset;
            SummaryFetchedTrades = SummarySavedCandles = 0;
        }
    }

    public enum TradesMigrationState
    {
        InProgress,
        Finished
    }
}
