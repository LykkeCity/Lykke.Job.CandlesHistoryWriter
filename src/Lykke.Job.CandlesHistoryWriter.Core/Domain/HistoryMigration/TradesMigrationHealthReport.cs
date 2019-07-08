// Copyright (c) 2019 Lykke Corp.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;

namespace Lykke.Job.CandlesHistoryWriter.Core.Domain.HistoryMigration
{
    public class TradesMigrationHealthReport
    {
        public int SqlQueryBatchSize { get; }
        public DateTime? RemoveByDate { get; }
        public int PersistenceQueueSize { get; set; }

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

        public TradesMigrationHealthReport(int sqlQueryBatchSize, DateTime? removeByDate)
        {
            SqlQueryBatchSize = sqlQueryBatchSize;
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
        public DateTime CurrentTradeBatchBegining { get; set; }
        public DateTime CurrentTradeBatchEnding { get; set; }
    }

    public enum TradesMigrationState
    {
        InProgress,
        Finished,
        Error
    }
}
