using System;
using System.Collections.Concurrent;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.HistoryMigration.Filtration;
using Lykke.Job.CandlesProducer.Contract;

namespace Lykke.Job.CandlesHistoryWriter.Core.Domain.HistoryMigration
{
    public class MidPricesFixHealthReport
    {
        public bool AnalyzeOnly { get; }

        private CandlesFiltrationState _state;
        public CandlesFiltrationState State
        {
            get => _state;
            set
            {
                if (value != CandlesFiltrationState.InProgress)
                    FinishTime = DateTime.UtcNow;
                _state = value;
            }
        }
        public DateTime StartTime { get; }
        public DateTime? FinishTime { get; set; }

        public TimeSpan? Duration => FinishTime == null
            ? DateTime.UtcNow - StartTime
            : FinishTime - StartTime;

        public string AssetPairId { get; }

        public int CorruptedCandlesCount { get; set; }
        public int FixedCandlesCount { get; set; }
        public CandleTimeInterval? CurrentInterval { get; set; }
        public DateTime DateFrom { get; set; }
        public DateTime DateTo { get; set; }
        public string Message { get; set; }

        public ConcurrentBag<string> Errors;

        public MidPricesFixHealthReport(string assetPairId, bool analyzeOnly)
        {
            AnalyzeOnly = analyzeOnly;

            AssetPairId = assetPairId;
            State = CandlesFiltrationState.InProgress;
            StartTime = DateTime.UtcNow;
            FinishTime = null;

            Errors = new ConcurrentBag<string>();
        }
    }
}
