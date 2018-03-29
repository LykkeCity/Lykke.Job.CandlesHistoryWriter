using System;
using System.Collections.Concurrent;

namespace Lykke.Job.CandlesHistoryWriter.Core.Domain.HistoryMigration.Filtration
{
    public class CandlesFiltrationHealthReport
    {
        private readonly object _stateLocker = new object();
        private readonly object _deletedLocker = new object();
        private readonly object _replacedLocker = new object();

        private CandlesFiltrationState _state;
        public CandlesFiltrationState State
        {
            get => _state;
            set
            {
                lock (_stateLocker)
                {
                    if (value != CandlesFiltrationState.InProgress)
                        FinishTime = DateTime.UtcNow;
                    _state = value;
                }
            }
        }
        public DateTime StartTime { get; }
        public DateTime? FinishTime { get; set; }

        public TimeSpan? Duration => FinishTime == null
            ? DateTime.UtcNow - StartTime
            : FinishTime - StartTime;

        public string AssetId { get; }
        public double LimitLow { get; }
        public double LimitHigh { get; }

        private int _deletedCandlesCount;
        public int DeletedCandlesCount
        {
            get => _deletedCandlesCount;
            set
            {
                lock (_deletedLocker)
                    _deletedCandlesCount = value;
            }
        }

        private int _replacedCandlesCount;
        public int ReplacedCandlesCount
        {
            get => _replacedCandlesCount;
            set
            {
                lock (_replacedLocker)
                    _replacedCandlesCount = value;
            }
        }

        public ConcurrentBag<string> Errors;

        public CandlesFiltrationHealthReport(string assetId, double limitLow, double limitHigh)
        {
            AssetId = assetId;
            State = CandlesFiltrationState.InProgress;
            StartTime = DateTime.UtcNow;
            FinishTime = null;
            LimitLow = limitLow;
            LimitHigh = limitHigh;

            Errors = new ConcurrentBag<string>();
        }
    }

    public enum CandlesFiltrationState
    {
        InProgress,
        Finished
    }
}
