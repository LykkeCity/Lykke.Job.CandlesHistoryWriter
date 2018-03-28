using System;
using System.Collections.Generic;
using System.Text;

namespace Lykke.Job.CandlesHistoryWriter.Core.Domain.HistoryMigration.Filtration
{
    public class CandlesFiltrationHealthReport
    {
        private CandlesFiltrationState _state;
        public CandlesFiltrationState State
        {
            get => _state;
            set
            {
                if (value == CandlesFiltrationState.Finished)
                    FinishTime = DateTime.UtcNow;
                _state = value;
            }
        }
        public DateTime StartTime { get; }
        public DateTime? FinishTime { get; set; }

        public TimeSpan? Duration => FinishTime == null
            ? DateTime.UtcNow - StartTime
            : FinishTime - StartTime;

        public string AssetId { get; set; }
        public double LimitLow { get; set; }
        public double LimitHigh { get; set; }
        public int DeletedCandlesCount { get; set; }
        public int ReplacedCandlesCount { get; set; }

        public CandlesFiltrationHealthReport(string assetId, double limitLow, double limitHigh)
        {
            AssetId = assetId;
            State = CandlesFiltrationState.InProgress;
            StartTime = DateTime.UtcNow;
            FinishTime = null;
            LimitLow = limitLow;
            LimitHigh = limitHigh;
        }
    }

    public enum CandlesFiltrationState
    {
        InProgress,
        Finished
    }
}
