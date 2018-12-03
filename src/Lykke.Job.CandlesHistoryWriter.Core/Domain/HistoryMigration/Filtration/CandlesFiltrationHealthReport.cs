using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.Candles;
using Lykke.Job.CandlesProducer.Contract;

namespace Lykke.Job.CandlesHistoryWriter.Core.Domain.HistoryMigration.Filtration
{
    public class CandlesFiltrationHealthReport
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
        public double LimitLow { get; }
        public double LimitHigh { get; }

        public ConcurrentDictionary<CandlePriceType, int> DeletedCandlesCount { get; }
        public ConcurrentDictionary<CandlePriceType, int> ReplacedCandlesCount { get; }
        
        public List<ICandle> ExtremeCandles { get; }

        public ConcurrentBag<string> Errors;

        public CandlesFiltrationHealthReport(string assetPairId, double limitLow, double limitHigh, bool analyzeOnly)
        {
            AnalyzeOnly = analyzeOnly;

            AssetPairId = assetPairId;
            State = CandlesFiltrationState.InProgress;
            StartTime = DateTime.UtcNow;
            FinishTime = null;
            LimitLow = limitLow;
            LimitHigh = limitHigh;

            DeletedCandlesCount = new ConcurrentDictionary<CandlePriceType, int>();
            ReplacedCandlesCount = new ConcurrentDictionary<CandlePriceType, int>();
            
            ExtremeCandles = new List<ICandle>();
            
            Errors = new ConcurrentBag<string>();
        }
    }

    public enum CandlesFiltrationState
    {
        InProgress,
        Finished
    }
}
