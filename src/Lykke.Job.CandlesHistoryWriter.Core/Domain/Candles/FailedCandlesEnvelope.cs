using System;
using System.Collections.Generic;
using System.Linq;

namespace Lykke.Job.CandlesHistoryWriter.Core.Domain.Candles
{
    public class FailedCandlesEnvelope : IFailedCandlesEnvelope
    {
        public DateTime ProcessingMoment { get; set; }
        public string Exception { get; set; }
        public IEnumerable<ICandle> Candles { get; set; }

        public static FailedCandlesEnvelope Create(IFailedCandlesEnvelope src)
        {
            return new FailedCandlesEnvelope
            {
                ProcessingMoment = src.ProcessingMoment,
                Exception = src.Exception,
                Candles = src.Candles.Select(Candle.Copy)
            };
        }
    }
}
