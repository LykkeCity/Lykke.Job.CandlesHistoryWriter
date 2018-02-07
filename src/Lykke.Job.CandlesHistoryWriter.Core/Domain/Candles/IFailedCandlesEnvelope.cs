using System;
using System.Collections.Generic;

namespace Lykke.Job.CandlesHistoryWriter.Core.Domain.Candles
{
    public interface IFailedCandlesEnvelope
    {
        DateTime ProcessingMoment { get; }
        string Exception { get; }
        IEnumerable<ICandle> Candles { get; }
    }
}
