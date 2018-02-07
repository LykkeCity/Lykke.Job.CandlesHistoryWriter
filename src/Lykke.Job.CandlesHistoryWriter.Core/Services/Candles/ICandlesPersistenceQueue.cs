using System.Collections.Immutable;
using Autofac;
using Common;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.Candles;

namespace Lykke.Job.CandlesHistoryWriter.Core.Services.Candles
{
    public interface ICandlesPersistenceQueue : IStartable, IStopable, IHaveState<IImmutableList<ICandle>>
    {
        void DispatchCandlesToPersist(int maxBatchSize);
        void EnqueueCandle(ICandle candle);
    }
}
