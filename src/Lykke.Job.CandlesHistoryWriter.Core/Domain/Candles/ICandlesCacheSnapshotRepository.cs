using System.Collections.Immutable;

namespace Lykke.Job.CandlesHistoryWriter.Core.Domain.Candles
{
    public interface ICandlesCacheSnapshotRepository : ISnapshotRepository<IImmutableDictionary<string, IImmutableList<ICandle>>>
    {
    }
}
