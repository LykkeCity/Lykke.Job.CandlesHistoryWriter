using System.Collections.Immutable;

namespace Lykke.Job.CandlesHistoryWriter.Core.Domain.Candles
{
    public interface ICandlesPersistenceQueueSnapshotRepository : ISnapshotRepository<IImmutableList<ICandle>>
    {
    }
}
