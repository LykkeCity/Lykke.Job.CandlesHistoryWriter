// Copyright (c) 2019 Lykke Corp.
// See the LICENSE file in the project root for more information.

using System.Collections.Immutable;

namespace Lykke.Job.CandlesHistoryWriter.Core.Domain.Candles
{
    public interface ICandlesPersistenceQueueSnapshotRepository : ISnapshotRepository<IImmutableList<ICandle>>
    {
    }
}
