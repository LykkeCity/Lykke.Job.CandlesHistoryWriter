// Copyright (c) 2019 Lykke Corp.
// See the LICENSE file in the project root for more information.

using System;

namespace Lykke.Job.CandlesHistoryWriter.Core.Services
{
    public interface IHealthService
    {
        TimeSpan AveragePersistDuration { get; }
        TimeSpan TotalPersistDuration { get; }
        TimeSpan LastPersistDuration { get; }

        TimeSpan AverageCacheDuration { get; }
        TimeSpan TotalCacheDuration { get; }
        TimeSpan LastCacheDuration { get; }

        int BatchesToPersistQueueLength { get; }
        int CandlesToDispatchQueueLength { get; }
        int AverageCandlesPersistedPerSecond { get; }
        int AverageCandleRowsPersistedPerSecond { get; }
        long TotalCandlesPersistedCount { get; }
        long TotalCandleRowsPersistedCount { get; }

        int AverageCandlesCachedPerSecond { get; }
        int AverageCandlesCachedPerBatch { get; }
        int AverageCandleBatchesPerSecond { get; }
        long TotalCandlesCachedCount { get; }
        long TotalCandleBatchesCachedCount { get; }
        
        void TraceStartPersistCandles();
        void TraceStopPersistCandles();
     
        void TraceEnqueueCandle();
        void TraceCandlesBatchDispatched(int candlesCount);
        void TraceCandlesBatchPersisted(int candlesCount);
        void TraceCandleRowsPersisted(int rowsCount);

        void TraceSetPersistenceQueueState(int amountOfCandlesToDispatch);

        void TraceStartCacheCandles();
        void TraceStopCacheCandles(int candlesCount);
    }
}
