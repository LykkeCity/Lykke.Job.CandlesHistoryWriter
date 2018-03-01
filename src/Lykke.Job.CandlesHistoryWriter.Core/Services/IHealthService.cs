using System;

namespace Lykke.Job.CandlesHistoryWriter.Core.Services
{
    public interface IHealthService
    {
        TimeSpan AveragePersistTime { get; }
        TimeSpan TotalPersistTime { get; }
        TimeSpan LastPersistTime { get; }

        int BatchesToPersistQueueLength { get; }
        int CandlesToDispatchQueueLength { get; }
        int AverageCandlesPersistedPerSecond { get; }
        int AverageCandleRowsPersistedPerSecond { get; }
        long TotalCandlesPersistedCount { get; }
        long TotalCandleRowsPersistedCount { get; }
        
        void TraceStartPersistCandles();
        void TraceStopPersistCandles();
     
        void TraceEnqueueCandle();
        void TraceCandlesBatchDispatched(int candlesCount);
        void TraceCandlesBatchPersisted(int candlesCount);
        void TraceCandleRowsPersisted(int rowsCount);

        void TraceSetPersistenceQueueState(int amountOfCandlesToDispatch);
        
    }
}
