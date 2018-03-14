using System;
using System.Diagnostics;
using System.Threading;
using JetBrains.Annotations;
using Lykke.Job.CandlesHistoryWriter.Core.Services;

namespace Lykke.Job.CandlesHistoryWriter.Services
{
    [UsedImplicitly(ImplicitUseKindFlags.InstantiatedNoFixedConstructorSignature)]
    public class HealthService : IHealthService
    {
        public TimeSpan LastPersistDuration { get; private set; }
        public TimeSpan TotalPersistDuration { get; private set; }
        public TimeSpan LastCacheDuration { get; private set; }
        public TimeSpan TotalCacheDuration { get; private set; }
        
        public TimeSpan AveragePersistDuration => _totalPersistCount != 0
            ? new TimeSpan(TotalPersistDuration.Ticks / _totalPersistCount)
            : TimeSpan.Zero;

        public TimeSpan AverageCacheDuration => TotalCandleBatchesCachedCount != 0
            ? new TimeSpan(TotalCacheDuration.Ticks / TotalCandleBatchesCachedCount)
            : TimeSpan.Zero;

        public int AverageCandlesPersistedPerSecond => TotalPersistDuration != TimeSpan.Zero
            ? (int) (_totalCandlesPersistedCount / TotalPersistDuration.TotalSeconds)
            : 0;

        public int AverageCandleRowsPersistedPerSecond => TotalPersistDuration != TimeSpan.Zero
            ? (int) (_totalCandleRowsPersistedCount / TotalPersistDuration.TotalSeconds)
            : 0;

        public int AverageCandlesCachedPerSecond => TotalCacheDuration != TimeSpan.Zero
            ? (int) (TotalCandlesCachedCount / TotalCacheDuration.TotalSeconds)
            : 0;

        public int AverageCandlesCachedPerBatch => TotalCandleBatchesCachedCount != 0
            ? (int) (TotalCandlesCachedCount / TotalCandleBatchesCachedCount)
            : 0;

        public int AverageCandleBatchesPerSecond => TotalCacheDuration != TimeSpan.Zero
            ? (int) (TotalCandleBatchesCachedCount / TotalCacheDuration.TotalSeconds)
            : 0;

        public long TotalCandlesPersistedCount => _totalCandlesPersistedCount;

        public long TotalCandleRowsPersistedCount => _totalCandleRowsPersistedCount;
        public long TotalCandlesCachedCount { get; set; }
        public long TotalCandleBatchesCachedCount { get; set; }

        public int BatchesToPersistQueueLength => _batchesToPersistQueueLength;

        public int CandlesToDispatchQueueLength => _candlesToDispatchQueueLength;

        private long _totalCandlesPersistedCount;
        private int _batchesToPersistQueueLength;
        private int _candlesToDispatchQueueLength;
        private Stopwatch _persistCandlesStopwatch;
        private long _totalPersistCount;
        private long _totalCandleRowsPersistedCount;
        private Stopwatch _cacheCandlesStopwatch;

        public void TraceStartPersistCandles()
        {
            if (_persistCandlesStopwatch != null)
            {
                return;
            }

            _persistCandlesStopwatch = Stopwatch.StartNew();
        }

        public void TraceStopPersistCandles()
        {
            _persistCandlesStopwatch.Stop();

            LastPersistDuration = _persistCandlesStopwatch.Elapsed;
            TotalPersistDuration += _persistCandlesStopwatch.Elapsed;
            ++_totalPersistCount;

            _persistCandlesStopwatch = null;
        }

        public void TraceEnqueueCandle()
        {
            Interlocked.Increment(ref _candlesToDispatchQueueLength);
        }

        public void TraceSetPersistenceQueueState(int amountOfCandlesToDispatch)
        {
            Interlocked.Add(ref _candlesToDispatchQueueLength, amountOfCandlesToDispatch);
        }

        public void TraceStartCacheCandles()
        {
            if (_cacheCandlesStopwatch != null)
            {
                return;
            }

            _cacheCandlesStopwatch = Stopwatch.StartNew();
        }

        public void TraceStopCacheCandles(int candlesCount)
        {
            _cacheCandlesStopwatch.Stop();

            LastCacheDuration = _cacheCandlesStopwatch.Elapsed;
            TotalCacheDuration += _cacheCandlesStopwatch.Elapsed;
            ++TotalCandleBatchesCachedCount;
            TotalCandlesCachedCount += candlesCount;

            _cacheCandlesStopwatch = null;
        }

        public void TraceCandlesBatchDispatched(int candlesCount)
        {
            Interlocked.Add(ref _candlesToDispatchQueueLength, -candlesCount);
            Interlocked.Increment(ref _batchesToPersistQueueLength);
        }

        public void TraceCandlesBatchPersisted(int candlesCount)
        {
            Interlocked.Add(ref _totalCandlesPersistedCount, candlesCount);
            Interlocked.Decrement(ref _batchesToPersistQueueLength);
        }

        public void TraceCandleRowsPersisted(int rowsCount)
        {
            Interlocked.Add(ref _totalCandleRowsPersistedCount, rowsCount);
        }
    }
}
