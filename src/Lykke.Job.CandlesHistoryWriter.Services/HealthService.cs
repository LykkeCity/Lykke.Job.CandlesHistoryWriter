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
        public TimeSpan LastPersistTime { get; private set; }

        public TimeSpan AveragePersistTime => _totalPersistCount != 0
            ? new TimeSpan(_totalPersistTime.Ticks / _totalPersistCount)
            : TimeSpan.Zero;

        public TimeSpan TotalPersistTime => _totalPersistTime;

        public int AverageCandlesPersistedPerSecond => TotalPersistTime != TimeSpan.Zero
            ? (int) (_totalCandlesPersistedCount / TotalPersistTime.TotalSeconds)
            : 0;

        public int AverageCandleRowsPersistedPerSecond => TotalPersistTime != TimeSpan.Zero
            ? (int) (_totalCandleRowsPersistedCount / TotalPersistTime.TotalSeconds)
            : 0;

        public long TotalCandlesPersistedCount => _totalCandlesPersistedCount;

        public long TotalCandleRowsPersistedCount => _totalCandleRowsPersistedCount;

        public int BatchesToPersistQueueLength => _batchesToPersistQueueLength;

        public int CandlesToDispatchQueueLength => _candlesToDispatchQueueLength;

        private long _totalCandlesPersistedCount;
        private int _batchesToPersistQueueLength;
        private int _candlesToDispatchQueueLength;
        private Stopwatch _persistCandlesStopwatch;
        private TimeSpan _totalPersistTime;
        private long _totalPersistCount;
        private long _totalCandleRowsPersistedCount;

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

            LastPersistTime = _persistCandlesStopwatch.Elapsed;
            _totalPersistTime += _persistCandlesStopwatch.Elapsed;
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
