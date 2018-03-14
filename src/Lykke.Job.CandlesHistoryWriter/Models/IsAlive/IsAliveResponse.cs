using System;

namespace Lykke.Job.CandlesHistoryWriter.Models.IsAlive
{
    /// <summary>
    /// Checks service is alive response
    /// </summary>
    public class IsAliveResponse
    {
        public string Name { get; set; }
        /// <summary>
        /// API version
        /// </summary>
        public string Version { get; set; }
        /// <summary>
        /// Environment variables
        /// </summary>
        public string Env { get; set; }
        public bool IsShuttingDown { get; set; }
        public bool IsShuttedDown { get; set; }
        public PersistenceInfo Persistence { get; set; }
        public CacheInfo Cache { get; set; }

        public class PersistenceInfo
        {
            public Duration Duration { get; set; }
            public PersistThroughput PersistThroughput { get; set; }
            public long TotalCandlesPersistedCount { get; set; }
            public long TotalCandleRowsPersistedCount { get; set; }
            public int BatchesToPersistQueueLength { get; set; }
            public int CandlesToDispatchQueueLength { get; set; }
        }

        public class Duration
        {
            public TimeSpan Total { get; set; }
            public TimeSpan Average { get; set; }
            public TimeSpan Last { get; set; }
        }

        public class PersistThroughput
        {
            public int AverageCandlesPersistedPerSecond { get; set; }
            public int AverageCandleRowsPersistedPerSecond { get; set; }
        }

        public class CacheInfo
        {
            public Duration Duration { get; set; }
            public CacheThroughput Throughput { get; set; }
            public long TotalCandlesCachedCount { get; set; }
            public long TotalCandleBatchesCachedCount { get; set; }
        }


        public class CacheThroughput
        {
            public int AverageCandlesCachedPerSecond { get; set; }
            public int AverageCandlesCachedPerBatch { get; set; }
            public int AverageCandleBatchesPerSecond { get; set; }
        }
    }
}
