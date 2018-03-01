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
        
        public class PersistenceInfo
        {
            public Times Times { get; set; }
            public Throughput Throughput { get; set; }
            public long TotalCandlesPersistedCount { get; set; }
            public long TotalCandleRowsPersistedCount { get; set; }
            public int BatchesToPersistQueueLength { get; set; }
            public int CandlesToDispatchQueueLength { get; set; }
        }

        public class Times
        {
            public TimeSpan TotalPersistTime { get; set; }
            public TimeSpan AveragePersistTime { get; set; }
            public TimeSpan LastPersistTime { get; set; }
        }

        public class Throughput
        {
            public int AverageCandlesPersistedPerSecond { get; set; }
            public int AverageCandleRowsPersistedPerSecond { get; set; }
        }
    }
}
