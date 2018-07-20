using System;

namespace Lykke.Job.CandlesHistoryWriter.Services.Settings
{
    public class PersistenceSettings
    {
        public TimeSpan PersistPeriod { get; set; }
        public int CandlesToDispatchLengthPersistThreshold { get; set; }
        public int CandlesToDispatchLengthThrottlingThreshold { get; set; }
        public TimeSpan ThrottlingEnqueueDelay { get; set; }
        public int MaxBatchesToPersistQueueLength { get; set; }
        public int MaxBatchSize { get; set; }
        public int NumberOfSqlConnections { get; set; }
    }
}
