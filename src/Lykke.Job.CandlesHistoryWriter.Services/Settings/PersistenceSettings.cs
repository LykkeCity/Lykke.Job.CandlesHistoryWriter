// Copyright (c) 2019 Lykke Corp.
// See the LICENSE file in the project root for more information.

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
        public int NumberOfSaveThreads { get; set; }
    }
}
