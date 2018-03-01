using System;

namespace Lykke.Job.CandlesHistoryWriter.Services.Settings
{
    public class MigrationSettings
    {
        public int CandlesToDispatchLengthThrottlingThreshold { get; set; }
        public TimeSpan ThrottlingDelay { get; set; }
    }
}
