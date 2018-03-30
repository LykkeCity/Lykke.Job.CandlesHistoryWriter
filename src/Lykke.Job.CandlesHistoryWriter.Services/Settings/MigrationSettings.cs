using System;
using Lykke.SettingsReader.Attributes;

namespace Lykke.Job.CandlesHistoryWriter.Services.Settings
{
    public class MigrationSettings
    {
        [Optional]
        public bool MigrationEnabled { get; set; }
        public QuotesSettings Quotes { get; set; }
        public TradesSettings Trades { get; set; }
    }

    public class QuotesSettings
    {
        public int CandlesToDispatchLengthThrottlingThreshold { get; set; }
        public TimeSpan ThrottlingDelay { get; set; }
    }

    public class TradesSettings
    {
        public string SqlTradesDataSourceConnString { get; set; }
        public int SqlQueryBatchSize { get; set; }
    }
}
