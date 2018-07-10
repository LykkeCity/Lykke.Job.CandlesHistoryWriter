using System;

namespace Lykke.Job.CandlesHistoryWriter.Services.Settings
{
    public class HistoryCacheSettings
    {
        public int HistoryTicksCacheSize { get; set; }
        public TimeSpan CacheCheckupPeriod { get; set; }
    }
}
