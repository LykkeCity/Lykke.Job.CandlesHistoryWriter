using System;
using System.Collections.Generic;
using Lykke.Job.CandlesProducer.Contract;

namespace Lykke.Job.CandlesHistoryWriter.Services.Settings
{
    public class HistoryCacheSettings
    {
        public Dictionary<CandleTimeInterval, int> HistoryTicksCacheSizes { get; set; }
        public TimeSpan CacheCheckupPeriod { get; set; }
        public DateTime MinDate { get; set; }
    }
}
