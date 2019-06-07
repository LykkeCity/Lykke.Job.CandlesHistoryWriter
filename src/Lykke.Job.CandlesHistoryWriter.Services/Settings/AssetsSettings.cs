using System;
using Lykke.SettingsReader.Attributes;

namespace Lykke.Job.CandlesHistoryWriter.Services.Settings
{
    public class AssetsSettings
    {
        [HttpCheck("/api/isalive")]
        public string ServiceUrl { get; set; }
        
        public TimeSpan CacheExpirationPeriod { get; set; }
        
        [Optional]
        public string ApiKey { get; set; }
    }
}
