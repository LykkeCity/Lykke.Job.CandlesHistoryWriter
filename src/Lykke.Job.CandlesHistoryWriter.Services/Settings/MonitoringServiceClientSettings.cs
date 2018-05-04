using JetBrains.Annotations;
using Lykke.SettingsReader.Attributes;

namespace Lykke.Job.CandlesHistoryWriter.Services.Settings
{
    [UsedImplicitly]
    public class MonitoringServiceClientSettings
    {
        [HttpCheck("api/isalive", false)]
        public string MonitoringServiceUrl { get; set; }
    }
}
