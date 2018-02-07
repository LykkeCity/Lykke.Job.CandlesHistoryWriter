using Lykke.SettingsReader.Attributes;

namespace Lykke.Job.CandlesHistoryWriter.Services.Settings
{
    public class RabbitEndpointSettings
    {
        [AmqpCheck]
        public string ConnectionString { get; set; }
        public string Namespace { get; set; }
    }
}
