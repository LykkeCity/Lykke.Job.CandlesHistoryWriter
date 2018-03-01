using Lykke.SettingsReader.Attributes;

namespace Lykke.Job.CandlesHistoryWriter.Services.Settings
{
    public class AzureQueueSettings
    {
        [AzureQueueCheck]
        public string ConnectionString { get; set; }

        public string QueueName { get; set; }
    }
}
