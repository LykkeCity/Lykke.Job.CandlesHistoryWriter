using Lykke.SettingsReader.Attributes;

namespace Lykke.Job.CandlesHistoryWriter.Services.Settings
{
    public class SlackNotificationsSettings
    {
        [AzureQueueCheck]
        public AzureQueueSettings AzureQueue { get; set; }
    }
}
