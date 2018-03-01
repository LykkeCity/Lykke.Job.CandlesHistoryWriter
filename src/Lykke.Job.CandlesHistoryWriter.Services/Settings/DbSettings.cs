using Lykke.SettingsReader.Attributes;

namespace Lykke.Job.CandlesHistoryWriter.Services.Settings
{
    public class DbSettings
    {
        [AzureTableCheck]
        public string LogsConnectionString { get; set; }

        [AzureBlobCheck]
        public string SnapshotsConnectionString { get; set; }

        [Optional]
        [AzureTableCheck]
        public string FeedHistoryConnectionString { get; set; }
    }
}
