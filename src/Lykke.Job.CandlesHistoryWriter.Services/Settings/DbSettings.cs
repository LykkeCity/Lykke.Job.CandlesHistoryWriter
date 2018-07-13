using Lykke.SettingsReader.Attributes;
using Lykke.Job.CandlesHistoryWriter.Core.Domain;

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

        public StorageMode StorageMode { get; set; }

        [SqlCheck]
        public string SqlConnectionString { get; set; }
    }
}
