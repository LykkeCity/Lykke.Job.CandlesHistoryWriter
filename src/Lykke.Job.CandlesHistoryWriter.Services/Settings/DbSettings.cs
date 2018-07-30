using Lykke.SettingsReader.Attributes;
using Lykke.Job.CandlesHistoryWriter.Core.Domain;

namespace Lykke.Job.CandlesHistoryWriter.Services.Settings
{
    public class DbSettings
    {
        public string LogsConnectionString { get; set; }

        public string SnapshotsConnectionString { get; set; }

        [Optional]
        public string FeedHistoryConnectionString { get; set; }

        public StorageMode StorageMode { get; set; }

    }
}
