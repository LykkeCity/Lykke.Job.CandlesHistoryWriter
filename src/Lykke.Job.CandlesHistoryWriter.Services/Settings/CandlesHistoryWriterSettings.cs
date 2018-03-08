using System;

namespace Lykke.Job.CandlesHistoryWriter.Services.Settings
{
    public class CandlesHistoryWriterSettings
    {        
        public AssetsCacheSettings AssetsCache { get; set; }
        public RabbitSettings Rabbit { get; set; }
        public QueueMonitorSettings QueueMonitor { get; set; }
        public PersistenceSettings Persistence { get; set; }
        public DbSettings Db { get; set; }
        public MigrationSettings Migration { get; set; }
        public ErrorManagementSettings ErrorManagement { get; set; }
        public int HistoryTicksCacheSize { get; set; }
        public TimeSpan CacheCleanupPeriod { get; set; }
    }
}
