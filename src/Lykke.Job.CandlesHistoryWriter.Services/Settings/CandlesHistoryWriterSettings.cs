using System;
using System.Runtime.InteropServices;
using JetBrains.Annotations;

namespace Lykke.Job.CandlesHistoryWriter.Services.Settings
{
    public class CandlesHistoryWriterSettings
    {        
        public AssetsCacheSettings AssetsCache { get; set; }
        public RabbitSettings Rabbit { get; set; }
        public QueueMonitorSettings QueueMonitor { get; set; }
        public PersistenceSettings Persistence { get; set; }
        public DbSettings Db { get; set; }
        [CanBeNull]
        public MigrationSettings Migration { get; set; }
        public ErrorManagementSettings ErrorManagement { get; set; }

        [CanBeNull]
        public ResourceMonitorSettings ResourceMonitor { get; set; }

        public int HistoryTicksCacheSize { get; set; }
        public TimeSpan CacheCleanupPeriod { get; set; }
    }
}
