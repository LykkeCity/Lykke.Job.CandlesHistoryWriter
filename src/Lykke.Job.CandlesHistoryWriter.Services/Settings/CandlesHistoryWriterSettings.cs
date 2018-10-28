using System;
using JetBrains.Annotations;
using Lykke.SettingsReader.Attributes;

namespace Lykke.Job.CandlesHistoryWriter.Services.Settings
{
    [UsedImplicitly]
    public class CandlesHistoryWriterSettings
    {        
        public AssetsCacheSettings AssetsCache { get; set; }
        
        public RabbitSettings Rabbit { get; set; }
        
        public QueueMonitorSettings QueueMonitor { get; set; }
        
        public PersistenceSettings Persistence { get; set; }
        
        public DbSettings Db { get; set; }
      
        [Optional, CanBeNull]
        public MigrationSettings Migration { get; set; }

        public ErrorManagementSettings ErrorManagement { get; set; }

        [Optional, CanBeNull]
        public ResourceMonitorSettings ResourceMonitor { get; set; }

        public int HistoryTicksCacheSize { get; set; }
        
        public TimeSpan CacheCleanupPeriod { get; set; }
        
        [Optional]
        public bool UseSerilog { get; set; }
    }
}
