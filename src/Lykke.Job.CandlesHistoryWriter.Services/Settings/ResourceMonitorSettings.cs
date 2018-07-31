using Lykke.SettingsReader.Attributes;

namespace Lykke.Job.CandlesHistoryWriter.Services.Settings
{
    public class ResourceMonitorSettings
    {
        [Optional]
        public ResourceMonitorMode MonitorMode { get; set; }
        [Optional]
        public double CpuThreshold { get; set; }
        [Optional]
        public int RamThreshold { get; set; }
    }

    public enum ResourceMonitorMode
    {
        Off,
        AppInsightsOnly,
        AppInsightsWithLog
    }
}
