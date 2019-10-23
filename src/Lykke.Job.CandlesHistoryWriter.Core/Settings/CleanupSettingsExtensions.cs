using Lykke.Job.CandlesProducer.Contract;
using System;
using System.Collections.Generic;
using System.Text;

namespace Lykke.Job.CandlesHistoryWriter.Core.Settings
{
    public static class CleanupSettingsExtensions
    {
        public static int GetCleaupSettings(this CandleTimeInterval timeInterval, CleanupSettings cleanupSettings)
        {
            switch (timeInterval)
            {
                case CandleTimeInterval.Sec: return cleanupSettings.NumberOfTi1;
                case CandleTimeInterval.Minute: return cleanupSettings.NumberOfTi60;
                case CandleTimeInterval.Min5: return cleanupSettings.NumberOfTi300;
                case CandleTimeInterval.Min15: return cleanupSettings.NumberOfTi900;
                case CandleTimeInterval.Min30: return cleanupSettings.NumberOfTi1800;
                case CandleTimeInterval.Hour: return cleanupSettings.NumberOfTi3600;
                case CandleTimeInterval.Hour4: return cleanupSettings.NumberOfTi7200;
                case CandleTimeInterval.Hour6: return cleanupSettings.NumberOfTi21600;
                case CandleTimeInterval.Hour12: return cleanupSettings.NumberOfTi43200;
                case CandleTimeInterval.Day: return cleanupSettings.NumberOfTi86400;
                case CandleTimeInterval.Week: return cleanupSettings.NumberOfTi604800;
                case CandleTimeInterval.Month: return cleanupSettings.NumberOfTi3000000;
                case CandleTimeInterval.Unspecified: return cleanupSettings.NumberOfTiDefault;
                default: throw new NotImplementedException();
            }
        }
    }
}
