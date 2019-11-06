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
            var retVal = 0;

            switch (timeInterval)
            {
                case CandleTimeInterval.Sec:
                    retVal = cleanupSettings.NumberOfTi1;
                    break;
                case CandleTimeInterval.Minute:
                    retVal = cleanupSettings.NumberOfTi60;
                    break;
                case CandleTimeInterval.Min5:
                    retVal = cleanupSettings.NumberOfTi300;
                    break;
                case CandleTimeInterval.Min15:
                    retVal = cleanupSettings.NumberOfTi900;
                    break;
                case CandleTimeInterval.Min30:
                    retVal = cleanupSettings.NumberOfTi1800;
                    break;
                case CandleTimeInterval.Hour:
                    retVal = cleanupSettings.NumberOfTi3600;
                    break;
                case CandleTimeInterval.Hour4:
                    retVal = cleanupSettings.NumberOfTi7200;
                    break;
                case CandleTimeInterval.Hour6:
                    retVal = cleanupSettings.NumberOfTi21600;
                    break;
                case CandleTimeInterval.Hour12:
                    retVal = cleanupSettings.NumberOfTi43200;
                    break;
                case CandleTimeInterval.Day:
                    retVal = cleanupSettings.NumberOfTi86400;
                    break;
                case CandleTimeInterval.Week:
                    retVal = cleanupSettings.NumberOfTi604800;
                    break;
                case CandleTimeInterval.Month:
                    retVal = cleanupSettings.NumberOfTi3000000;
                    break;
                case CandleTimeInterval.Unspecified:
                    retVal = cleanupSettings.NumberOfTiDefault;
                    break;
                default: throw new NotImplementedException();
            }

            return retVal > 0 ? retVal : cleanupSettings.NumberOfTiDefault;
        }
    }
}
