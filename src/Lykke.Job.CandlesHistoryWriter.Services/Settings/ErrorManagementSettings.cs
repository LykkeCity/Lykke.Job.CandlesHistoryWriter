using Lykke.SettingsReader.Attributes;
using System;

namespace Lykke.Job.CandlesHistoryWriter.Services.Settings
{
    public class ErrorManagementSettings
    {
        public bool NotifyOnCantStoreAssetPair { get; set; }
        /// <summary>
        /// Log notification timeout.
        /// </summary>
        public TimeSpan NotifyOnCantStoreAssetPairTimeout { get; set; }
    }
}
