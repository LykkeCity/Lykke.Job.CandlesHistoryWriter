using Lykke.SettingsReader.Attributes;

namespace Lykke.Job.CandlesHistoryWriter.Services.Settings
{
    public class ErrorManagementSettings
    {
        [AmqpCheck]
        public bool NotifyOnCantStoreAssetPair { get; set; }
        /// <summary>
        /// Log notification timeout in seconds.
        /// </summary>
        public int NotifyOnCantStoreAssetPairTimeout { get; set; }
    }
}
