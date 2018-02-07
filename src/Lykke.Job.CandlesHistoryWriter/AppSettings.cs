using System;
using System.Collections.Generic;
using Lykke.Service.Assets.Client.Custom;
using Lykke.Job.CandlesHistoryWriter.Services.Settings;
using Lykke.SettingsReader.Attributes;

namespace Lykke.Job.CandlesHistoryWriter
{
    public class AppSettings
    {
        [Optional]
        public CandlesHistorySettings CandlesHistory { get; set; }
        [Optional]
        public CandlesHistorySettings MtCandlesHistory { get; set; }
        public SlackNotificationsSettings SlackNotifications { get; set; }

        [Optional]
        public Dictionary<string, string> CandleHistoryAssetConnections
        {
            get => _candleHistoryAssetConnections;
            set => _candleHistoryAssetConnections = new Dictionary<string, string>(value, StringComparer.InvariantCultureIgnoreCase);
        }

        [Optional]
        public Dictionary<string, string> MtCandleHistoryAssetConnections
        {
            get => _mtCandleHistoryAssetConnections;
            set => _mtCandleHistoryAssetConnections = new Dictionary<string, string>(value, StringComparer.InvariantCultureIgnoreCase);
        }

        public AssetsSettings Assets { get; set; }

        public RedisSettings RedisSettings { get; set; }

        private Dictionary<string, string> _candleHistoryAssetConnections;
        private Dictionary<string, string> _mtCandleHistoryAssetConnections;
    }
}
