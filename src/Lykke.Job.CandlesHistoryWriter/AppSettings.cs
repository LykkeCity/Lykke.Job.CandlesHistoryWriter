using System.Collections.Generic;
using Lykke.Service.Assets.Client.Custom;
using Lykke.Job.CandlesHistoryWriter.Services.Settings;
using Lykke.SettingsReader.Attributes;
using System.Linq;
using JetBrains.Annotations;

namespace Lykke.Job.CandlesHistoryWriter
{
    [UsedImplicitly]
    public class AppSettings
    {
        [Optional]
        [UsedImplicitly(ImplicitUseKindFlags.Assign)]
        public CandlesHistoryWriterSettings CandlesHistoryWriter { get; set; }

        [Optional]
        [UsedImplicitly(ImplicitUseKindFlags.Assign)]
        public CandlesHistoryWriterSettings MtCandlesHistoryWriter { get; set; }

        [UsedImplicitly(ImplicitUseKindFlags.Assign)]
        public SlackNotificationsSettings SlackNotifications { get; set; }

        [Optional]
        [UsedImplicitly(ImplicitUseKindFlags.Assign)]
        public Dictionary<string, string> CandleHistoryAssetConnections
        {
            get => _candleHistoryAssetConnections;
            set => _candleHistoryAssetConnections = value.ToDictionary(x => x.Key.ToUpper(), x => x.Value);
        }

        [Optional]
        [UsedImplicitly(ImplicitUseKindFlags.Assign)]
        public Dictionary<string, string> MtCandleHistoryAssetConnections
        {
            get => _mtCandleHistoryAssetConnections;
            set => _mtCandleHistoryAssetConnections = value.ToDictionary(x => x.Key.ToUpper(), x => x.Value);
        }

        [UsedImplicitly(ImplicitUseKindFlags.Assign)]
        public AssetsSettings Assets { get; set; }

        [UsedImplicitly(ImplicitUseKindFlags.Assign)]
        public RedisSettings RedisSettings { get; set; }

        private Dictionary<string, string> _candleHistoryAssetConnections;
        private Dictionary<string, string> _mtCandleHistoryAssetConnections;
    }
}
