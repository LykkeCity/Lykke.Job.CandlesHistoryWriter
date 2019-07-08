// Copyright (c) 2019 Lykke Corp.
// See the LICENSE file in the project root for more information.

using System.Collections.Generic;
using Lykke.Job.CandlesHistoryWriter.Services.Settings;
using Lykke.SettingsReader.Attributes;
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

        [Optional, CanBeNull]
        [UsedImplicitly(ImplicitUseKindFlags.Assign)]
        public SlackNotificationsSettings SlackNotifications { get; set; }

        [UsedImplicitly(ImplicitUseKindFlags.Assign)]
        public AssetsSettings Assets { get; set; }

        [UsedImplicitly(ImplicitUseKindFlags.Assign)]
        public RedisSettings RedisSettings { get; set; }

        [Optional, CanBeNull]
        [UsedImplicitly(ImplicitUseKindFlags.Assign)]
        public MonitoringServiceClientSettings MonitoringServiceClient { get; set; }
    }
}
