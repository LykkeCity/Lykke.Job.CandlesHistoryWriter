// Copyright (c) 2019 Lykke Corp.
// See the LICENSE file in the project root for more information.

using System;
using JetBrains.Annotations;
using Lykke.SettingsReader.Attributes;

namespace Lykke.Job.CandlesHistoryWriter.Services.Settings
{
    public class MigrationSettings
    {
        [Optional]
        public bool MigrationEnabled { get; set; }
        [Optional, CanBeNull]
        public QuotesSettings Quotes { get; set; }
        [Optional, CanBeNull]
        public TradesSettings Trades { get; set; }
    }

    public class QuotesSettings
    {
        public int CandlesToDispatchLengthThrottlingThreshold { get; set; }
        public TimeSpan ThrottlingDelay { get; set; }
    }

    public class TradesSettings
    {
        public string SqlTradesDataSourceConnString { get; set; }
        public int SqlQueryBatchSize { get; set; }
        public TimeSpan SqlCommandTimeout { get; set; }
        public int CandlesPersistenceQueueLimit { get; set; }
    }
}
