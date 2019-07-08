// Copyright (c) 2019 Lykke Corp.
// See the LICENSE file in the project root for more information.

using System;
using Lykke.Job.CandlesProducer.Contract;

namespace Lykke.Job.CandlesHistoryWriter.Core.Domain.HistoryMigration.HistoryProviders.MeFeedHistory
{
    public interface IFeedHistory
    {
        string AssetPair { get; }
        CandlePriceType PriceType { get; }
        DateTime DateTime { get; }
        FeedHistoryItem[] Candles { get; }
    }
}
