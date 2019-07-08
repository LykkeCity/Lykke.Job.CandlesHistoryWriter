// Copyright (c) 2019 Lykke Corp.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using System.Linq;
using JetBrains.Annotations;
using Lykke.Job.CandlesProducer.Contract;
using Lykke.Service.Assets.Client.Models;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.Candles;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.HistoryMigration.HistoryProviders.MeFeedHistory;

namespace Lykke.Job.CandlesHistoryWriter.Services.HistoryMigration.HistoryProviders.MeFeedHistory
{
    [UsedImplicitly]
    public class EmptyMissedCandlesGenerator : IMissedCandlesGenerator
    {
        public IReadOnlyList<ICandle> FillGapUpTo(AssetPair assetPair, IFeedHistory feedHistory)
        {
            return feedHistory.Candles
                .Select(item => item.ToCandle(feedHistory.AssetPair, feedHistory.PriceType, feedHistory.DateTime))
                .ToList();
        }

        public IReadOnlyList<ICandle> FillGapUpTo(AssetPair assetPair, CandlePriceType priceType, DateTime dateTime, ICandle endCandle)
        {
            return Array.Empty<ICandle>();
        }

        public void RemoveAssetPair(string assetPair)
        {
        }
    }
}
