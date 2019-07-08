// Copyright (c) 2019 Lykke Corp.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using Lykke.Job.CandlesProducer.Contract;
using Lykke.Service.Assets.Client.Models;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.Candles;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.HistoryMigration.HistoryProviders.MeFeedHistory;

namespace Lykke.Job.CandlesHistoryWriter.Services.HistoryMigration.HistoryProviders.MeFeedHistory
{
    public interface IMissedCandlesGenerator
    {
        IReadOnlyList<ICandle> FillGapUpTo(AssetPair assetPair, IFeedHistory feedHistory);
        IReadOnlyList<ICandle> FillGapUpTo(AssetPair assetPair, CandlePriceType priceType, DateTime dateTime, ICandle endCandle);
        void RemoveAssetPair(string assetPair);
    }
}
