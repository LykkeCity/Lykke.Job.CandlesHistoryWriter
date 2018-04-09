using System;
using System.Collections.Generic;
using Lykke.Job.CandlesHistoryWriter.Core.Domain;
using Lykke.Job.CandlesProducer.Contract;
using Lykke.Service.Assets.Client.Custom;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.Candles;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.HistoryMigration.HistoryProviders.MeFeedHistory;

namespace Lykke.Job.CandlesHistoryWriter.Services.HistoryMigration.HistoryProviders.MeFeedHistory
{
    public interface IMissedCandlesGenerator
    {
        IReadOnlyList<ICandle> FillGapUpTo(AssetPair assetPair, IFeedHistory feedHistory);
        IReadOnlyList<ICandle> FillGapUpTo(AssetPair assetPair, CandlePriceType priceType, DateTime dateTime,
            ICandle endCandle);
        void RemoveAssetPair(string assetPair);
    }
}
