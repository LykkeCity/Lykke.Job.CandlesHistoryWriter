using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Lykke.Job.CandlesProducer.Contract;
using Lykke.Service.Assets.Client.Models;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.Candles;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.HistoryMigration.HistoryProviders.MeFeedHistory;
using Lykke.Job.CandlesHistoryWriter.Core.Services.HistoryMigration.HistoryProviders;

namespace Lykke.Job.CandlesHistoryWriter.Services.HistoryMigration.HistoryProviders.MeFeedHistory
{
    [UsedImplicitly]
    public class MeFeedHistoryProvider : IHistoryProvider
    {
        private readonly IFeedHistoryRepository _feedHistoryRepository;
        private readonly IMissedCandlesGenerator _missedCandlesGenerator;

        public MeFeedHistoryProvider(
            IFeedHistoryRepository feedHistoryRepository,
            IMissedCandlesGenerator missedCandlesGenerator)
        {
            _feedHistoryRepository = feedHistoryRepository;
            _missedCandlesGenerator = missedCandlesGenerator;
        }

        public async Task<DateTime?> GetStartDateAsync(string assetPair, CandlePriceType priceType)
        {
            var oldestFeedHistory = await _feedHistoryRepository.GetTopRecordAsync(assetPair, priceType);

            return oldestFeedHistory
                ?.Candles
                .First()
                .ToCandle(assetPair, priceType, oldestFeedHistory.DateTime).Timestamp;
        }

        public async Task GetHistoryByChunksAsync(
            AssetPair assetPair, 
            CandlePriceType priceType, 
            DateTime endDate, 
            ICandle endCandle, 
            Func<IReadOnlyList<ICandle>, Task> readChunkFunc, 
            CancellationToken cancellationToken)
        {
            await _feedHistoryRepository.GetCandlesByChunksAsync(assetPair.Id, priceType, endDate, async feedHistoryItems =>
            {
                foreach (var feedHistory in feedHistoryItems)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    var candles = _missedCandlesGenerator.FillGapUpTo(assetPair, feedHistory)
                        .Where(c => c.Timestamp < endDate)
                        .ToArray();

                    if (candles.Any())
                    {
                        await readChunkFunc(candles);
                    }
                }
            });

            if (cancellationToken.IsCancellationRequested)
            {
                return;
            }

            var endingCandles = _missedCandlesGenerator.FillGapUpTo(assetPair, priceType, endDate, endCandle);

            if (endingCandles.Any())
            {
                await readChunkFunc(endingCandles);
            }
        }
    }
}
