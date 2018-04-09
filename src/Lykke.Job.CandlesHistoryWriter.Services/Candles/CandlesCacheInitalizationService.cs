﻿using System;
using System.Linq;
using System.Threading.Tasks;
using Common.Log;
using Lykke.Job.CandlesHistoryWriter.Core.Domain;
using Lykke.Job.CandlesProducer.Contract;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.Candles;
using Lykke.Job.CandlesHistoryWriter.Core.Services;
using Lykke.Job.CandlesHistoryWriter.Core.Services.Assets;
using Lykke.Job.CandlesHistoryWriter.Core.Services.Candles;

namespace Lykke.Job.CandlesHistoryWriter.Services.Candles
{
    public class CandlesCacheInitalizationService : ICandlesCacheInitalizationService
    {
        private readonly ILog _log;
        private readonly IAssetPairsManager _assetPairsManager;
        private readonly IClock _clock;
        private readonly ICandlesCacheService _candlesCacheService;
        private readonly ICandlesHistoryRepository _candlesHistoryRepository;
        private readonly int _amountOfCandlesToStore;

        public CandlesCacheInitalizationService(
            ILog log,
            IAssetPairsManager assetPairsManager,
            IClock clock,
            ICandlesCacheService candlesCacheService,
            ICandlesHistoryRepository candlesHistoryRepository,
            int amountOfCandlesToStore)
        {
            _log = log;
            _assetPairsManager = assetPairsManager;
            _clock = clock;
            _candlesCacheService = candlesCacheService;
            _candlesHistoryRepository = candlesHistoryRepository;
            _amountOfCandlesToStore = amountOfCandlesToStore;
        }

        public async Task InitializeCacheAsync()
        {
            await _log.WriteInfoAsync(nameof(CandlesCacheInitalizationService), nameof(InitializeCacheAsync), null, "Caching candles history...");

            var assetPairs = await _assetPairsManager.GetAllEnabledAsync();
            var now = _clock.UtcNow;
            var cacheAssetPairTasks = assetPairs
                .Where(a => _candlesHistoryRepository.CanStoreAssetPair(a.Id))
                .Select(assetPair => CacheAssetPairCandlesAsync(assetPair, now));

            await Task.WhenAll(cacheAssetPairTasks);

            await _log.WriteInfoAsync(nameof(CandlesCacheInitalizationService), nameof(InitializeCacheAsync), null, "All candles history is cached");
        }

        private async Task CacheAssetPairCandlesAsync(AssetPair assetPair, DateTime toDate)
        {
            await _log.WriteInfoAsync(nameof(CandlesCacheInitalizationService), nameof(InitializeCacheAsync), null, $"Caching {assetPair.Id} candles history...");

            foreach (var priceType in Constants.StoredPriceTypes)
            {
                foreach (var timeInterval in Constants.StoredIntervals)
                {
                    var alignedToDate = toDate.TruncateTo(timeInterval);
                    var alignedFromDate = alignedToDate.AddIntervalTicks(-_amountOfCandlesToStore, timeInterval);
                    var candles = await _candlesHistoryRepository.GetCandlesAsync(assetPair.Id, timeInterval, priceType, alignedFromDate, alignedToDate);
                    
                    await _candlesCacheService.InitializeAsync(assetPair.Id, priceType, timeInterval, candles.ToArray());
                }
            }

            await _log.WriteInfoAsync(nameof(CandlesCacheInitalizationService), nameof(InitializeCacheAsync), null, $"{assetPair.Id} candles history is cached");
        }
    }
}
