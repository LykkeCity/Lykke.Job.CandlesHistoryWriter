using System;
using System.Linq;
using System.Threading.Tasks;
using Common.Log;
using Lykke.Job.CandlesProducer.Contract;
using Lykke.Service.Assets.Client.Models;
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

        public CacheInitializationState InitializationState { get; private set; }

        public CandlesCacheInitalizationService(
            ILog log,
            IAssetPairsManager assetPairsManager,
            IClock clock,
            ICandlesCacheService candlesCacheService,
            ICandlesHistoryRepository candlesHistoryRepository,
            int amountOfCandlesToStore)
        {
            _log = log ?? throw new ArgumentNullException(nameof(log));
            _assetPairsManager = assetPairsManager ?? throw new ArgumentNullException(nameof(assetPairsManager));
            _clock = clock ?? throw new ArgumentNullException(nameof(clock));
            _candlesCacheService = candlesCacheService ?? throw new ArgumentNullException(nameof(candlesCacheService));
            _candlesHistoryRepository = candlesHistoryRepository ?? throw new ArgumentNullException(nameof(candlesHistoryRepository));
            _amountOfCandlesToStore = amountOfCandlesToStore;

            InitializationState = CacheInitializationState.Idle;
        }

        public async Task InitializeCacheAsync()
        {
            // Depending on cache invalidation period and on asset pairs amount, there may be a case when
            // the invalidation timer fires before the cache loading has stopped. This will be a signal 
            // to skip timer-based invalidation.
            if (InitializationState == CacheInitializationState.InProgress)
                return;

            InitializationState = CacheInitializationState.InProgress;

            try
            {
                await _log.WriteInfoAsync(nameof(CandlesCacheInitalizationService), nameof(InitializeCacheAsync), null,
                    "Caching candles history...");

                var assetPairs = await _assetPairsManager.GetAllEnabledAsync();
                var now = _clock.UtcNow;
                var cacheAssetPairTasks = assetPairs
                    .Where(a => _candlesHistoryRepository.CanStoreAssetPair(a.Id))
                    .Select(assetPair => CacheAssetPairCandlesAsync(assetPair, now));

                await Task.WhenAll(cacheAssetPairTasks);

                await _candlesCacheService.UpdateValidationToken(); // Initial validation token set.

                await _log.WriteInfoAsync(nameof(CandlesCacheInitalizationService), nameof(InitializeCacheAsync), null,
                    "All candles history is cached");
            }
            finally
            {
                InitializationState = CacheInitializationState.Idle;
            }
        }

        private async Task CacheAssetPairCandlesAsync(AssetPair assetPair, DateTime now)
        {
            await _log.WriteInfoAsync(nameof(CandlesCacheInitalizationService), nameof(InitializeCacheAsync), null, $"Caching {assetPair.Id} candles history...");

            foreach (var priceType in Constants.StoredPriceTypes)
            {
                foreach (var timeInterval in Constants.StoredIntervals)
                {
                    var alignedToDate = now.TruncateTo(timeInterval).AddIntervalTicks(1, timeInterval);
                    var alignedFromDate = alignedToDate.AddIntervalTicks(-_amountOfCandlesToStore - 1, timeInterval);
                    var candles = await _candlesHistoryRepository.GetCandlesAsync(assetPair.Id, timeInterval, priceType, alignedFromDate, alignedToDate);
                    
                    await _candlesCacheService.InitializeAsync(assetPair.Id, priceType, timeInterval, candles.ToArray());
                }
            }

            await _log.WriteInfoAsync(nameof(CandlesCacheInitalizationService), nameof(InitializeCacheAsync), null, $"{assetPair.Id} candles history is cached");
        }
    }
}
