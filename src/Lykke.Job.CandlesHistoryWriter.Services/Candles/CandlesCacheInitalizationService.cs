using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Common.Log;
using Lykke.Common.Log;
using Lykke.Service.Assets.Client.Models;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.Candles;
using Lykke.Job.CandlesHistoryWriter.Core.Services;
using Lykke.Job.CandlesHistoryWriter.Core.Services.Assets;
using Lykke.Job.CandlesHistoryWriter.Core.Services.Candles;
using Lykke.Job.CandlesProducer.Contract;
using MoreLinq;

namespace Lykke.Job.CandlesHistoryWriter.Services.Candles
{
    public class CandlesCacheInitalizationService : ICandlesCacheInitalizationService
    {
        private readonly ICandlesCacheSemaphore _cacheSem;
        private readonly ILog _log;
        private readonly IAssetPairsManager _assetPairsManager;
        private readonly IClock _clock;
        private readonly ICandlesCacheService _candlesCacheService;
        private readonly ICandlesHistoryRepository _candlesHistoryRepository;
        private readonly Dictionary<CandleTimeInterval, int> _amountOfCandlesToStore;
        private readonly MarketType _marketType;
        private readonly DateTime _minDate;

        private readonly object _initializationStateLocker = new object();

        public CacheInitializationState InitializationState { get; private set; }

        public CandlesCacheInitalizationService(
            ICandlesCacheSemaphore cacheSem,
            ILogFactory logFactory,
            IAssetPairsManager assetPairsManager,
            IClock clock,
            ICandlesCacheService candlesCacheService,
            ICandlesHistoryRepository candlesHistoryRepository,
            Dictionary<CandleTimeInterval, int> amountOfCandlesToStore,
            MarketType marketType,
            DateTime minDate)
        {
            _cacheSem = cacheSem ?? throw new ArgumentNullException(nameof(cacheSem));

            if (logFactory == null)
                throw new ArgumentNullException(nameof(logFactory));
            
            _log = logFactory.CreateLog(this);
            _assetPairsManager = assetPairsManager ?? throw new ArgumentNullException(nameof(assetPairsManager));
            _clock = clock ?? throw new ArgumentNullException(nameof(clock));
            _candlesCacheService = candlesCacheService ?? throw new ArgumentNullException(nameof(candlesCacheService));
            _candlesHistoryRepository = candlesHistoryRepository ?? throw new ArgumentNullException(nameof(candlesHistoryRepository));
            _amountOfCandlesToStore = amountOfCandlesToStore;
            _marketType = marketType;
            _minDate = minDate;

            InitializationState = CacheInitializationState.Idle;
        }

        public async Task InitializeCacheAsync()
        {
            // Depending on cache invalidation period and on asset pairs amount, there may be a case when
            // the invalidation timer fires before the cache loading has stopped. This will be a signal 
            // to skip timer-based invalidation.
            // Below we combine two approaches:
            // - we're exporting a fast signal for any timer-based routine that we have already been initializing the cache;
            // - and we additionally block all the cache-related operations for other parts of code.
            // The first is needed to avoid queueing of timer events. If we simply use blocks, we will finally face a problem
            // when cache initialization may become infinitly repeated. The second is important for avoidining a multi-threaded
            // write operations to the cache: if we've got a candle update set, we need to await for cache fill completion and
            // then proceed.
            lock (_initializationStateLocker)
            {
                if (InitializationState == CacheInitializationState.InProgress)
                    return;

                InitializationState = CacheInitializationState.InProgress;
            }

            await _cacheSem.WaitAsync();

            try
            {
                _log.Info(nameof(InitializeCacheAsync), "Caching candles history...");

                SlotType activeSlot = await _candlesCacheService.GetActiveSlotAsync(_marketType);
                
                //initialize cache to inactive slot
                SlotType initSlot = activeSlot == SlotType.Slot0 
                    ? SlotType.Slot1 
                    : SlotType.Slot0;

                var assetPairs = await _assetPairsManager.GetAllAsync();
                var now = _clock.UtcNow;

                foreach (var pairs in assetPairs.Where(a => _candlesHistoryRepository.CanStoreAssetPair(a.Id)).Batch(6))
                {
                    var tasks = pairs.Select(p => CacheAssetPairCandlesAsync(p, now, initSlot));
                    await Task.WhenAll(tasks);
                }

                await _candlesCacheService.InjectCacheValidityToken(); // Initial validation token set.

                await _candlesCacheService.SetActiveSlotAsync(_marketType, initSlot); //switch slots

                _log.Info(nameof(InitializeCacheAsync), "All candles history is cached");
            }
            finally
            {
                InitializationState = CacheInitializationState.Idle;
                _cacheSem.Release();
            }
        }

        private async Task CacheAssetPairCandlesAsync(AssetPair assetPair, DateTime now, SlotType slotType)
        {
            try
            {
                _log.Info(nameof(InitializeCacheAsync), $"Caching {assetPair.Id} candles history...");

                foreach (CandlePriceType priceType in Constants.StoredPriceTypes)
                {
                    foreach (CandleTimeInterval timeInterval in Constants.InitFromDbIntervals)
                    {
                        DateTime alignedToDate = now.TruncateTo(timeInterval).AddIntervalTicks(1, timeInterval);
                        DateTime alignedFromDate = alignedToDate.AddIntervalTicks(-_amountOfCandlesToStore[timeInterval] - 1, timeInterval);

                        if (alignedFromDate < _minDate)
                            alignedFromDate = _minDate.TruncateTo(timeInterval);
                        
                        ICandle[] candles = (await _candlesHistoryRepository.GetCandlesAsync(assetPair.Id, timeInterval, priceType, alignedFromDate, alignedToDate)).ToArray();

                        if (!candles.Any()) 
                            continue;
                        
                        var intervals = _candlesCacheService.GetRedisCacheIntervals(timeInterval);

                        foreach (var interval in intervals)
                        {
                            ICandle[] intervalCandles = interval != timeInterval
                                ? CandlesMerger.MergeIntoBiggerIntervals(candles, interval).ToArray()
                                : candles;

                            await _candlesCacheService.InitializeAsync(assetPair.Id, priceType, interval, intervalCandles, slotType);
                        }
                    }
                }

                _log.Info($"{assetPair.Id} candles history is cached.");
            }
            catch (Exception ex)
            {
                _log.Error(nameof(CacheAssetPairCandlesAsync), ex);
            }
        }
    }
}
