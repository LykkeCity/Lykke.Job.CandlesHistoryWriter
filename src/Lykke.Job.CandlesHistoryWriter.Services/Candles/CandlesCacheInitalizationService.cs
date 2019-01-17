using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Common.Log;
using Lykke.Common.Log;
using Lykke.Service.Assets.Client.Models;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.Candles;
using Lykke.Job.CandlesHistoryWriter.Core.Services;
using Lykke.Job.CandlesHistoryWriter.Core.Services.Assets;
using Lykke.Job.CandlesHistoryWriter.Core.Services.Candles;
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
        private readonly int _amountOfCandlesToStore;
        private readonly MarketType _marketType;
        private readonly Dictionary<string, double> _pairTime;

        private readonly object _initializationStateLocker = new object();

        public CacheInitializationState InitializationState { get; private set; }

        public CandlesCacheInitalizationService(
            ICandlesCacheSemaphore cacheSem,
            ILogFactory logFactory,
            IAssetPairsManager assetPairsManager,
            IClock clock,
            ICandlesCacheService candlesCacheService,
            ICandlesHistoryRepository candlesHistoryRepository,
            int amountOfCandlesToStore,
            MarketType marketType)
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

            InitializationState = CacheInitializationState.Idle;
            _pairTime = new Dictionary<string, double>();
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
                Console.WriteLine("Caching candles history...");

                SlotType activeSlot = await _candlesCacheService.GetActiveSlotAsync(_marketType);
                
                //initialize cache to inactive slot
                SlotType initSlot = activeSlot == SlotType.Slot0 
                    ? SlotType.Slot1 
                    : SlotType.Slot0;

                var assetPairs = await _assetPairsManager.GetAllAsync();
                var now = _clock.UtcNow;
                var cacheAssetPairTasks = assetPairs
                    .Where(a => _candlesHistoryRepository.CanStoreAssetPair(a.Id))
                    .Select(assetPair => CacheAssetPairCandlesAsync(assetPair, now, initSlot));

                foreach (var canlesTask in cacheAssetPairTasks.Batch(5))
                {
                    await Task.WhenAll(canlesTask);
                }

                await _candlesCacheService.InjectCacheValidityToken(); // Initial validation token set.

                await _candlesCacheService.SetActiveSlotAsync(_marketType, initSlot); //switch slots

                Console.WriteLine("All candles history is cached");
            }
            finally
            {
                InitializationState = CacheInitializationState.Idle;
                _cacheSem.Release();
            }
        }

        public void ShowStat()
        {
            Console.WriteLine($"Avg for pair processing: {_pairTime.Values.Sum()/_pairTime.Keys.Count} sec");
        }

        private async Task CacheAssetPairCandlesAsync(AssetPair assetPair, DateTime now, SlotType slotType)
        {
            try
            {
                var candlesSw = new Stopwatch();
                var pairSw = new Stopwatch();
                pairSw.Start();

                foreach (var priceType in Constants.StoredPriceTypes)
                {
                    foreach (var timeInterval in Constants.StoredIntervals)
                    {
                        candlesSw.Start();
                        var candles = await _candlesHistoryRepository.GetExactCandlesAsync(assetPair.Id, timeInterval, priceType, now, _amountOfCandlesToStore);
                        candlesSw.Stop();
                        
                        if (!candles.Any()) 
                            continue;

                        if (candlesSw.Elapsed.TotalSeconds > 2)
                            Console.WriteLine($"{priceType} {timeInterval} {assetPair.Id}: {candles.Count} [{candlesSw.Elapsed.TotalSeconds} sec.]");
                        
                        candlesSw.Reset();

                        await _candlesCacheService.InitializeAsync(assetPair.Id, priceType, timeInterval, candles, slotType);
                    }
                }

                pairSw.Stop();
                Console.WriteLine($"{assetPair.Id} candles history is cached [{pairSw.Elapsed}]");
                _pairTime.Add(assetPair.Id, pairSw.Elapsed.TotalSeconds);
                pairSw.Reset();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }
    }
}
