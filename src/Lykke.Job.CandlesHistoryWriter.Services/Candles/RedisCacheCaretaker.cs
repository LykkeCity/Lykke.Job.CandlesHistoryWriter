using System;
using System.Threading;
using System.Threading.Tasks;
using Autofac;
using Common;
using JetBrains.Annotations;
using Lykke.Common.Log;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.Candles;
using Lykke.Job.CandlesHistoryWriter.Core.Services.Candles;

namespace Lykke.Job.CandlesHistoryWriter.Services.Candles
{
    [UsedImplicitly]
    public class RedisCacheCaretaker : IStartable, IDisposable
    {
        private readonly ICandlesHistoryRepository _historyRepository;
        private readonly ICandlesCacheService _redisCacheService;
        private readonly ICandlesCacheInitalizationService _cacheInitalizationService;
        private readonly int _amountOfCandlesToStore;
        private readonly MarketType _marketType;

        private readonly TimerTrigger _maintainTicker;

        public RedisCacheCaretaker(
            ICandlesHistoryRepository historyRepository,
            ICandlesCacheService redisCacheService,
            ICandlesCacheInitalizationService cacheInitalizationService,
            TimeSpan cacheCheckupPeriod,
            int amountOfCandlesToStore,
            MarketType marketType,
            ILogFactory logFactory)
        {
            _historyRepository = historyRepository ?? throw new ArgumentNullException(nameof(historyRepository));
            _redisCacheService = redisCacheService ?? throw new ArgumentNullException(nameof(redisCacheService));
            _cacheInitalizationService = cacheInitalizationService ?? throw new ArgumentNullException(nameof(cacheInitalizationService));
            _amountOfCandlesToStore = amountOfCandlesToStore;
            _marketType = marketType;

            _maintainTicker = new TimerTrigger(nameof(RedisCacheCaretaker), cacheCheckupPeriod, logFactory);
            _maintainTicker.Triggered += MaintainTickerOnTriggered;
        }

        private async Task MaintainTickerOnTriggered(ITimerTrigger timer, TimerTriggeredHandlerArgs args, CancellationToken cancellationToken)
        {
            // TODO: such an approach is Ok for the case of the single running service instance. But once we get
            // TODO: a necessity to run more instances, the code below will provoke a problem.

            await TruncateCache();

            await ReloadCacheIfNeededAsync();
        }

        private async Task TruncateCache()
        {
            // Shall not truncate cache while reloading data.
            if (_cacheInitalizationService.InitializationState != CacheInitializationState.Idle)
                return;
            
            SlotType activeSlot = await _redisCacheService.GetActiveSlotAsync(_marketType);

            foreach (var assetId in _historyRepository.GetStoredAssetPairs())
            {
                foreach (var priceType in Constants.StoredPriceTypes)
                {
                    foreach (var timeInterval in Constants.StoredIntervals)
                    {
                        _redisCacheService.TruncateCache(assetId, priceType, timeInterval, _amountOfCandlesToStore, activeSlot);
                    }
                }
            }
        }

        private async Task ReloadCacheIfNeededAsync()
        {
            if ( ! _redisCacheService.CheckCacheValidity())
                await _cacheInitalizationService.InitializeCacheAsync();
        }

        public void Start()
        {
            _maintainTicker.Start();
        }

        public void Dispose()
        {
            _maintainTicker?.Stop();
            _maintainTicker?.Dispose();
        }
    }
}
