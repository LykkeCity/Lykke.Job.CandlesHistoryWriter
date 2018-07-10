using System;
using System.Threading;
using System.Threading.Tasks;
using Common;
using Common.Log;
using JetBrains.Annotations;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.Candles;
using Lykke.Job.CandlesHistoryWriter.Core.Services.Candles;

namespace Lykke.Job.CandlesHistoryWriter.Services.Candles
{
    [UsedImplicitly]
    public class RedisCacheCaretaker : IDisposable
    {
        private readonly ICandlesHistoryRepository _historyRepository;
        private readonly ICandlesCacheService _redisCacheService;
        private readonly ICandlesCacheInitalizationService _cacheInitiaInitalizationService;
        private readonly int _amountOfCandlesToStore;

        private readonly TimerTrigger _maintainTicker;

        public RedisCacheCaretaker(
            ICandlesHistoryRepository historyRepository,
            ICandlesCacheService redisCacheService,
            ICandlesCacheInitalizationService cacheInitiaInitalizationService,
            TimeSpan cacheCheckupPeriod,
            int amountOfCandlesToStore,
            ILog log)
        {
            _historyRepository = historyRepository ?? throw new ArgumentNullException(nameof(historyRepository));
            _redisCacheService = redisCacheService ?? throw new ArgumentNullException(nameof(redisCacheService));
            _cacheInitiaInitalizationService = cacheInitiaInitalizationService ?? throw new ArgumentNullException(nameof(cacheInitiaInitalizationService));
            _amountOfCandlesToStore = amountOfCandlesToStore;

            _maintainTicker = new TimerTrigger(nameof(RedisCacheCaretaker), cacheCheckupPeriod, log);
            _maintainTicker.Triggered += MaintainTickerOnTriggered;
        }

        private async Task MaintainTickerOnTriggered(ITimerTrigger timer, TimerTriggeredHandlerArgs args, CancellationToken cancellationToken)
        {
            // TODO: such an approach is Ok for the case of the single running service instance. But once we get
            // TODO: a necessity to run more instances, the code below will provoke a problem.

            TruncateCache();

            await ReloadCacheIfNeededAsync();
        }

        private void TruncateCache()
        {
            // Shall not truncate cache while reloading data.
            if (_cacheInitiaInitalizationService.InitializationState != CacheInitializationState.Idle)
                return;

            foreach (var assetId in _historyRepository.GetStoredAssetPairs())
            {
                foreach (var priceType in Constants.StoredPriceTypes)
                {
                    foreach (var timeInterval in Constants.StoredIntervals)
                    {
                        _redisCacheService.TruncateCache(assetId, priceType, timeInterval, _amountOfCandlesToStore);
                    }
                }
            }
        }

        private async Task ReloadCacheIfNeededAsync()
        {
            if ( ! _redisCacheService.CheckCacheValidity())
                await _cacheInitiaInitalizationService.InitializeCacheAsync();
        }

        public void Start()
        {
            _maintainTicker.Start();
        }

        public void Dispose()
        {
            _maintainTicker?.Stop();
        }
    }
}
