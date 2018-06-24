using System;
using System.Threading;
using System.Threading.Tasks;
using Common;
using Common.Log;
using JetBrains.Annotations;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.Candles;
using Lykke.Job.CandlesHistoryWriter.Core.Services.Candles;
using StackExchange.Redis;

namespace Lykke.Job.CandlesHistoryWriter.Services.Candles
{
    [UsedImplicitly]
    public class RedisCacheCaretaker : TimerPeriod
    {
        private readonly ICandlesHistoryRepository _historyRepository;
        private readonly IDatabase _database;
        private readonly ICandlesCacheInitalizationService _cacheInitiaInitalizationService;
        private readonly MarketType _market;
        private readonly int _amountOfCandlesToStore;

        public RedisCacheCaretaker(
            ICandlesHistoryRepository historyRepository,
            IDatabase database,
            ICandlesCacheInitalizationService cacheInitiaInitalizationService,
            MarketType market,
            TimeSpan cacheCheckupPeriod,
            int amountOfCandlesToStore,
            ILog log)
            : base(nameof(RedisCacheCaretaker), (int)cacheCheckupPeriod.TotalMilliseconds, log)
        {
            _historyRepository = historyRepository ?? throw new ArgumentNullException(nameof(historyRepository));
            _database = database ?? throw new ArgumentNullException(nameof(database));
            _cacheInitiaInitalizationService = cacheInitiaInitalizationService ?? throw new ArgumentNullException(nameof(cacheInitiaInitalizationService));
            _market = market;
            _amountOfCandlesToStore = amountOfCandlesToStore;
        }

        public override Task Execute()
        {
            // TODO: such an approach is Ok for the case of the single running service instance. But once we get
            // TODO: a necessity to run more instances, the code below will provoke a problem.

            TruncateCache();

            ReloadCacheIfNeeded();
            
            return Task.CompletedTask;
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
                        var key = RedisCandlesCacheService.GetKey(_market, assetId, priceType, timeInterval);

                        _database.SortedSetRemoveRangeByRank(key, 0, -_amountOfCandlesToStore - 1, CommandFlags.FireAndForget);
                    }
                }
            }
        }

        private void ReloadCacheIfNeeded()
        {
            var vkey = RedisCandlesCacheService.GetValidationKey(_market);
            if (_database.KeyExists(vkey))
                return;

            _cacheInitiaInitalizationService.InitializeCacheAsync()
                .GetAwaiter()
                .GetResult();
        }
    }
}
