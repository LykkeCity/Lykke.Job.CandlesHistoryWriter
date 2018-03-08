using System;
using System.Threading.Tasks;
using Common;
using Common.Log;
using JetBrains.Annotations;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.Candles;
using StackExchange.Redis;

namespace Lykke.Job.CandlesHistoryWriter.Services.Candles
{
    [UsedImplicitly]
    public class RedisCacheTruncator : TimerPeriod
    {
        private readonly ICandlesHistoryRepository _historyRepository;
        private readonly IDatabase _database;
        private readonly MarketType _market;
        private readonly int _amountOfCandlesToStore;

        public RedisCacheTruncator(
            ICandlesHistoryRepository historyRepository,
            IDatabase database,
            MarketType market,
            TimeSpan cacheCleanupPeriod, 
            int amountOfCandlesToStore,
            ILog log)
            : base(nameof(RedisCacheTruncator), (int)cacheCleanupPeriod.TotalMilliseconds, log)
        {
            _historyRepository = historyRepository;
            _database = database;
            _market = market;
            _amountOfCandlesToStore = amountOfCandlesToStore;
        }

        public override async Task Execute()
        {
            foreach (var assetId in _historyRepository.GetStoredAssetPairs())
            {
                foreach (var priceType in Constants.StoredPriceTypes)
                {
                    foreach (var timeInterval in Constants.StoredIntervals)
                    {
                        await _database.SortedSetRemoveRangeByRankAsync(RedisCandlesCacheService.GetKey(_market, assetId, priceType, timeInterval), 0, -_amountOfCandlesToStore - 1);
                    }
                }
            }
        }
    }
}
