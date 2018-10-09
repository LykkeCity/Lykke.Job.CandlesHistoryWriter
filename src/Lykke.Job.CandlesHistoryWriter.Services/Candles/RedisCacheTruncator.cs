using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Common;
using Common.Log;
using JetBrains.Annotations;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.Candles;
using MarginTrading.SettingsService.Contracts;
using StackExchange.Redis;

namespace Lykke.Job.CandlesHistoryWriter.Services.Candles
{
    [UsedImplicitly]
    public class RedisCacheTruncator : TimerPeriod
    {
        private readonly IAssetPairsApi _assetPairsApi;
        private readonly IDatabase _database;
        private readonly MarketType _market;
        private readonly int _amountOfCandlesToStore;

        public RedisCacheTruncator(
            IAssetPairsApi assetPairsApi,
            IDatabase database,
            MarketType market,
            TimeSpan cacheCleanupPeriod, 
            int amountOfCandlesToStore,
            ILog log)
            : base(nameof(RedisCacheTruncator), (int)cacheCleanupPeriod.TotalMilliseconds, log)
        {
            _assetPairsApi = assetPairsApi;
            _database = database;
            _market = market;
            _amountOfCandlesToStore = amountOfCandlesToStore;
        }

        public override async Task Execute()
        {
            foreach (var assetId in (await _assetPairsApi.List()).Select(x => x.Id))
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
    }
}
