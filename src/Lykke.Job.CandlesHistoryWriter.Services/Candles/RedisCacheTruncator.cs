// Copyright (c) 2019 Lykke Corp.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Common;
using Common.Log;
using JetBrains.Annotations;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.Candles;
using Lykke.Job.CandlesHistoryWriter.Core.Services.Candles;
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
        private readonly ICandlesAmountManager _candlesAmountManager;

        public RedisCacheTruncator(
            IAssetPairsApi assetPairsApi,
            IDatabase database,
            MarketType market,
            TimeSpan cacheCleanupPeriod,
            ICandlesAmountManager candlesAmountManager,
            ILog log)
            : base(nameof(RedisCacheTruncator), (int)cacheCleanupPeriod.TotalMilliseconds, log)
        {
            _assetPairsApi = assetPairsApi;
            _database = database;
            _market = market;
            _candlesAmountManager = candlesAmountManager;
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

                        var candlesAmountToStore = _candlesAmountManager.GetCandlesAmountToStore(timeInterval);
                        _database.SortedSetRemoveRangeByRank(key, 0, -candlesAmountToStore - 1, CommandFlags.FireAndForget);
                    }
                }
            }
        }
    }
}
