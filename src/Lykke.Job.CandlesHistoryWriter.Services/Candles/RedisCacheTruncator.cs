// Copyright (c) 2019 Lykke Corp.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Common;
using Common.Log;
using JetBrains.Annotations;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.Candles;
using Lykke.Job.CandlesHistoryWriter.Core.Services.Candles;
using Lykke.Job.CandlesHistoryWriter.Services.Settings;
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
        private readonly ICandlesShardValidator _candlesShardValidator;

        public RedisCacheTruncator(
            IAssetPairsApi assetPairsApi,
            IDatabase database,
            MarketType market,
            TimeSpan cacheCleanupPeriod,
            ICandlesAmountManager candlesAmountManager,
            ILog log, 
            ICandlesShardValidator candlesShardValidator)
            : base(nameof(RedisCacheTruncator), (int)cacheCleanupPeriod.TotalMilliseconds, log)
        {
            _assetPairsApi = assetPairsApi;
            _database = database;
            _market = market;
            _candlesAmountManager = candlesAmountManager;
            _candlesShardValidator = candlesShardValidator;
        }

        public override async Task Execute()
        {
            foreach (var assetId in (await _assetPairsApi.List()).Select(x => x.Id))
            {
                if (!_candlesShardValidator.CanHandle(assetId))
                    continue;

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
