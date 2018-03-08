using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Common;
using Common.Log;
using JetBrains.Annotations;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.Candles;
using Lykke.SettingsReader;
using StackExchange.Redis;

namespace Lykke.Job.CandlesHistoryWriter.Services.Candles
{
    [UsedImplicitly]
    public class RedisCacheTruncator : TimerPeriod
    {
        private readonly IReloadingManager<Dictionary<string, string>> _assetConnectionStrings;
        private readonly IDatabase _database;
        private readonly MarketType _market;
        private readonly int _amountOfCandlesToStore;

        public RedisCacheTruncator(
            IReloadingManager<Dictionary<string, string>> assetConnectionStrings,
            IDatabase database,
            MarketType market,
            TimeSpan cacheCleanupPeriod, 
            int amountOfCandlesToStore,
            ILog log)
            : base(nameof(QueueMonitor), (int)cacheCleanupPeriod.TotalMilliseconds, log)
        {
            _assetConnectionStrings = assetConnectionStrings;
            _database = database;
            _market = market;
            _amountOfCandlesToStore = amountOfCandlesToStore;
        }

        public override async Task Execute()
        {
            foreach (var assetId in _assetConnectionStrings.CurrentValue.Keys)
            {
                foreach (var priceType in Constants.StoredPriceTypes)
                {
                    foreach (var timeInterval in Constants.StoredIntervals)
                    {
                        await _database.SortedSetRemoveRangeByRankAsync($"CandlesHistory:{_market}:{assetId}:{priceType}:{timeInterval}", 0, -_amountOfCandlesToStore - 1);
                    }
                }
            }
        }
    }
}
