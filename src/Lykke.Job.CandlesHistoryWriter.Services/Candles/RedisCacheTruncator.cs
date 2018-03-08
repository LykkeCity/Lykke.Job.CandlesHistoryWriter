using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Common;
using Common.Log;
using JetBrains.Annotations;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.Candles;
using Lykke.Job.CandlesProducer.Contract;
using Lykke.SettingsReader;
using StackExchange.Redis;

namespace Lykke.Job.CandlesHistoryWriter.Services.Candles
{
    [UsedImplicitly]
    public class RedisCacheTruncator : TimerPeriod
    {
        private readonly IDatabase _database;
        private readonly int _amountOfCandlesToStore;

        private readonly List<string> _cacheKeys;

        public RedisCacheTruncator(
            IReloadingManager<Dictionary<string, string>> assetConnectionStrings,
            IDatabase database,
            MarketType market,
            TimeSpan cacheCleanupPeriod, 
            int amountOfCandlesToStore,
            ILog log)
            : base(nameof(QueueMonitor), (int)cacheCleanupPeriod.TotalMilliseconds, log)
        {
            _database = database;
            _amountOfCandlesToStore = amountOfCandlesToStore;

            var priceTypes = Enum.GetValues(typeof(CandlePriceType));
            var timeIntervals = Enum.GetValues(typeof(CandleTimeInterval));

            _cacheKeys = new List<string>();

            foreach (var assetId in assetConnectionStrings.CurrentValue.Keys)
            {
                foreach (var priceType in priceTypes)
                {
                    foreach (var timeInterval in timeIntervals)
                    {
                        var keyToTest = $"CandlesHistory:{market}:{assetId}:{priceType}:{timeInterval}";
                        if (_database.KeyExists(keyToTest)) _cacheKeys.Add(keyToTest);
                    }
                }
            }
        }

        public override async Task Execute()
        {
            foreach (var key in _cacheKeys)
                await _database.SortedSetRemoveRangeByRankAsync(key, 0, -_amountOfCandlesToStore - 1);
        }
    }
}
