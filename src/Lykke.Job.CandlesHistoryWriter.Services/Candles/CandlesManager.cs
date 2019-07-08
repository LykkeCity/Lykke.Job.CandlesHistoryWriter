// Copyright (c) 2019 Lykke Corp.
// See the LICENSE file in the project root for more information.

using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.Candles;
using Lykke.Job.CandlesHistoryWriter.Core.Services.Candles;

namespace Lykke.Job.CandlesHistoryWriter.Services.Candles
{
    public class CandlesManager : ICandlesManager
    {
        private readonly ICandlesCacheService _candlesCacheService;
        private readonly ICandlesPersistenceQueue _candlesPersistenceQueue;

        public CandlesManager(
            ICandlesCacheService candlesCacheService,
            ICandlesPersistenceQueue candlesPersistenceQueue)
        {
            _candlesCacheService = candlesCacheService;
            _candlesPersistenceQueue = candlesPersistenceQueue;
        }

        public Task ProcessCandlesAsync(IReadOnlyList<ICandle> candles)
        {
            if (!candles.Any())
            {
                return Task.CompletedTask;
            }

            var cacheTask = _candlesCacheService.CacheAsync(candles);

            foreach (var candle in candles)
            {
                _candlesPersistenceQueue.EnqueueCandle(candle);
            }

            return cacheTask;
        }
    }
}
