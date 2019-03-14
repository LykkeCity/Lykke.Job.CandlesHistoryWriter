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

            var cacheTask = _candlesCacheService.CacheAsync(candles.Where(x => Constants.RedisIntervals.Contains(x.TimeInterval)).ToArray());

            foreach (var candle in candles.Where(x => Constants.DbStoredIntervals.Contains(x.TimeInterval)))
            {
                _candlesPersistenceQueue.EnqueueCandle(candle);
            }

            return cacheTask;
        }
    }
}
