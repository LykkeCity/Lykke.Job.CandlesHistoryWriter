using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using Common;
using Lykke.Job.CandlesProducer.Contract;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.Candles;
using Lykke.Job.CandlesHistoryWriter.Core.Services.Candles;

namespace Lykke.Job.CandlesHistoryWriter.Services.Candles
{
    public class CandlesManager : ICandlesManager
    {
        private readonly ICandlesCacheService _candlesCacheService;
        private readonly ICandlesHistoryRepository _candlesHistoryRepository;
        private readonly ICandlesPersistenceQueue _candlesPersistenceQueue;

        public CandlesManager(
            ICandlesCacheService candlesCacheService,
            ICandlesHistoryRepository candlesHistoryRepository,
            ICandlesPersistenceQueue candlesPersistenceQueue)
        {
            _candlesCacheService = candlesCacheService;
            _candlesHistoryRepository = candlesHistoryRepository;
            _candlesPersistenceQueue = candlesPersistenceQueue;
        }

        public async Task ProcessCandleAsync(ICandle candle)
        {
            try
            {
                if (!_candlesHistoryRepository.CanStoreAssetPair(candle.AssetPairId))
                {
                    return;
                }

                if (!Constants.StoredIntervals.Contains(candle.TimeInterval))
                {
                    return;
                }
                
                await _candlesCacheService.CacheAsync(candle);
                _candlesPersistenceQueue.EnqueueCandle(candle);
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Failed to process candle: {candle.ToJson()}", ex);
            }
        }        
    }
}
