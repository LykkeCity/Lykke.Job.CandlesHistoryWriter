using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Common;
using JetBrains.Annotations;
using Lykke.Job.CandlesProducer.Contract;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.Candles;
using Lykke.Job.CandlesHistoryWriter.Core.Services;
using Lykke.Job.CandlesHistoryWriter.Core.Services.Candles;
using MoreLinq;
using StackExchange.Redis;

namespace Lykke.Job.CandlesHistoryWriter.Services.Candles
{
    /// <summary>
    /// Caches candles in the redis using lexographical indexes with candles data as the auxiliary information of the index
    /// </summary>
    [UsedImplicitly]
    public class RedisCandlesCacheService : ICandlesCacheService
    {
        private const string TimestampFormat = "yyyyMMddHHmmss";

        private readonly ICandlesCacheSemaphore _cacheSem;

        private readonly IHealthService _healthService;
        private readonly IDatabase _database;
        private readonly MarketType _market;
        private SlotType? _activeSlot;

        public RedisCandlesCacheService(ICandlesCacheSemaphore cacheSem, IHealthService healthService, IDatabase database, MarketType market)
        {
            _cacheSem = cacheSem ?? throw new ArgumentNullException(nameof(cacheSem));
            _healthService = healthService ?? throw new ArgumentNullException(nameof(healthService));
            _database = database ?? throw new ArgumentNullException(nameof(database));
            _market = market;
        }

        public IImmutableDictionary<string, IImmutableList<ICandle>> GetState()
        {
            throw new NotSupportedException();
        }

        public void SetState(IImmutableDictionary<string, IImmutableList<ICandle>> state)
        {
            throw new NotSupportedException();
        }

        public string DescribeState(IImmutableDictionary<string, IImmutableList<ICandle>> state)
        {
            throw new NotSupportedException();
        }

        public async Task InitializeAsync(
            string assetPairId, 
            CandlePriceType priceType,
            CandleTimeInterval timeInterval,
            IReadOnlyCollection<ICandle> candles,
            SlotType slotType)
        {
            foreach (var candle in candles)
            {
                if (candle.AssetPairId != assetPairId)
                {
                    throw new ArgumentException($"Candle {candle.ToJson()} has invalid AssetPriceId", nameof(candles));
                }
                if (candle.PriceType != priceType)
                {
                    throw new ArgumentException($"Candle {candle.ToJson()} has invalid PriceType", nameof(candles));
                }
                if (candle.TimeInterval != timeInterval)
                {
                    throw new ArgumentException($"Candle {candle.ToJson()} has invalid TimeInterval", nameof(candles));
                }
            }

            // Since we have introduced a semaphore waiting in cache initialization service, we do not
            // need any additional concurrent-safe actions here. Yes, it's better to wait a semaphore
            // somewhere else but here 'cause otherwise we'll get a fully synchronous cache operation.

            var key = GetKey(_market, assetPairId, priceType, timeInterval, slotType);

            await _database.KeyDeleteAsync(key);

            foreach (var candlesBatch in candles.Batch(100))
            {
                var entites = candlesBatch
                    .Select(candle => new SortedSetEntry(SerializeCandle(candle), 0))
                    .ToArray();

                await _database.SortedSetAddAsync(key, entites);
            }
        }

        public async Task CacheAsync(IReadOnlyList<ICandle> candles)
        {
            // To avoid possible race condition with cache initialization triggered by timer:
            await _cacheSem.WaitAsync();

            try
            {
                _healthService.TraceStartCacheCandles();

                // Transaction is needed here, despite of this method is non concurrent-safe,
                // without transaction at the moment candle can be missed or doubled
                // depending on the order of the remove/add calls

                var transaction = _database.CreateTransaction();
                var tasks = new List<Task>();
                SlotType activeSlot = await GetActiveSlotAsync(_market);

                foreach (var candle in candles)
                {
                    var key = GetKey(_market, candle.AssetPairId, candle.PriceType, candle.TimeInterval, activeSlot);
                    var serializedValue = SerializeCandle(candle);

                    // Removes old candle

                    var currentCandleKey = candle.Timestamp.ToString(TimestampFormat);
                    var nextCandleKey = candle.Timestamp.AddIntervalTicks(1, candle.TimeInterval)
                        .ToString(TimestampFormat);

                    tasks.Add(transaction.SortedSetRemoveRangeByValueAsync(key, currentCandleKey, nextCandleKey,
                        Exclude.Stop));

                    // Adds new candle

                    tasks.Add(transaction.SortedSetAddAsync(key, serializedValue, 0));
                }

                if (!await transaction.ExecuteAsync())
                {
                    throw new InvalidOperationException("Redis transaction is rolled back");
                }

                // Operations in the transaction can't be awaited before transaction is executed, so
                // saves tasks and waits they here, just to calm down the Resharper

                await Task.WhenAll(tasks);

                _healthService.TraceStopCacheCandles(candles.Count);

            }
            finally
            {
                _cacheSem.Release();
            }
        }

        private static byte[] SerializeCandle(ICandle candle)
        {
            // result is: 
            // 0 .. TimestampFormat.Length - 1 bytes: timestamp as yyyyMMddHHmmss in ASCII
            // TimestampFormat.Length .. end bytes: serialized RedistCachedCandle

            var timestampString = candle.Timestamp.ToString(TimestampFormat);
            var timestampBytes = Encoding.ASCII.GetBytes(timestampString);

            using (var stream = new MemoryStream())
            {
                stream.Write(timestampBytes, 0, timestampBytes.Length);

                var cachedCandle = new RedisCachedCandle
                {
                    Open = candle.Open,
                    Close = candle.Close,
                    Low = candle.Low,
                    High = candle.High,
                    TradingVolume = candle.TradingVolume,
                    TradingOppositVolume = candle.TradingOppositeVolume,
                    LastTradePrice = candle.LastTradePrice,
                    LastUpdateTimestamp = candle.LastUpdateTimestamp
                };

                MessagePack.MessagePackSerializer.Serialize(stream, cachedCandle);

                stream.Flush();

                return stream.ToArray();
            }
        }

        public async Task InjectCacheValidityToken()
        {
            var vkey = GetValidationKey(_market);

            await _database.KeyDeleteAsync(vkey); // The operation is ignored is the key does not exist

            await _database.SetAddAsync(vkey, $"CandlesHistoryCacheIsStillValidIfYouCanSeeMe.LastKeyUpdate-{DateTime.UtcNow}");
        }

        public bool CheckCacheValidity()
        {
            var vkey = GetValidationKey(_market);
            return _database.KeyExists(vkey);
        }

        public void TruncateCache(string assetId, CandlePriceType priceType, CandleTimeInterval timeInterval, int storedCandlesCountLimit, SlotType slotType)
        {
            var key = GetKey(_market, assetId, priceType, timeInterval, slotType);

            _database.SortedSetRemoveRangeByRank(key, 0, -storedCandlesCountLimit - 1, CommandFlags.FireAndForget);
        }

        public async Task<SlotType> GetActiveSlotAsync(MarketType marketType)
        {
            if (_activeSlot != null)
                return _activeSlot.Value;
            
            var key = GetActiveSlotKey(marketType);
            
            var value = await _database.StringGetAsync(key);

            if (value.HasValue)
            {
                _activeSlot = Enum.Parse<SlotType>(value);
            }
            else
            {
                _database.StringSet(key, SlotType.Slot0.ToString());
                _activeSlot = SlotType.Slot0;
            }
            
            return _activeSlot.Value;
        }

        public void SetActiveSlot(MarketType marketType, SlotType slotType)
        {
            var key = GetActiveSlotKey(marketType);
            _database.StringSet(key, slotType.ToString());
            _activeSlot = slotType;
        }

        private static string GetKey(MarketType market, string assetPairId, CandlePriceType priceType, CandleTimeInterval timeInterval, SlotType slotType)
        {
            return $"CandlesHistory:{market}:{slotType.ToString()}:{assetPairId}:{priceType}:{timeInterval}";
        }

        private static string GetValidationKey(MarketType market)
        {
            return $"CandlesHistory:{market}:ValidationToken";
        }
        
        private static string GetActiveSlotKey(MarketType market)
        {
            return $"CandlesHistory:{market}:ActiveSlot";
        }
    }
}
