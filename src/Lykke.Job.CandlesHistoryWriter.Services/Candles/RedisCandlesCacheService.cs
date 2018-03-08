using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Autofac;
using Common;
using JetBrains.Annotations;
using Lykke.Job.CandlesProducer.Contract;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.Candles;
using Lykke.Job.CandlesHistoryWriter.Core.Services.Candles;
using MoreLinq;
using StackExchange.Redis;

namespace Lykke.Job.CandlesHistoryWriter.Services.Candles
{
    /// <summary>
    /// Caches candles in the redis using lexographical indexes with candles data as the auxiliary information of the index
    /// </summary>
    [UsedImplicitly]
    public class RedisCandlesCacheService : ICandlesCacheService, IStartable, IStopable
    {
        private const string TimestampFormat = "yyyyMMddHHmmss";

        private readonly IDatabase _database;
        private readonly MarketType _market;
        private readonly int _amountOfCandlesToStore;
        private readonly TimeSpan _cacheCleanupPeriod;
        private DateTime _lastCleanupTime;

        public RedisCandlesCacheService(IDatabase database, MarketType market, int amountOfCandlesToStore, TimeSpan cacheCleanupPeriod)
        {
            _database = database;
            _market = market;
            _amountOfCandlesToStore = amountOfCandlesToStore;
            _cacheCleanupPeriod = cacheCleanupPeriod;

            _lastCleanupTime = DateTime.UtcNow;
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
            IReadOnlyCollection<ICandle> candles)
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

            // TODO: This is non concurrent-safe update

            var key = GetKey(assetPairId, priceType, timeInterval);

            // Cleans cache

            await _database.SortedSetRemoveRangeByRankAsync(key, 0, -1);

            foreach (var candlesBatch in candles.Batch(100))
            {
                var entites = candlesBatch
                    .Select(candle => new SortedSetEntry(SerializeCandle(candle), 0))
                    .ToArray();

                await _database.SortedSetAddAsync(key, entites);
            }
        }

        public async Task CacheAsync(ICandle candle)
        {
            // TODO: This is non concurrent-safe method

            var key = GetKey(candle.AssetPairId, candle.PriceType, candle.TimeInterval);
            var serializedValue = SerializeCandle(candle);
            
            // Peek the top candle from cache
            var candleOnTop = DeserializeCandle(_database.ListGetByIndex(key, -1), null, CandlePriceType.Unspecified, CandleTimeInterval.Unspecified);

            // Transaction is needed here, despite of this method is non concurrent-safe,
            // without transaction at the moment candle can be missed or doubled
            // depending on the order of the remove/add calls

            var transaction = _database.CreateTransaction();

            // If the top candle is the current one, we need to remove it first, and then add once again with updated content.
            // This is important because we may have several candle updates during its time interval.
            if (DateTime.UtcNow.TruncateTo(candle.TimeInterval).CompareTo(candleOnTop.Timestamp) == 0)
                await transaction.SortedSetRemoveRangeByRankAsync(key, -1, -1);

            // Adds new candle
            await transaction.SortedSetAddAsync(key, serializedValue, 0);

            if (!await transaction.ExecuteAsync())
            {
                throw new InvalidOperationException("Redis transaction is rolled back");
            }

            if (_lastCleanupTime.Add(_cacheCleanupPeriod) >= DateTime.UtcNow)
            {
                await _database.SortedSetRemoveRangeByRankAsync(key, 0, -_amountOfCandlesToStore - 1);
                _lastCleanupTime = DateTime.UtcNow;
            }
        }

        public async Task<IEnumerable<ICandle>> GetCandlesAsync(string assetPairId, CandlePriceType priceType, CandleTimeInterval timeInterval, DateTime fromMoment, DateTime toMoment)
        {
            var key = GetKey(assetPairId, priceType, timeInterval);
            var from = fromMoment.ToString(TimestampFormat);
            var to = toMoment.ToString(TimestampFormat);
            var serializedValues = await _database.SortedSetRangeByValueAsync(key, from, to, Exclude.Stop);
            
            return serializedValues.Select(v => DeserializeCandle(v, assetPairId, priceType, timeInterval));
        }

        private static ICandle DeserializeCandle(byte[] value, string assetPairId, CandlePriceType priceType, CandleTimeInterval timeInterval)
        {
            // value is: 
            // 0 .. TimestampFormat.Length - 1 bytes: timestamp as yyyyMMddHHmmss in ASCII
            // TimestampFormat.Length .. end bytes: serialized RedistCachedCandle

            var timestampLength = TimestampFormat.Length;
            var timestampString = Encoding.ASCII.GetString(value, 0, timestampLength);
            var timestamp = DateTime.ParseExact(timestampString, TimestampFormat, CultureInfo.InvariantCulture);

            using (var stream = new MemoryStream(value, timestampLength, value.Length - timestampLength, writable: false))
            {
                var cachedCandle = MessagePack.MessagePackSerializer.Deserialize<RedisCachedCandle>(stream);

                return Candle.Create(
                    assetPairId,
                    priceType,
                    timeInterval,
                    timestamp,
                    cachedCandle.Open,
                    cachedCandle.Close,
                    cachedCandle.High,
                    cachedCandle.Low,
                    cachedCandle.TradingVolume,
                    cachedCandle.TradingOppositVolume,
                    cachedCandle.LastTradePrice,
                    cachedCandle.LastUpdateTimestamp);
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

        private string GetKey(string assetPairId, CandlePriceType priceType, CandleTimeInterval timeInterval)
        {
            return $"CandlesHistory:{_market}:{assetPairId}:{priceType}:{timeInterval}";
        }

        public void Start()
        {
            throw new NotImplementedException();
        }

        public void Dispose()
        {
            // do nothing
        }

        public void Stop()
        {
            throw new NotImplementedException();
        }
    }
}
