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

        private readonly IHealthService _healthService;
        private readonly IDatabase _database;
        private readonly MarketType _market;

        public RedisCandlesCacheService(IHealthService healthService, IDatabase database, MarketType market)
        {
            _healthService = healthService;
            _database = database;
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

            var key = GetKey(_market, assetPairId, priceType, timeInterval);

            // Cleans cache

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
            _healthService.TraceStartCacheCandles();

            // Transaction is needed here, despite of this method is non concurrent-safe,
            // without transaction at the moment candle can be missed or doubled
            // depending on the order of the remove/add calls

            var transaction = _database.CreateTransaction();
            var tasks = new List<Task>();

            foreach (var candle in candles)
            {  
                var key = GetKey(_market, candle.AssetPairId, candle.PriceType, candle.TimeInterval);
                var serializedValue = SerializeCandle(candle);

                // Removes old candle

                var currentCandleKey = candle.Timestamp.ToString(TimestampFormat);
                var nextCandleKey = candle.Timestamp.AddIntervalTicks(1, candle.TimeInterval).ToString(TimestampFormat);

                tasks.Add(transaction.SortedSetRemoveRangeByValueAsync(key, currentCandleKey, nextCandleKey, Exclude.Stop));

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

        public static string GetKey(MarketType market, string assetPairId, CandlePriceType priceType, CandleTimeInterval timeInterval)
        {
            return $"CandlesHistory:{market}:{assetPairId}:{priceType}:{timeInterval}";
        }

        public static string GetAssetPairsRootKey(MarketType market)
        {
            return $"CandlesHistory:{market}";
        }
    }
}
