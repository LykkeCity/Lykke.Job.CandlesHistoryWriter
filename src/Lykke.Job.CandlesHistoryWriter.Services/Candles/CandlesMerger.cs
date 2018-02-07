using System;
using System.Collections.Generic;
using System.Linq;
using Common;
using Lykke.Job.CandlesProducer.Contract;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.Candles;

namespace Lykke.Job.CandlesHistoryWriter.Services.Candles
{
    public static class CandlesMerger
    {
        /// <summary>
        /// Merges all of candles placed in chronological order
        /// </summary>
        /// <param name="candles">Candles in hronological order</param>
        /// <param name="newTimestamp">
        /// <see cref="ICandle.Timestamp"/> of merged candle, if not specified, 
        /// then <see cref="ICandle.Timestamp"/> of all candles should be equals, 
        /// and it will be used as merged candle <see cref="ICandle.Timestamp"/>
        /// </param>
        /// <returns>Merged candle, or null, if no candles to merge</returns>
        private static ICandle MergeAll(IEnumerable<ICandle> candles, DateTime? newTimestamp = null)
        {
            if (candles == null)
            {
                return null;
            }

            var open = 0d;
            var close = 0d;
            var high = 0d;
            var low = 0d;
            var tradingVolume = 0d;
            var tradingOppositeVolume = 0d;
            var lastTradePrice = 0d;
            var assetPairId = string.Empty;
            var priceType = CandlePriceType.Unspecified;
            var timeInterval = CandleTimeInterval.Unspecified;
            var timestamp = DateTime.MinValue;
            var lastUpdateTimestamp = DateTime.MinValue;
            var count = 0;

            using (var enumerator = candles.GetEnumerator())
            {
                while (enumerator.MoveNext())
                {
                    var candle = enumerator.Current;

                    if (count == 0)
                    {
                        open = candle.Open;
                        close = candle.Close;
                        high = candle.High;
                        low = candle.Low;
                        tradingVolume = candle.TradingVolume;
                        tradingOppositeVolume = candle.TradingOppositeVolume;
                        lastTradePrice = candle.LastTradePrice;
                        assetPairId = candle.AssetPairId;
                        priceType = candle.PriceType;
                        timeInterval = candle.TimeInterval;
                        timestamp = candle.Timestamp;
                        lastUpdateTimestamp = candle.LastUpdateTimestamp;
                    }
                    else
                    {
                        if (assetPairId != candle.AssetPairId)
                        {
                            throw new InvalidOperationException($"Can't merge candles of different asset pairs. Current candle={candle.ToJson()}");
                        }

                        if (priceType != candle.PriceType)
                        {
                            throw new InvalidOperationException($"Can't merge candles of different price types. Current candle={candle.ToJson()}");
                        }

                        if (timeInterval != candle.TimeInterval)
                        {
                            throw new InvalidOperationException($"Can't merge candles of different time intervals. Current candle={candle.ToJson()}");
                        }

                        if (!newTimestamp.HasValue && timestamp != candle.Timestamp)
                        {
                            throw new InvalidOperationException($"Can't merge candles with different timestamps. Current candle={candle.ToJson()}");
                        }

                        close = candle.Close;
                        high = Math.Max(high, candle.High);
                        low = Math.Min(low, candle.Low);
                        tradingVolume += candle.TradingVolume;
                        tradingOppositeVolume += candle.TradingOppositeVolume;
                        lastUpdateTimestamp = candle.LastUpdateTimestamp > lastUpdateTimestamp
                            ? candle.LastUpdateTimestamp
                            : lastUpdateTimestamp;
                        lastTradePrice = candle.LastUpdateTimestamp > lastUpdateTimestamp
                            ? candle.LastTradePrice
                            : lastTradePrice;
                    }

                    count++;
                }
            }

            if (count > 0)
            {
                return Candle.Create(
                    open: open,
                    close: close,
                    high: high,
                    low: low,
                    assetPair: assetPairId,
                    priceType: priceType,
                    timeInterval: timeInterval,
                    timestamp: newTimestamp ?? timestamp,
                    tradingVolume: tradingVolume,
                    tradingOppositeVolume: tradingOppositeVolume,
                    lastTradePrice: lastTradePrice,
                    lastUpdateTimestamp: lastUpdateTimestamp);
            }

            return null;
        }

        /// <summary>
        /// Merges candles into bigger intervals (e.g. Minute -> Min15).
        /// </summary>
        /// <param name="candles">Candles to merge</param>
        /// <param name="newInterval">New interval</param>
        public static IEnumerable<ICandle> MergeIntoBiggerIntervals(IEnumerable<ICandle> candles, CandleTimeInterval newInterval)
        {
            return candles
                .GroupBy(c => c.Timestamp.TruncateTo(newInterval))
                .Select(g => MergeAll(g, g.Key));
        }
    }
}
