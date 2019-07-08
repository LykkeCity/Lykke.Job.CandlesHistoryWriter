// Copyright (c) 2019 Lykke Corp.
// See the LICENSE file in the project root for more information.

using System;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.Candles;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.HistoryMigration.HistoryProviders.TradesSQLHistory;
using Lykke.Job.CandlesProducer.Contract;

namespace Lykke.Job.CandlesHistoryWriter.Services.HistoryMigration
{
    internal static class CandleExtensions
    {
        /// <summary>
        /// Extends a candle by another one candle. These two candles must be compatible by TimeStamp, TimeInterval and PriceType.
        /// </summary>
        public static ICandle ExtendBy(this ICandle self, ICandle newCandle)
        {
            if (self.Timestamp != newCandle.Timestamp)
                throw new InvalidOperationException("It's impossible to extend a candle by another one with the different time stamp.");

            if (self.TimeInterval != newCandle.TimeInterval)
                throw new InvalidOperationException($"It's impossible to extend a candle with time interval {self.TimeInterval} by another one with {newCandle.TimeInterval}.");

            if (self.PriceType != newCandle.PriceType)
                throw new InvalidOperationException($"It's impossible to extend a candle eith price type {self.PriceType} by another one with {newCandle.PriceType}");

            var selfIsOlder = self.LastUpdateTimestamp <= newCandle.LastUpdateTimestamp;

            return Candle.Create(
                self.AssetPairId,
                self.PriceType,
                self.TimeInterval,
                self.Timestamp,
                selfIsOlder ? self.Open : newCandle.Open,
                selfIsOlder ? newCandle.Close : self.Close,
                Math.Max(self.High, newCandle.High),
                Math.Min(self.Low, newCandle.Low),
                Convert.ToDouble((decimal)self.TradingVolume + (decimal)newCandle.TradingVolume), // It's a bit messy, but really allows to keep precision in addiction operation.
                Convert.ToDouble((decimal)self.TradingOppositeVolume + (decimal)newCandle.TradingOppositeVolume),
                selfIsOlder ? newCandle.LastTradePrice : self.LastTradePrice,
                selfIsOlder ? newCandle.LastUpdateTimestamp : self.LastUpdateTimestamp
            );
        }

        /// <summary>
        /// Extends a candle by a trade, if trade's DateTime corresponds to candle's TimeStamp (i.e., the trade belongs to the same time period).
        /// </summary>
        public static ICandle ExtendBy(this ICandle self, TradeHistoryItem trade, decimal volumeMultiplier = 1.0M)
        {
            var tradeCandle = trade.CreateCandle(self.AssetPairId, self.PriceType, self.TimeInterval, volumeMultiplier);

            return self.ExtendBy(tradeCandle);
        }

        /// <summary>
        /// Creates a new candle with all of the parameter values from self but with new time interval.
        /// </summary>
        public static ICandle RebaseToInterval(this ICandle self, CandleTimeInterval newInterval)
        {
            return Candle.Create(
                self.AssetPairId,
                self.PriceType,
                newInterval,
                self.Timestamp.TruncateTo(newInterval),
                self.Open,
                self.Close,
                self.High,
                self.Low,
                self.TradingVolume,
                self.TradingOppositeVolume,
                self.LastTradePrice,
                self.LastUpdateTimestamp
            );
        }
    }

    internal static class TradeHistoryItemExtensions
    {
        /// <summary>
        /// Detects if the given trade item lays in time borders of the given candle.
        /// </summary>
        public static bool BelongsTo(this TradeHistoryItem trade, ICandle candle)
        {
            return trade.DateTime.TruncateTo(candle.TimeInterval) == candle.Timestamp;
        }

        public static ICandle CreateCandle(this TradeHistoryItem trade, string assetPairId, CandlePriceType priceType, CandleTimeInterval interval, decimal volumeMultiplier = 1.0M)
        {
            return Candle.Create(
                assetPairId,
                priceType,
                interval,
                trade.DateTime.TruncateTo(interval),
                (double)trade.Price,
                (double)trade.Price,
                (double)trade.Price,
                (double)trade.Price,
                Convert.ToDouble((trade.IsStraight ? trade.Volume : trade.OppositeVolume) * volumeMultiplier),
                Convert.ToDouble((trade.IsStraight ? trade.OppositeVolume : trade.Volume) * volumeMultiplier),
                0, // Last Trade Price is enforced to be = 0
                trade.DateTime
            );
        }
    }
}
