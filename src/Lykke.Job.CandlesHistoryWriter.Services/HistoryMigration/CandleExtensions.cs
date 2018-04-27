using System;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.Candles;
using Lykke.Job.CandlesProducer.Contract;

namespace Lykke.Job.CandlesHistoryWriter.Services.HistoryMigration
{
    static class CandleExtensions
    {
        /// <summary>
        /// This method was originally developed for the purpose of Trades migrations from Azure SQL table.
        /// The table' structure allows only to operate with Id (autoincremental integer) and DateTime
        /// (rounded to seconds) fields. So, if we have 2 second candles with equal DateTime properties, we 
        /// need to checkup IDs: the lower is the older one. But this logic stops working when we do not
        /// have a warranty of candle processing order ascending by Id.
        /// </summary>
        /// <param name="self"></param>
        /// <param name="newCandle"></param>
        /// <returns></returns>
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
}
