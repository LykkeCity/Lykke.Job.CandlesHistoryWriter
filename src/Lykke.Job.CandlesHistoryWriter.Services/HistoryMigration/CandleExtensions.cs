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
            if (self.Timestamp.CompareTo(newCandle.Timestamp) != 0 ||
                self.TimeInterval != newCandle.TimeInterval ||
                self.PriceType != newCandle.PriceType)
                return self;

            var selfIsOlder = self.LastUpdateTimestamp.CompareTo(newCandle.LastUpdateTimestamp) <= 0;

            return Candle.Create(
                self.AssetPairId,
                self.PriceType,
                self.TimeInterval,
                self.Timestamp,
                selfIsOlder ? self.Open : newCandle.Open,
                selfIsOlder ? newCandle.Close : self.Close,
                Math.Max(self.High, newCandle.High),
                Math.Min(self.Low, newCandle.Low),
                self.TradingVolume + newCandle.TradingVolume,
                self.TradingOppositeVolume + newCandle.TradingOppositeVolume,
                newCandle.LastTradePrice,
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
