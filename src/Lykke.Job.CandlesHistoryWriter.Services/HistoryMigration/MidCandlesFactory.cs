using System;
using Common;
using Lykke.Job.CandlesProducer.Contract;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.Candles;

namespace Lykke.Job.CandlesHistoryWriter.Services.HistoryMigration
{
    public static class MidCandlesFactory
    {
        /// <summary>
        /// Creates mid candle of two candles (ask and bid)
        /// </summary>
        /// <param name="askCandle">first candle</param>
        /// <param name="bidCandle">second candle</param>
        public static ICandle Create(ICandle askCandle, ICandle bidCandle)
        {
            if (askCandle == null || bidCandle == null)
            {
                return askCandle ?? bidCandle;
            }

            if (askCandle.AssetPairId != bidCandle.AssetPairId)
            {
                throw new InvalidOperationException($"Can't create mid candle of different asset pairs. candle1={askCandle.ToJson()}, candle2={bidCandle.ToJson()}");
            }

            if (askCandle.PriceType != CandlePriceType.Ask)
            {
                throw new InvalidOperationException($"Ask candle should has according price type. candle={askCandle.ToJson()}");
            }

            if (bidCandle.PriceType != CandlePriceType.Bid)
            {
                throw new InvalidOperationException($"Bid candle should has according price type. candle={bidCandle.ToJson()}");
            }

            if (askCandle.TimeInterval != bidCandle.TimeInterval)
            {
                throw new InvalidOperationException($"Can't create mid candle of different time intervals. candle1={askCandle.ToJson()}, candle2={bidCandle.ToJson()}");
            }

            if (askCandle.Timestamp != bidCandle.Timestamp)
            {
                throw new InvalidOperationException($"Can't create mid candle from candles with different timestamps. candle1={askCandle.ToJson()}, candle2={bidCandle.ToJson()}");
            }

            return Candle.Create(
                open: (askCandle.Open + bidCandle.Open) / 2,
                close: (askCandle.Close + bidCandle.Close) / 2,
                high: (askCandle.High + bidCandle.High) / 2,
                low: (askCandle.Low + bidCandle.Low) / 2,
                assetPair: askCandle.AssetPairId,
                priceType: CandlePriceType.Mid,
                timeInterval: askCandle.TimeInterval,
                timestamp: askCandle.Timestamp,
                tradingVolume: askCandle.LastUpdateTimestamp > bidCandle.LastUpdateTimestamp
                    ? askCandle.TradingVolume
                    : bidCandle.TradingVolume,
                tradingOppositeVolume: askCandle.LastUpdateTimestamp > bidCandle.LastUpdateTimestamp
                    ? askCandle.TradingOppositeVolume
                    : bidCandle.TradingOppositeVolume,
                lastUpdateTimestamp: askCandle.LastUpdateTimestamp > bidCandle.LastUpdateTimestamp
                    ? askCandle.LastUpdateTimestamp
                    : bidCandle.LastUpdateTimestamp);
        }
    }
}
