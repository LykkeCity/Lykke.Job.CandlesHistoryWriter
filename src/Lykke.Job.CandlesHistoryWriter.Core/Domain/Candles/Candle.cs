using System;
using Lykke.Job.CandlesProducer.Contract;

namespace Lykke.Job.CandlesHistoryWriter.Core.Domain.Candles
{
    public class Candle : ICandle
    {
        public string AssetPairId { get; }
        public CandlePriceType PriceType { get; }
        public CandleTimeInterval TimeInterval { get; }
        public DateTime Timestamp { get; }
        public double Open { get; }
        public double Close { get; }
        public double High { get; }
        public double Low { get; }
        public double TradingVolume { get; }
        public double TradingOppositeVolume { get; }
        public DateTime LastUpdateTimestamp { get; }

        private Candle(
            string assetPair, 
            CandlePriceType priceType, 
            CandleTimeInterval timeInterval,
            DateTime timestamp, 
            double open, 
            double close,
            double high, 
            double low,
            double tradingVolume,
            double tradingOppositeVolume,
            DateTime lastUpdateTimestamp)
        {
            AssetPairId = assetPair;
            PriceType = priceType;
            TimeInterval = timeInterval;
            Timestamp = timestamp;
            Open = open;
            Close = close;
            High = high;
            Low = low;
            TradingVolume = tradingVolume;
            TradingOppositeVolume = tradingOppositeVolume;
            LastUpdateTimestamp = lastUpdateTimestamp;
        }

        public static Candle Create(
            string assetPair,
            CandlePriceType priceType, 
            CandleTimeInterval timeInterval,
            DateTime timestamp, 
            double open, 
            double close, 
            double high, 
            double low, 
            double tradingVolume,
            double tradingOppositeVolume,
            DateTime lastUpdateTimestamp)
        {
            return new Candle(
                assetPair,
                priceType,
                timeInterval,
                timestamp.TruncateTo(timeInterval),
                open,
                close,
                high,
                low,
                tradingVolume,
                tradingOppositeVolume,
                lastUpdateTimestamp);
        }

        public Candle Update(
            double close, 
            double low, 
            double high, 
            double tradingVolume, 
            double tradingOppositeVolume,
            DateTime updateTimestamp)
        {
            if (updateTimestamp > LastUpdateTimestamp)
            {
                return new Candle(
                    assetPair: AssetPairId,
                    priceType: PriceType,
                    timeInterval: TimeInterval,
                    timestamp: Timestamp,
                    open: Open,
                    close: close,
                    high: high,
                    low: low,
                    tradingVolume: tradingVolume, 
                    tradingOppositeVolume: tradingOppositeVolume,
                    lastUpdateTimestamp: updateTimestamp);
            }

            return this;
        }

        public static Candle Copy(ICandle candle)
        {
            return new Candle
            (
                assetPair: candle.AssetPairId,
                priceType: candle.PriceType,
                timeInterval: candle.TimeInterval,
                timestamp: candle.Timestamp,
                open: candle.Open,
                close: candle.Close,
                high: candle.High,
                low: candle.Low,
                tradingVolume: candle.TradingVolume,
                tradingOppositeVolume: candle.TradingOppositeVolume,
                lastUpdateTimestamp: candle.LastUpdateTimestamp
            );
        }
    }
}
