using System;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.Candles;
using Lykke.Job.CandlesProducer.Contract;
using MessagePack;

namespace Lykke.Job.CandleHistoryWriter.Repositories.Snapshots
{
    [MessagePackObject]
    public class SnapshotCandleEntity : ICandle
    {
        [Key(0)]
        public string AssetPairId { get; set; }

        [Key(1)]
        public CandlePriceType PriceType { get; set; }

        [Key(2)]
        public CandleTimeInterval TimeInterval { get; set; }

        [Key(3)]
        public DateTime Timestamp { get; set; }

        [Key(4)]
        public decimal Open { get; set; }

        [Key(5)]
        public decimal Close { get; set; }

        [Key(6)]
        public decimal High { get; set; }

        [Key(7)]
        public decimal Low { get; set; }

        [Key(8)]
        public decimal TradingVolume { get; set; }

        [Key(9)]
        public DateTime LastUpdateTimestamp { get; set; }

        [Key(10)]
        public decimal TradingOppositeVolume { get; set; }
        
        double ICandle.Open => (double) Open;

        double ICandle.Close => (double) Close;

        double ICandle.High => (double) High;

        double ICandle.Low => (double) Low;

        double ICandle.TradingVolume => (double) TradingVolume;

        double ICandle.TradingOppositeVolume => (double) TradingOppositeVolume;

        public static SnapshotCandleEntity Copy(ICandle candle)
        {
            return new SnapshotCandleEntity
            {
                AssetPairId = candle.AssetPairId,
                PriceType = candle.PriceType,
                TimeInterval = candle.TimeInterval,
                Timestamp = candle.Timestamp,
                Open = ConvertDouble(candle.Open),
                Close = ConvertDouble(candle.Close),
                Low = ConvertDouble(candle.Low),
                High = ConvertDouble(candle.High),
                TradingVolume = ConvertDouble(candle.TradingVolume),
                TradingOppositeVolume = ConvertDouble(candle.TradingOppositeVolume),
                LastUpdateTimestamp = candle.LastUpdateTimestamp
            };
        }

        private static decimal ConvertDouble(double d)
        {
            try
            {
                return Convert.ToDecimal(d);
            }
            catch (OverflowException)
            {
                return d > 0 ? decimal.MaxValue : decimal.MinValue;
            }
        }
    }
}
