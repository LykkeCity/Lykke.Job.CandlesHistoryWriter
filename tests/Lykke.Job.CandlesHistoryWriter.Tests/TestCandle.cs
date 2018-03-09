using System;
using Lykke.Job.CandlesProducer.Contract;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.Candles;

namespace Lykke.Job.CandlesHistoryWriter.Tests
{
    public class TestCandle : ICandle
    {
        public string AssetPairId { get; set; }
        public CandlePriceType PriceType { get; set; }
        public CandleTimeInterval TimeInterval { get; set; }
        public DateTime Timestamp { get; set; }
        public double Open { get; set; }
        public double Close { get; set; }
        public double High { get; set; }
        public double Low { get; set; }
        public double TradingVolume { get; set; }
        public double TradingOppositeVolume { get; set; }
        public DateTime LastUpdateTimestamp { get; set; }
    }
}
