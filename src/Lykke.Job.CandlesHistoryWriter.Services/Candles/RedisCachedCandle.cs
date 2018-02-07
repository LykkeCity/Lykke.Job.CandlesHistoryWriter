using System;
using MessagePack;

namespace Lykke.Job.CandlesHistoryWriter.Services.Candles
{
    [MessagePackObject]
    public class RedisCachedCandle
    {
        [Key(1)]
        public double Open { get; set; }
        [Key(2)]
        public double Close { get; set; }
        [Key(3)]
        public double High { get; set; }
        [Key(4)]
        public double Low { get; set; }
        [Key(5)]
        public double TradingVolume { get; set; }
        [Key(6)]
        public double TradingOppositVolume { get; set; }
        [Key(7)]
        public double LastTradePrice { get; set; }
        [Key(8)]
        public DateTime LastUpdateTimestamp { get; set; }
    }
}
