// Copyright (c) 2019 Lykke Corp.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using System.Text;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.Candles;
using Lykke.Job.CandlesProducer.Contract;

namespace Lykke.Job.CandleHistoryWriter.Repositories.Candles
{
    public class SqlCandleHistoryItem : ICandle
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
        public double LastTradePrice { get; set; }
        public DateTime LastUpdateTimestamp { get; set; }
    }
}
