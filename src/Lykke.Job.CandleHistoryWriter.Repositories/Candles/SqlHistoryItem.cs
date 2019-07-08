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
        public double LastTradePrice { get; }
        public DateTime LastUpdateTimestamp { get; }

        private SqlCandleHistoryItem(
            long Id,
            string assetPairId,
            int priceType,
            double open,
            double close,
            double high,
            double low,
            int timeInterval,
            double tradingVolume,
            double tradingOppositeVolume,
            double lastTradePrice,
            DateTime timestamp,
            DateTime lastUpdateTimestamp)
        {
            AssetPairId = assetPairId;
            PriceType = (CandlePriceType)priceType;
            TimeInterval = (CandleTimeInterval)timeInterval;
            Timestamp = timestamp;
            Open = open;
            Close = close;
            High = high;
            Low = low;
            TradingVolume = tradingVolume;
            TradingOppositeVolume = tradingOppositeVolume;
            LastTradePrice = lastTradePrice;
            LastUpdateTimestamp = lastUpdateTimestamp;
        }

    }
}
