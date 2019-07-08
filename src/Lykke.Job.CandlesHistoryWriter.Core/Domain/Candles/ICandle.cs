// Copyright (c) 2019 Lykke Corp.
// See the LICENSE file in the project root for more information.

using System;
using Lykke.Job.CandlesProducer.Contract;

namespace Lykke.Job.CandlesHistoryWriter.Core.Domain.Candles
{
    public interface ICandle
    {
        string AssetPairId { get; }
        CandlePriceType PriceType { get; }
        CandleTimeInterval TimeInterval { get; }
        DateTime Timestamp { get; }
        double Open { get; }
        double Close { get; }
        double High { get; }
        double Low { get; }
        double TradingVolume { get; }
        double TradingOppositeVolume { get; }
        double LastTradePrice { get; }
        DateTime LastUpdateTimestamp { get; }
    }
}
