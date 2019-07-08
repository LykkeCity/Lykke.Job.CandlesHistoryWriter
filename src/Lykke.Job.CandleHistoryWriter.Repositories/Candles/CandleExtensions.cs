// Copyright (c) 2019 Lykke Corp.
// See the LICENSE file in the project root for more information.

using Lykke.Job.CandlesHistoryWriter.Core.Domain.Candles;

namespace Lykke.Job.CandleHistoryWriter.Repositories.Candles
{
    internal static class CandleExtensions
    {
        public static CandleHistoryItem ToItem(this ICandle candle, int tick)
        {
            return new CandleHistoryItem
            (
                candle.Open,
                candle.Close,
                candle.High,
                candle.Low,
                tick,
                candle.TradingVolume,
                candle.TradingOppositeVolume,
                candle.LastTradePrice,
                candle.LastUpdateTimestamp
            );
        }
    }
}
