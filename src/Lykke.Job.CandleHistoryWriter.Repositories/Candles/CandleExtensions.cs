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
                candle.LastUpdateTimestamp
            );
        }
    }
}
