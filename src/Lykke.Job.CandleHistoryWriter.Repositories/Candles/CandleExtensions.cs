using Lykke.Job.CandlesHistoryWriter.Core.Domain.Candles;

namespace Lykke.Service.CandleHistory.Repositories.Candles
{
    internal static class CandleExtensions
    {
        public static CandleHistoryItem ToItem(this ICandle candle, int tick)
        {
            return new CandleHistoryItem
            (
                open: candle.Open,
                close: candle.Close,
                high: candle.High,
                low: candle.Low,
                tick: tick,
                tradingVolume: candle.TradingVolume,
                tradingOppositeVolume:  candle.TradingOppositeVolume,
                lastUpdateTimestamp: candle.LastUpdateTimestamp
            );
        }
    }
}
