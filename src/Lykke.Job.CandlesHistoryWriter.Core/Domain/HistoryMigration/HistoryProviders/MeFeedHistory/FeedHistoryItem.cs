using System;
using Lykke.Job.CandlesProducer.Contract;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.Candles;

namespace Lykke.Job.CandlesHistoryWriter.Core.Domain.HistoryMigration.HistoryProviders.MeFeedHistory
{
    public class FeedHistoryItem
    {
        public double Open { get; set; }
        public double Close { get; set; }
        public double High { get; set; }
        public double Low { get; set; }
        public int Tick { get; set; }

        public ICandle ToCandle(string assetPairId, CandlePriceType priceType, DateTime baseTime)
        {
            var timeStamp = baseTime.AddSeconds(Tick);

            return Candle.Create(
                open: Open,
                close: Close,
                high: High,
                low: Low,
                assetPair: assetPairId,
                priceType: priceType,
                timeInterval: CandleTimeInterval.Sec,
                timestamp: timeStamp,
                tradingVolume: 0,
                tradingOppositeVolume: 0,
                lastUpdateTimestamp: timeStamp);
        }
    }
}
