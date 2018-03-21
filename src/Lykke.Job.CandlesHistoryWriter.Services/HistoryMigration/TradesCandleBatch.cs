using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.Candles;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.HistoryMigration.HistoryProviders.TradesSQLHistory;
using Lykke.Job.CandlesProducer.Contract;

namespace Lykke.Job.CandlesHistoryWriter.Services.HistoryMigration
{
    public class TradesCandleBatch
    {
        public int CandlesCount { get; }

        public string AssetId { get; }
        public CandleTimeInterval TimeInterval { get; }

        public static CandlePriceType PriceType =>
            CandlePriceType.Trades;

        public DateTime MinTimeStamp { get; private set; }
        public DateTime MaxTimeStamp { get; private set; }

        public Dictionary<long, ICandle> Candles { get; }

        public TradesCandleBatch(string assetId, CandleTimeInterval interval, IEnumerable<TradeHistoryItem> trades)
        {
            AssetId = assetId;
            TimeInterval = interval;

            MinTimeStamp = DateTime.MaxValue;
            MaxTimeStamp = DateTime.MinValue;

            Candles = new Dictionary<long, ICandle>();

            CandlesCount = MakeFromTrades(trades);
        }

        public TradesCandleBatch(string assetId, CandleTimeInterval interval, TradesCandleBatch basis)
        {
            AssetId = assetId;
            TimeInterval = interval;

            MinTimeStamp = DateTime.MaxValue;
            MaxTimeStamp = DateTime.MinValue;

            Candles = new Dictionary<long, ICandle>();

            CandlesCount = DeriveFromSmallerIntervalAsync(basis);
        }

        private int MakeFromTrades(IEnumerable<TradeHistoryItem> trades)
        {
            var count = 0;
            foreach (var trade in trades)
            {
                var truncatedDate = trade.DateTime.TruncateTo(TimeInterval);
                var timestamp = truncatedDate.ToFileTimeUtc();

                var tradeCandle = Candle.Create(
                    AssetId,
                    PriceType,
                    TimeInterval,
                    truncatedDate,
                    (double)trade.Price,
                    (double)trade.Price,
                    (double)trade.Price,
                    (double)trade.Price,
                    (double)trade.Volume,
                    (double)trade.OppositeVolume,
                    (double)trade.Price,
                    trade.DateTime
                );

                if (!Candles.TryGetValue(timestamp, out var existingCandle))
                {
                    Candles.Add(timestamp, tradeCandle);
                    count++;
                    if (truncatedDate < MinTimeStamp)
                        MinTimeStamp = truncatedDate;
                    if (truncatedDate > MaxTimeStamp)
                        MaxTimeStamp = truncatedDate;
                }
                else
                    Candles[timestamp] = existingCandle.ExtendBy(tradeCandle);
            }

            return count;
        }

        private int DeriveFromSmallerIntervalAsync(TradesCandleBatch basis)
        {
            if ((int)(basis.TimeInterval) >= (int)TimeInterval)
                throw new InvalidOperationException($"Can't derive candles for time interval {TimeInterval.ToString()} from candles of {basis.TimeInterval.ToString()}.");

            if (basis.AssetId != AssetId)
                throw new InvalidOperationException($"Can't derive candles for asset pair ID {AssetId} from candles of {basis.AssetId}");

            var count = 0;

            foreach (var candle in basis.Candles)
            {
                var truncatedDate = candle.Value.Timestamp.TruncateTo(TimeInterval);
                var timestamp = truncatedDate.ToFileTimeUtc();

                if (!Candles.TryGetValue(timestamp, out var existingCandle))
                {
                    Candles.Add(timestamp, candle.Value.RebaseToInterval(TimeInterval));
                    count++;
                    if (truncatedDate < MinTimeStamp)
                        MinTimeStamp = truncatedDate;
                    if (truncatedDate > MaxTimeStamp)
                        MaxTimeStamp = truncatedDate;
                }
                else
                    Candles[timestamp] = existingCandle.ExtendBy(candle.Value.RebaseToInterval(TimeInterval));
            }

            return count;
        }
    }
}
