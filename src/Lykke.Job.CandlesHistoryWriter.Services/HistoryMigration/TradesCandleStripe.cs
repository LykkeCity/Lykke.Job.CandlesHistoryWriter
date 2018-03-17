using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.Candles;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.HistoryMigration.HistoryProviders.TradesSQLHistory;
using Lykke.Job.CandlesProducer.Contract;

namespace Lykke.Job.CandlesHistoryWriter.Services.HistoryMigration
{
    public class TradesCandleStripe
    {
        public string AssetId { get; }
        public CandleTimeInterval TimeInterval { get; }

        public static CandlePriceType PriceType =>
            CandlePriceType.Trades;

        public Dictionary<long, ICandle> Candles { get; }

        public TradesCandleStripe(string assetId, CandleTimeInterval interval)
        {
            AssetId = assetId;
            TimeInterval = interval;

            Candles = new Dictionary<long, ICandle>();
        }

        public async Task MakeFromTrades(IEnumerable<TradeHistoryItem> trades)
        {
            await Task.Run(() =>
            {
                _makeFromTrades(trades);
            });
        }

        public async Task DeriveFromSmallerIntervalAsync(TradesCandleStripe basis)
        {
            await Task.Run(() =>
            {
                _deriveFromSmallerIntervalAsync(basis);
            });
        }

        private void _makeFromTrades(IEnumerable<TradeHistoryItem> trades)
        {
            if (Candles.Any())
                Candles.Clear();

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
                    Candles.Add(timestamp, tradeCandle);
                else
                    Candles[timestamp] = existingCandle.ExtendBy(tradeCandle);
            }
        }

        private void _deriveFromSmallerIntervalAsync(TradesCandleStripe basis)
        {
            if ((int)(basis.TimeInterval) >= (int)TimeInterval)
                throw new InvalidOperationException($"Can't derive candles for time interval {TimeInterval.ToString()} from candles of {basis.TimeInterval.ToString()}.");

            if (basis.AssetId != AssetId)
                throw new InvalidOperationException($"Can't derive candles for asset pair ID {AssetId} from candles of {basis.AssetId}");

            if (Candles.Any())
                Candles.Clear();

            foreach (var candle in basis.Candles)
            {
                var truncatedDate = candle.Value.Timestamp.TruncateTo(TimeInterval);
                var timestamp = truncatedDate.ToFileTimeUtc();

                if (!Candles.TryGetValue(timestamp, out var existingCandle))
                    Candles.Add(timestamp, candle.Value.RebaseToInterval(TimeInterval));
                else
                    Candles[timestamp] = existingCandle.ExtendBy(candle.Value.RebaseToInterval(TimeInterval));
            }
        }
    }
}
