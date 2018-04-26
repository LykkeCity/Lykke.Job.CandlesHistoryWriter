using System;
using System.Collections.Generic;
using System.Linq;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.Candles;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.HistoryMigration.HistoryProviders.TradesSQLHistory;
using Lykke.Job.CandlesProducer.Contract;

namespace Lykke.Job.CandlesHistoryWriter.Services.HistoryMigration
{
    public class TradesCandleBatch
    {
        public string AssetId { get; }
        
        public CandleTimeInterval TimeInterval { get; }

        public static CandlePriceType PriceType =>
            CandlePriceType.Trades;

        public DateTime MinTimeStamp { get; private set; }
        public DateTime MaxTimeStamp { get; private set; }

        public IDictionary<DateTime, ICandle> Candles { get; }

        private readonly string _assetToken;
        // TODO: Remove unused field
        private readonly string _reverseAssetToken;

        public TradesCandleBatch(string assetId, string assetToken, string reverseAssetToken,
            CandleTimeInterval interval, IReadOnlyCollection<TradeHistoryItem> trades)
        {
            AssetId = assetId;
            _assetToken = assetToken;
            _reverseAssetToken = reverseAssetToken;
            TimeInterval = interval;

            MinTimeStamp = DateTime.MaxValue;
            MaxTimeStamp = DateTime.MinValue;

            Candles = MakeFromTrades(trades);
        }

        public TradesCandleBatch(string assetId, CandleTimeInterval interval, TradesCandleBatch basis)
        {
            AssetId = assetId;
            // Here we do not set up asset tokens for they are not needed.
            TimeInterval = interval;

            MinTimeStamp = DateTime.MaxValue;
            MaxTimeStamp = DateTime.MinValue;

            Candles = DeriveFromSmallerInterval(basis);
        }

        private IDictionary<DateTime, ICandle> MakeFromTrades(IReadOnlyCollection<TradeHistoryItem> trades)
        {
            // While making a new candle based on trades, we consider the following rules to be good enough:
            // 1. For each trade should be checked, if opposite order is limit order (is it pressented in the batch?). 
            //    If so, then buy order trades should be skipped. This is needed to avoid volume duplication when 
            //    there are two limit orders in the trade.
            // 2. Base and quoting volumes should be calculated according to direction of the trade asset pair.
            //    If asset pair is direct (trade asset is equal to base asset of the asset pair), then base 
            //    volume = trade volume, quoting voule = trade opposite volume. Otherwise, base volume = trade opposite 
            //    volume, quoting voule = trade volume.

            var limitOrderIds = trades
                .Select(t => t.OrderId)
                .ToHashSet();
            var candles = new Dictionary<DateTime, ICandle>();

            foreach (var trade in trades)
            {
                // While iterating the whole trades batch, we form a temporary list of the similar trades (which have
                // the same DateTime and Price values). Such a list contains all the straight and reverse, buy and 
                // sell trades which correspond to the single deal. Next, we analyze the temporary list instead of the
                // whole trades list. Imagine: the whole list may contain 10 000 or more trades while the stricted
                // list contains not more than 10-20 trades. For we need to find the reverse asset pair trades for
                // the straight ones, it will thus increase performance of such a search, as we do not iterate a large
                // sequence multiple times.

                // If the trade is straight or reverse.
                var isStraight = trade.AssetToken == _assetToken;
                var hasOppositeLimitOrder = limitOrderIds.Contains(trade.OppositeOrderId);

                if (isStraight && hasOppositeLimitOrder && trade.Direction == TradeDirection.Buy)
                {
                    continue;
                }

                var truncatedDate = trade.DateTime.TruncateTo(TimeInterval);

                var tradeCandle = Candle.Create(
                    AssetId,
                    PriceType,
                    TimeInterval,
                    truncatedDate,
                    (double) trade.Price,
                    (double) trade.Price,
                    (double) trade.Price,
                    (double) trade.Price,
                    isStraight ? (double) trade.Volume : (double) trade.OppositeVolume,
                    isStraight ? (double) trade.OppositeVolume : (double) trade.Volume,
                    0, // Last Trade Price is enforced to be = 0
                    trade.DateTime
                );

                if (!candles.TryGetValue(truncatedDate, out var existingCandle))
                {
                    candles.Add(truncatedDate, tradeCandle);

                    if (truncatedDate < MinTimeStamp)
                        MinTimeStamp = truncatedDate;
                    if (truncatedDate > MaxTimeStamp)
                        MaxTimeStamp = truncatedDate;
                }
                else
                {
                    candles[truncatedDate] = existingCandle.ExtendBy(tradeCandle);
                }
            }

            return candles;
        }

        private IDictionary<DateTime, ICandle> DeriveFromSmallerInterval(TradesCandleBatch basis)
        {
            if ((int)basis.TimeInterval >= (int)TimeInterval)
                throw new InvalidOperationException($"Can't derive candles for time interval {TimeInterval.ToString()} from candles of {basis.TimeInterval.ToString()}.");

            if (basis.AssetId != AssetId)
                throw new InvalidOperationException($"Can't derive candles for asset pair ID {AssetId} from candles of {basis.AssetId}");

            var candles = new Dictionary<DateTime, ICandle>();

            foreach (var candle in basis.Candles)
            {
                var truncatedDate = candle.Value.Timestamp.TruncateTo(TimeInterval);

                if (!candles.TryGetValue(truncatedDate, out var existingCandle))
                {
                    candles.Add(truncatedDate, candle.Value.RebaseToInterval(TimeInterval));

                    if (truncatedDate < MinTimeStamp)
                        MinTimeStamp = truncatedDate;
                    if (truncatedDate > MaxTimeStamp)
                        MaxTimeStamp = truncatedDate;
                }
                else
                {
                    candles[truncatedDate] = existingCandle.ExtendBy(candle.Value.RebaseToInterval(TimeInterval));
                }
            }

            return candles;
        }
    }
}
