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
        public int CandlesCount { get; }

        public string AssetId { get; }
        private string _assetToken;
        private string _reverseAssetToken;

        public CandleTimeInterval TimeInterval { get; }

        public static CandlePriceType PriceType =>
            CandlePriceType.Trades;

        public DateTime MinTimeStamp { get; private set; }
        public DateTime MaxTimeStamp { get; private set; }

        public Dictionary<long, ICandle> Candles { get; }

        public TradesCandleBatch(string assetId, string assetToken, string reverseAssetToken,
            CandleTimeInterval interval, IEnumerable<TradeHistoryItem> trades)
        {
            AssetId = assetId;
            _assetToken = assetToken;
            _reverseAssetToken = reverseAssetToken;
            TimeInterval = interval;

            MinTimeStamp = DateTime.MaxValue;
            MaxTimeStamp = DateTime.MinValue;

            Candles = new Dictionary<long, ICandle>();

            CandlesCount = MakeFromTrades(trades);
        }

        public TradesCandleBatch(string assetId, CandleTimeInterval interval, TradesCandleBatch basis)
        {
            AssetId = assetId;
            // Here we do not set up asset tokens for they are not needed.
            TimeInterval = interval;

            MinTimeStamp = DateTime.MaxValue;
            MaxTimeStamp = DateTime.MinValue;

            Candles = new Dictionary<long, ICandle>();

            CandlesCount = DeriveFromSmallerInterval(basis);
        }

        private int MakeFromTrades(IEnumerable<TradeHistoryItem> trades)
        {
            // While making a new candle based on trades, we consider the following rules to be good enough:
            // 1. For each trade should be checked, if opposite order is limit order (is it pressented in the batch?). 
            //    If so, then buy order trades should be skipped. This is needed to avoid volume duplication when 
            //    there are two limit orders in the trade.
            // 2. Base and quoting volumes should be calculated according to direction of the trade asset pair.
            //    If asset pair is direct (trade asset is equal to base asset of the asset pair), then base 
            //    volume = trade volume, quoting voule = trade opposite volume. Otherwise, base volume = trade opposite 
            //    volume, quoting voule = trade volume.

            var count = 0;
            var similarTrades = new List<TradeHistoryItem>();
            foreach (var trade in trades)
            {
                // While iterating the whole trades batch, we form a temporary list of the similar trades (which have
                // the same DateTime and Price values). Such a list contains all the straight and reverse, buy and 
                // sell trades which correspond to the single deal. Next, we analyze the temporary list instead of the
                // whole trades list. Imagine: the whole list may contain 10 000 or more trades while the stricted
                // list contains not more than 10-20 trades. For we need to find the reverse asset pair trades for
                // the straight ones, it will thus increase performance of such a search, as we do not iterate a large
                // sequence multiple times.

                if (!similarTrades.Any() ||
                    trade.DateTime == similarTrades.Last().DateTime &&
                    trade.Price == similarTrades.Last().Price)
                    similarTrades.Add(trade);
                else
                {
                    foreach (var item in similarTrades)
                    {
                        // If the trade is straight or reverse.
                        var isStraight = item.AssetToken == _assetToken;

                        if (isStraight &&
                            item.Direction == TradeDirection.Buy &&
                            similarTrades.Any(t => t.OrderId == item.OppositeOrderId))
                            continue;

                        var truncatedDate = item.DateTime.TruncateTo(TimeInterval);
                        var timestamp = truncatedDate.ToFileTimeUtc();

                        var tradeCandle = Candle.Create(
                            AssetId,
                            PriceType,
                            TimeInterval,
                            truncatedDate,
                            (double)item.Price,
                            (double)item.Price,
                            (double)item.Price,
                            (double)item.Price,
                            isStraight ? (double)item.Volume : (double)item.OppositeVolume,
                            isStraight ? (double)item.OppositeVolume : (double)item.Volume,
                            0, // Last Trade Price is enforced to be = 0
                            item.DateTime
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

                    // Let's prepare for the next stricted trades batch filling.
                    similarTrades.Clear();
                    similarTrades.Add(trade);
                }
            }

            return count;
        }

        private int DeriveFromSmallerInterval(TradesCandleBatch basis)
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
