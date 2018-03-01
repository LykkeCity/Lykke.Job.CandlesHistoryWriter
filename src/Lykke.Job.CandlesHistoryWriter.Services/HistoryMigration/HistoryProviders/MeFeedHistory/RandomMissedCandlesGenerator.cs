using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using Common;
using Lykke.Job.CandlesProducer.Contract;
using Lykke.Service.Assets.Client.Custom;
using Lykke.Job.CandlesHistoryWriter.Core;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.Candles;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.HistoryMigration.HistoryProviders.MeFeedHistory;

namespace Lykke.Job.CandlesHistoryWriter.Services.HistoryMigration.HistoryProviders.MeFeedHistory
{
    /// <summary>
    /// Generates missed candles for ask and bid sec candles history
    /// </summary>
    public class RandomMissedCandlesGenerator : IMissedCandlesGenerator
    {
        private readonly ConcurrentDictionary<string, Candle> _lastCandles;
        private readonly ConcurrentDictionary<string, decimal> _lastNonZeroPrices;
        private readonly Random _rnd;

        public RandomMissedCandlesGenerator()
        {
            _lastCandles = new ConcurrentDictionary<string, Candle>();
            _lastNonZeroPrices = new ConcurrentDictionary<string, decimal>();
            _rnd = new Random();
        }

        public IReadOnlyList<ICandle> FillGapUpTo(IAssetPair assetPair, IFeedHistory feedHistory)
        {
            var key = GetKey(feedHistory.AssetPair, feedHistory.PriceType);
            var candles = feedHistory.Candles
                .Select(item => item.ToCandle(feedHistory.AssetPair, feedHistory.PriceType, feedHistory.DateTime))
                .ToList();

            _lastCandles.TryGetValue(key, out var lastCandle);

            bool removeFirstCandle;

            // Use the last candle as the first candle, if any
            if (lastCandle != null)
            {
                removeFirstCandle = true;
                candles.Insert(0, lastCandle);
            }
            else
            {
                removeFirstCandle = false;
            }
            
            var result = GenerateMissedCandles(assetPair, candles);

            // Remember the last candle, if any
            if (result.Any())
            {
                _lastCandles[key] = Candle.Copy(result.Last());
            }

            if (removeFirstCandle)
            {
                result.RemoveAt(0);
            }

            return new ReadOnlyCollection<ICandle>(result);
        }

        public IReadOnlyList<ICandle> FillGapUpTo(IAssetPair assetPair, CandlePriceType priceType, DateTime dateTime, ICandle endCandle)
        {
            var key = GetKey(assetPair.Id, priceType);

            _lastCandles.TryGetValue(key, out var lastCandle);

            if (lastCandle == null)
            {
                return new List<ICandle>();
            }

            var lastCandleHeight = lastCandle.High - lastCandle.Low;
            var endCandleHeight = endCandle?.High - endCandle?.Low ?? lastCandleHeight;
            var spread = CalculateSpread(lastCandleHeight, endCandleHeight);

            var result = GenerateCandles(
                    assetPair,
                    priceType,
                    lastCandle.Timestamp,
                    dateTime,
                    lastCandle.Close,
                    endCandle?.Open ?? lastCandle.Open,
                    spread)
                .ToList();

            // Remember the last candle, if any
            if (result.Any())
            {
                _lastCandles[key] = Candle.Copy(result.Last());
            }

            return result;
        }

        public void RemoveAssetPair(string assetPair)
        {
            foreach (var priceType in Candles.Constants.StoredPriceTypes)
            {
                _lastCandles.TryRemove(GetKey(assetPair, priceType), out var _);
            }
        }

        public IEnumerable<ICandle> GenerateCandles(
            IAssetPair assetPair,
            CandlePriceType priceType,
            DateTime exclusiveStartDate,
            DateTime exclusiveEndDate,
            double exclusiveStartPrice,
            double exclusiveEndPrice,
            double spread)
        {
            return GenerateCandles(
                assetPair,
                priceType,
                exclusiveStartDate,
                exclusiveEndDate,
                ConvertToDecimal(exclusiveStartPrice),
                ConvertToDecimal(exclusiveEndPrice),
                ConvertToDecimal(spread));
        }

        private IList<ICandle> GenerateMissedCandles(IAssetPair assetPair, IReadOnlyList<ICandle> candles)
        {
            var result = new List<ICandle>();

            if (candles.Any())
            {
                var nextCandle = NormalizeCandlePrices(candles.First());

                for (var i = 0; i < candles.Count - 1; i++)
                {
                    var currentCandle = nextCandle;

                    UpdateLastNonZeroPrice(currentCandle);

                    nextCandle = NormalizeCandlePrices(candles[i + 1]);

                    var firstDate = currentCandle.Timestamp;
                    var lastDate = nextCandle.Timestamp;

                    result.Add(currentCandle);

                    var currentCandleHeight = currentCandle.High - currentCandle.Low;
                    var nextCandleHeight = nextCandle.High - nextCandle.Low;
                    var spread = CalculateSpread(currentCandleHeight, nextCandleHeight);

                    var generagedCandles = GenerateCandles(
                        assetPair,
                        currentCandle.PriceType,
                        firstDate,
                        lastDate,
                        currentCandle.Close,
                        nextCandle.Open,
                        spread);

                    result.AddRange(generagedCandles);
                }

                UpdateLastNonZeroPrice(nextCandle);

                result.Add(nextCandle);
            }

            return result;
        }

        private static double CalculateSpread(double candle1Height, double candle2Height)
        {
            return (candle1Height + candle2Height) * 0.5 * 50;
        }

        private IEnumerable<ICandle> GenerateCandles(
            IAssetPair assetPair, 
            CandlePriceType priceType, 
            DateTime exclusiveStartDate, 
            DateTime exclusiveEndDate, 
            decimal exclusiveStartPrice,
            decimal exclusiveEndPrice,
            decimal spread)
        {
            var start = exclusiveStartDate.AddSeconds(1);
            var end = exclusiveEndDate.AddSeconds(-1);

            if (exclusiveEndDate - exclusiveStartDate <= TimeSpan.FromSeconds(1))
            {
                yield break;
            }

            var duration = (decimal)(exclusiveEndDate - exclusiveStartDate).TotalSeconds;
            var prevClose = exclusiveStartPrice;
            var trendSign = exclusiveStartPrice < exclusiveEndPrice ? 1 : -1;
            // Absolute start to end price change in % of start price
            var totalPriceChange = exclusiveStartPrice != 0m
                ? Math.Abs((exclusiveEndPrice - exclusiveStartPrice) / exclusiveStartPrice)
                : Math.Abs(exclusiveEndPrice - exclusiveStartPrice);
            var stepPriceChange = totalPriceChange / duration;
            var effectiveSpread = spread != 0
                ? Math.Abs(spread)
                : totalPriceChange * 0.2m;
            // Start in opposite dirrection
            var currentTrendSign = -trendSign;

            if (effectiveSpread == 0)
            {
                if (exclusiveStartPrice != 0)
                {
                    effectiveSpread = exclusiveStartPrice * 0.2m;
                }
                else if (exclusiveEndPrice != 0)
                {
                    effectiveSpread = exclusiveEndPrice * 0.2m;
                }
                else
                {
                    effectiveSpread = (decimal)Math.Pow(10, -assetPair.Accuracy / 4d);
                }
            }

            if (stepPriceChange == 0)
            {
                stepPriceChange = effectiveSpread / duration;
            }

            var backupMid = exclusiveStartPrice;

            if (backupMid == 0)
            {
                backupMid = exclusiveEndPrice != 0 ? exclusiveEndPrice : effectiveSpread;
            }

            //File.WriteAllLines(@"C:\temp\candles.csv", new[]
            //{
            //    "timestamp,t,mid,min,max,open,close,low,high,closePriceMaxDeviation,rangeMinMaxDeviationFactor,height"
            //});

            for (var timestamp = start; timestamp <= end; timestamp = timestamp.AddSeconds(1))
            {
                // Interpolation parameter (0..1)
                var t = (decimal)(timestamp - exclusiveStartDate).TotalSeconds / duration;

                // Lineary interpolated price for current candle
                var mid = MathEx.Lerp(exclusiveStartPrice, exclusiveEndPrice, t);

                if (mid <= 0)
                {
                    mid = backupMid;
                }

                var halfSpread = effectiveSpread * 0.5m;

                // Next candle opens at prev candle close and from 5% up to 50% of stepPriceChange, 
                // direction is dependent of trend sign
                var open = prevClose + stepPriceChange * _rnd.NextDecimal(0.05m, 0.5m) * currentTrendSign;

                if (open <= 0)
                {
                    open = mid;
                }

                // Lets candles goes from 10% near the generated range boundaries and 
                // up to 100% of the spread in the middle of the generated range, 
                // and only inside the spread at the range boundaries
                var rangeMinMaxDeviationFactor = MathEx.Lerp(0.1m, 1m, 2m * (0.5m - Math.Abs(0.5m - t)));
                var min = mid - halfSpread * rangeMinMaxDeviationFactor;
                var max = mid + halfSpread * rangeMinMaxDeviationFactor;

                if (min <= 0)
                {
                    min = mid;
                }

                var maxClosePriceDeviation = CalculateMaxClosePriceDeviation(currentTrendSign, open, mid, min, max);

                // Candle can be closed from the 10% and up to closePriceMaxDeviation% of price change from the open price
                // But only inside min/max and if it touched min/max, the sign will be changed, and close price will be regenerated

                decimal GenerateClose(decimal sign)
                {
                    return open + stepPriceChange * _rnd.NextDecimal(0.05m, maxClosePriceDeviation) * sign;
                }

                var close = GenerateClose(currentTrendSign);

                if (close >= max && currentTrendSign > 0 || 
                    close <= min && currentTrendSign < 0)
                {
                    currentTrendSign = -currentTrendSign;

                    close = GenerateClose(currentTrendSign);
                }

                if (close <= 0)
                {
                    close = mid;
                }

                // Max low/high deviation from open/close is 20% - 50% of candle height, depending of current trend sign
                var height = Math.Abs(open - close);
                var high = Math.Max(open, close) + _rnd.NextDecimal(0m, currentTrendSign > 0 ? 0.5m : 0.2m) * height;
                var low = Math.Min(open, close) - _rnd.NextDecimal(0m, currentTrendSign < 0 ? 0.5m : 0.2m) * height;

                if (low <= 0)
                {
                    low = Math.Min(open, close);
                }

                if (high <= 0)
                {
                    high = Math.Max(open, close);
                }

                //File.AppendAllLines(@"C:\temp\candles.csv", new []
                //{
                //    $"{timestamp},{t},{mid},{min},{max},{open},{close},{low},{high},{maxClosePriceDeviation},{rangeMinMaxDeviationFactor},{height}"
                //});

                var newCandle = Candle.Create(
                    assetPair.Id,
                    priceType,
                    CandleTimeInterval.Sec,
                    timestamp,
                    (double) Math.Round(open, assetPair.Accuracy),
                    (double) Math.Round(close, assetPair.Accuracy),
                    (double) Math.Round(high, assetPair.Accuracy),
                    (double) Math.Round(low, assetPair.Accuracy),
                    0,
                    0,
                    0,
                    timestamp);

                if (open == 0 || close == 0 || high == 0 || low == 0)
                {
                    var context = new
                    {
                        AssetPair = new
                        {
                            assetPair.Id,
                            assetPair.Accuracy
                        },
                        exclusiveStartDate,
                        exclusiveEndDate,
                        start,
                        end,
                        exclusiveStartPrice,
                        exclusiveEndPrice,
                        duration,
                        spread,
                        effectiveSpread,
                        prevClose,
                        trendSign,
                        totalPriceChange,
                        timestamp,
                        t,
                        mid,
                        halfSpread,
                        currentTrendSign,
                        rangeMinMaxDeviationFactor,
                        min,
                        max,
                        height
                    };

                    throw new InvalidOperationException($"Generated candle {newCandle.ToJson()} has zero prices. Context: {context.ToJson()}");
                }

                prevClose = close;

                yield return newCandle;
            }
        }

        private static decimal CalculateMaxClosePriceDeviation(int currentTrendSign, decimal open, decimal mid, decimal min, decimal max)
        {
            // Close price max deviation is dependent of how close candle to the spread border is, and is the 800%
            // in the middle of the spread, 50% in at the border of the spread

            if (currentTrendSign > 0)
            {
                var factor = max - mid == 0
                    ? 0.5m
                    : MathEx.Clamp((max - open) / (max - mid), 0, 1);

                return MathEx.Lerp(0.5m, 8m, factor);
            }
            else
            {
                var factor = mid - min == 0
                    ? 0.5m
                    : MathEx.Clamp((open - min) / (mid - min), 0, 1);

                return MathEx.Lerp(0.5m, 8m, factor);
            }
        }

        private ICandle NormalizeCandlePrices(ICandle candle)
        {
            var lastNonZeroPrice = GetLastNonZeroPrice(candle.AssetPairId, candle.PriceType);
            var open = ConvertToDecimal(candle.Open);
            var close = ConvertToDecimal(candle.Close);
            var high = ConvertToDecimal(candle.High);
            var low = ConvertToDecimal(candle.Low);

            if (open == 0 || close == 0 || high == 0 || low == 0)
            {
                open = open == 0 ? lastNonZeroPrice : open;
                close = close == 0 ? lastNonZeroPrice : close;
                high = high == 0 ? lastNonZeroPrice : high;
                low = low == 0 ? lastNonZeroPrice : low;

                return Candle.Create(candle.AssetPairId, candle.PriceType, candle.TimeInterval, candle.Timestamp,
                    (double) open,
                    (double) close,
                    (double) high,
                    (double) low,
                    0,
                    0,
                    0,
                    candle.Timestamp);
            }

            return candle;
        }

        private void UpdateLastNonZeroPrice(ICandle candle)
        {
            var key = GetKey(candle.AssetPairId, candle.PriceType);

            var price = ConvertToDecimal(candle.Close);

            if (price == 0m)
            {
                price = ConvertToDecimal(candle.High);

                if (price == 0m)
                {
                    price = ConvertToDecimal(candle.Low);

                    if (price == 0m)
                    {
                        price = ConvertToDecimal(candle.Open);
                    }
                }
            }

            if (price != 0m)
            {
                _lastNonZeroPrices[key] = price;
            }
        }

        private decimal GetLastNonZeroPrice(string assetPair, CandlePriceType priceType)
        {
            var key = GetKey(assetPair, priceType);

            _lastNonZeroPrices.TryGetValue(key, out var price);

            return price;
        }

        private static decimal ConvertToDecimal(double d)
        {
            if (double.IsNaN(d))
            {
                return 0;
            }
            if (double.IsInfinity(d))
            {
                return 0;
            }

            return Convert.ToDecimal(d);
        }

        private static string GetKey(string assetPair, CandlePriceType priceType)
        {
            return $"{assetPair}-{priceType}";
        }
    }
}
