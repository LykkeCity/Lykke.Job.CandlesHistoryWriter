// Copyright (c) 2019 Lykke Corp.
// See the LICENSE file in the project root for more information.

using System;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.Candles;
using Lykke.Job.CandlesProducer.Contract;
using Newtonsoft.Json;

namespace Lykke.Job.CandleHistoryWriter.Repositories.Candles
{
    public class CandleHistoryItem
    {
        [JsonProperty("O")]
        public double Open { get; private set; }

        [JsonProperty("C")]
        public double Close { get; private set; }

        [JsonProperty("H")]
        public double High { get; private set; }

        [JsonProperty("L")]
        public double Low { get; private set; }

        [JsonProperty("T")]
        public int Tick { get; }

        [JsonProperty("V")]
        public double TradingVolume { get; private set; }

        [JsonProperty("OV")]
        public  double TradingOppositeVolume { get; private set; }

        [JsonProperty("LTP")]	
        public double LastTradePrice { get; private set; }

        [JsonProperty("U")]
        public DateTime LastUpdateTimestamp { get; private set; }

        [JsonConstructor]
        public CandleHistoryItem(
            double open, 
            double close, 
            double high, 
            double low, 
            int tick, 
            double tradingVolume, 
            double tradingOppositeVolume,
            double lastTradePrice,
            DateTime lastUpdateTimestamp)
        {
            Open = open;
            Close = close;
            High = high;
            Low = low;
            Tick = tick;
            TradingVolume = tradingVolume;
            TradingOppositeVolume = tradingOppositeVolume;
            LastTradePrice = lastTradePrice;
            LastUpdateTimestamp = lastUpdateTimestamp;
        }

        public ICandle ToCandle(string assetPairId, CandlePriceType priceType, DateTime baseTime, CandleTimeInterval timeInterval)
        {
            var normalizedTick = Tick - GetIntervalTickOrigin(timeInterval);

            return Candle.Create
            (
                open: Open,
                close: Close,
                high: High,
                low: Low,
                assetPair: assetPairId,
                priceType: priceType,
                timeInterval: timeInterval,
                timestamp: baseTime.AddIntervalTicks(normalizedTick, timeInterval),
                tradingVolume: TradingVolume,
                tradingOppositeVolume: TradingOppositeVolume,
                lastTradePrice: LastTradePrice,
                lastUpdateTimestamp: LastUpdateTimestamp
            );
        }

        /// <summary>
        /// Merges candle change with the same asset pair, price type, time interval and timestamp
        /// </summary>
        /// <param name="candleState">Candle state</param>
        public void InplaceMergeWith(ICandle candleState)
        {
            if (LastUpdateTimestamp >= candleState.LastUpdateTimestamp)
            {
                return;
            }
            
            Close = candleState.Close;
            High = Math.Max(High, candleState.High);
            Low = Math.Min(Low, candleState.Low);
            TradingVolume = candleState.TradingVolume;
            TradingOppositeVolume = candleState.TradingOppositeVolume;
            LastUpdateTimestamp = candleState.LastUpdateTimestamp;
        }

        private static int GetIntervalTickOrigin(CandleTimeInterval interval)
        {
            switch (interval)
            {
                case CandleTimeInterval.Month:
                case CandleTimeInterval.Day:
                    return 1;
                case CandleTimeInterval.Week:
                case CandleTimeInterval.Hour12:
                case CandleTimeInterval.Hour6:
                case CandleTimeInterval.Hour4:
                case CandleTimeInterval.Hour:
                case CandleTimeInterval.Min30:
                case CandleTimeInterval.Min15:
                case CandleTimeInterval.Min5:
                case CandleTimeInterval.Minute:
                case CandleTimeInterval.Sec:
                    return 0;
                default:
                    throw new ArgumentOutOfRangeException(nameof(interval), interval, "Unexpected TimeInterval value.");
            }
        }
    }
}
