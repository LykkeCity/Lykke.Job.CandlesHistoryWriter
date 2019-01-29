using System;
using System.Collections.Generic;
using System.Linq;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.Candles;
using Lykke.Job.CandlesHistoryWriter.Services.Candles;
using Lykke.Job.CandlesProducer.Contract;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Lykke.Job.CandlesHistoryWriter.Tests
{
    [TestClass]
    public class CandlesMergerTests
    {
        [TestMethod]
        public void Test_candles_merged_to_bigger_interval()
        {
            const string assetPair = "BTCUSD";
            var date = new DateTime(2019, 03, 04, 0, 1, 0);

            var candles = new List<Candle>
            {
                Candle.Create(assetPair, CandlePriceType.Ask, CandleTimeInterval.Minute, date, 4000, 4100, 4100, 4000,
                    0, 0, 0, date),
                Candle.Create(assetPair, CandlePriceType.Ask, CandleTimeInterval.Minute, date.AddMinutes(1), 4000, 4100,
                    4100, 4000, 0, 0, 0, date.AddMinutes(1)),
                Candle.Create(assetPair, CandlePriceType.Ask, CandleTimeInterval.Minute, date.AddMinutes(2), 3900, 4100,
                    4200, 3800, 0, 0, 0, date.AddMinutes(2)),
                Candle.Create(assetPair, CandlePriceType.Ask, CandleTimeInterval.Minute, date.AddMinutes(3), 4000, 4150,
                    4100, 4000, 0, 0, 0, date.AddMinutes(3)),
                Candle.Create(assetPair, CandlePriceType.Ask, CandleTimeInterval.Minute, date.AddMinutes(4), 3850, 4100,
                    4100, 4000, 0, 0, 0, date.AddMinutes(4)),
                Candle.Create(assetPair, CandlePriceType.Ask, CandleTimeInterval.Minute, date.AddMinutes(5), 4000, 4100,
                    4100, 4000, 0, 0, 0, date.AddMinutes(5)),
                Candle.Create(assetPair, CandlePriceType.Ask, CandleTimeInterval.Minute, date.AddMinutes(6), 4000, 4100,
                    4300, 3700, 0, 0, 0, date.AddMinutes(6)),
                Candle.Create(assetPair, CandlePriceType.Ask, CandleTimeInterval.Minute, date.AddMinutes(7), 4000, 4100,
                    4100, 4000, 0, 0, 0, date.AddMinutes(7)),
                Candle.Create(assetPair, CandlePriceType.Ask, CandleTimeInterval.Minute, date.AddMinutes(8), 4000, 4180,
                    4100, 4000, 0, 0, 0, date.AddMinutes(8))
            };

            var newCandles = CandlesMerger.MergeIntoBiggerIntervals(candles, CandleTimeInterval.Min5).ToArray();
            
            Assert.AreEqual(2, newCandles.Length);
            Assert.IsTrue(newCandles.All(x => x.TimeInterval == CandleTimeInterval.Min5));
            Assert.AreEqual(4000, newCandles[0].Open);
            Assert.AreEqual(4150, newCandles[0].Close);
            Assert.AreEqual(4200, newCandles[0].High);
            Assert.AreEqual(3800, newCandles[0].Low);
            Assert.AreEqual(3850, newCandles[1].Open);
            Assert.AreEqual(4180, newCandles[1].Close);
            Assert.AreEqual(4300, newCandles[1].High);
            Assert.AreEqual(3700, newCandles[1].Low);
        }
    }
}
