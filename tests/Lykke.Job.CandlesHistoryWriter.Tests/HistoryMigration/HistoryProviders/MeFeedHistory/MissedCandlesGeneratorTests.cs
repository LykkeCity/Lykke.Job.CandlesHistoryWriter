using System;
using System.Linq;
using Lykke.Job.CandlesProducer.Contract;
using Lykke.Service.Assets.Client.Models;
using Lykke.Job.CandlesHistoryWriter.Services.HistoryMigration.HistoryProviders.MeFeedHistory;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Lykke.Job.CandlesHistoryWriter.Tests.HistoryMigration.HistoryProviders.MeFeedHistory
{
    [TestClass]
    public class RandomMissedCandlesGeneratorTests
    {
        [TestMethod]
        public void Test_that_not_NaN_prices_candles_generated_case1()
        {
            // Arrange
            var generator = new RandomMissedCandlesGenerator();

            // Act
            var candles = generator.GenerateCandles(
                    new AssetPairResponseModel
                    {
                        Id = "EURUSD",
                        Accuracy = 5
                    },
                    CandlePriceType.Ask,
                    new DateTime(2017, 08, 16, 15, 14, 49, DateTimeKind.Utc),
                    new DateTime(2017, 08, 16, 15, 14, 57, DateTimeKind.Utc),
                    0.1,
                    double.NaN,
                    0.1)
                .ToArray();

            // Assert
            Assert.AreEqual(7, candles.Length);
            foreach (var candle in candles)
            {
                Assert.IsFalse(double.IsNaN(candle.Open));
                Assert.IsFalse(double.IsNaN(candle.Close));
                Assert.IsFalse(double.IsNaN(candle.Low));
                Assert.IsFalse(double.IsNaN(candle.High));
            }
        }

        [TestMethod]
        public void Test_that_not_NaN_prices_candles_generated_case2()
        {
            // Arrange
            var generator = new RandomMissedCandlesGenerator();

            // Act
            var candles = generator.GenerateCandles(
                    new AssetPairResponseModel
                    {
                        Id = "EURUSD",
                        Accuracy = 5
                    },
                    CandlePriceType.Ask,
                    new DateTime(2017, 08, 16, 15, 14, 49, DateTimeKind.Utc),
                    new DateTime(2017, 08, 16, 15, 14, 57, DateTimeKind.Utc),
                    0,
                    1.17046,
                    0)
                .ToArray();

            // Assert
            Assert.AreEqual(7, candles.Length);
            foreach (var candle in candles)
            {
                Assert.IsFalse(double.IsNaN(candle.Open));
                Assert.IsFalse(double.IsNaN(candle.Close));
                Assert.IsFalse(double.IsNaN(candle.Low));
                Assert.IsFalse(double.IsNaN(candle.High));
            }
        }

        [TestMethod]
        public void Test_that_the_same_start_and_end_prices_with_zerospread_produces_different_prices()
        {
            // Arrange
            var generator = new RandomMissedCandlesGenerator();

            // Act
            var candles = generator.GenerateCandles(
                    new AssetPairResponseModel
                    {
                        Id = "EURUSD",
                        Accuracy = 5
                    },
                    CandlePriceType.Ask,
                    new DateTime(2017, 08, 16, 15, 14, 49, DateTimeKind.Utc),
                    new DateTime(2017, 08, 16, 15, 14, 51, DateTimeKind.Utc),
                    1.17046,
                    1.17046,
                    0)
                .ToArray();

            // Assert
            Assert.AreEqual(1, candles.Length);
            Assert.AreNotEqual(1.17046, candles[0].Open, 0.000001);
            Assert.AreNotEqual(1.17046, candles[0].Close, 0.000001);
            Assert.AreNotEqual(1.17046, candles[0].High, 0.000001);
            Assert.AreNotEqual(1.17046, candles[0].Low, 0.000001);
        }

        [TestMethod]
        public void Test_that_near_zero_prices_not_generates_negative_prices()
        {
            // Arrange
            var generator = new RandomMissedCandlesGenerator();

            // Act
            var candles = generator.GenerateCandles(
                    new AssetPairResponseModel
                    {
                        Id = "EURUSD",
                        Accuracy = 5
                    },
                    CandlePriceType.Ask,
                    new DateTime(2017, 08, 16, 15, 14, 49, DateTimeKind.Utc),
                    new DateTime(2017, 08, 16, 15, 15, 49, DateTimeKind.Utc),
                    1.17046,
                    0.0046,
                    50)
                .ToArray();

            // Assert
            Assert.AreEqual(59, candles.Length);
            foreach (var candle in candles)
            {
                Assert.IsTrue(candle.Open > 0, $"Open price {candle.Open} for candle {candle.Timestamp} is not pisitive");
                Assert.IsTrue(candle.Close > 0, $"Close price {candle.Close} for candle {candle.Timestamp} is not pisitive");
                Assert.IsTrue(candle.Low > 0, $"Low price {candle.Low} for candle {candle.Timestamp} is not pisitive");
                Assert.IsTrue(candle.High > 0, $"High price {candle.High} for candle {candle.Timestamp} is not pisitive");
            }
        }

        [TestMethod]
        public void Test_that_one_sec_candles_gap_generates_single_candle()
        {
            // Arrange
            var generator = new RandomMissedCandlesGenerator();

            // Act
            var candles = generator.GenerateCandles(
                    new AssetPairResponseModel
                    {
                        Id = "BTCEUR",
                        Accuracy = 5
                    },
                    CandlePriceType.Bid,
                    new DateTime(2016, 04, 28, 10, 57, 29, DateTimeKind.Utc),
                    new DateTime(2016, 04, 28, 10, 57, 31, DateTimeKind.Utc),
                    1,
                    2,
                    0.2)
                .ToArray();

            // Assert
            Assert.AreEqual(1, candles.Length);
            Assert.AreEqual(new DateTime(2016, 04, 28, 10, 57, 30, DateTimeKind.Utc), candles.First().Timestamp);
        }

        [TestMethod]
        public void Test_that_zero_candles_gap_generates_no_candles()
        {
            // Arrange
            var generator = new RandomMissedCandlesGenerator();

            // Act
            var candles = generator.GenerateCandles(
                    new AssetPairResponseModel
                    {
                        Id = "BTCEUR",
                        Accuracy = 5
                    },
                    CandlePriceType.Bid,
                    new DateTime(2016, 04, 28, 10, 57, 29, DateTimeKind.Utc),
                    new DateTime(2016, 04, 28, 10, 57, 30, DateTimeKind.Utc),
                    1,
                    2,
                    0.2)
                .ToArray();

            // Assert
            Assert.AreEqual(0, candles.Length);
        }

        [TestMethod]
        public void Test_that_generator_generates_all_candles()
        {
            // Arrange
            var generator = new RandomMissedCandlesGenerator();

            // Act
            var candles = generator.GenerateCandles(
                new AssetPairResponseModel
                {
                    Id = "BTCEUR",
                    Accuracy = 5
                },
                CandlePriceType.Bid,
                new DateTime(2017, 10, 25, 00, 00, 00, DateTimeKind.Utc).AddSeconds(-1),
                new DateTime(2017, 10, 26, 00, 00, 00, DateTimeKind.Utc),
                1.3212,
                1.1721,
                0.2);
            
            // Assert
            Assert.AreEqual(60 * 60 * 24, candles.Count());
        }
    }
}
