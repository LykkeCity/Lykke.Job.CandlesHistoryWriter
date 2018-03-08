using System;
using System.Linq;
using System.Threading.Tasks;
using Lykke.Job.CandlesProducer.Contract;
using Lykke.Job.CandlesHistoryWriter.Core.Services.Candles;
using Lykke.Job.CandlesHistoryWriter.Services.Candles;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using StackExchange.Redis;

namespace Lykke.Job.CandlesHistoryWriter.Tests
{

    // TODO: the current implementation of some unit tests is not capable for redis cache service testing
    // for it does not support read IDatabase-like stub. These test methods are temporarely marked with [Ignore].

    [TestClass]
    public class CachedCandlesCacheServiceTests
    {
        private ICandlesCacheService _service;
        private IDatabase _db;

        [TestInitialize]
        public void InitializeTest()
        {
            _db = (new Mock<IDatabase>()).Object;
            _service = new RedisCandlesCacheService(_db, Core.Domain.Candles.MarketType.Mt);
        }


        #region History initialization

        [Ignore]
        [TestMethod]
        public async Task History_initializes_and_then_obtains()
        {
            // Arrange
            var history = new[]
            {
                new TestCandle
                {
                    AssetPairId = "EURUSD",
                    PriceType = CandlePriceType.Ask,
                    TimeInterval = CandleTimeInterval.Day,
                    Open = 1.5,
                    Close = 1.6,
                    Low = 1.3,
                    High = 1.8,
                    Timestamp = new DateTime(2017, 06, 10, 0, 0, 0, DateTimeKind.Utc)
                },
                new TestCandle
                {
                    AssetPairId = "EURUSD",
                    PriceType = CandlePriceType.Ask,
                    TimeInterval = CandleTimeInterval.Day,
                    Open = 1.6,
                    Close = 1.7,
                    Low = 1.4,
                    High = 1.9,
                    Timestamp = new DateTime(2017, 06, 11, 0, 0, 0, DateTimeKind.Utc)
                },
                new TestCandle
                {
                    AssetPairId = "EURUSD",
                    PriceType = CandlePriceType.Ask,
                    TimeInterval = CandleTimeInterval.Day,
                    Open = 1.6,
                    Close = 1.2,
                    Low = 1.3,
                    High = 1.5,
                    Timestamp = new DateTime(2017, 06, 12, 0, 0, 0, DateTimeKind.Utc)
                },
                new TestCandle
                {
                    AssetPairId = "EURUSD",
                    PriceType = CandlePriceType.Ask,
                    TimeInterval = CandleTimeInterval.Day,
                    Open = 1.3,
                    Close = 1.6,
                    Low = 1.3,
                    High = 1.8,
                    Timestamp = new DateTime(2017, 06, 13, 0, 0, 0, DateTimeKind.Utc)
                },
                new TestCandle
                {
                    AssetPairId = "EURUSD",
                    PriceType = CandlePriceType.Ask,
                    TimeInterval = CandleTimeInterval.Day,
                    Open = 1.6,
                    Close = 1.6,
                    Low = 1.4,
                    High = 1.7,
                    Timestamp = new DateTime(2017, 06, 14, 0, 0, 0, DateTimeKind.Utc)
                },
            };

            // Act
            await _service.InitializeAsync("EURUSD", CandlePriceType.Ask, CandleTimeInterval.Day, history);

            var obtainedHistory = (await _service.GetCandlesAsync("EURUSD", CandlePriceType.Ask, CandleTimeInterval.Day,
                    new DateTime(2017, 06, 01, 0, 0, 0, DateTimeKind.Utc),
                    new DateTime(2017, 07, 01, 0, 0, 0, DateTimeKind.Utc)))
                .ToArray();

            // Assert
            Assert.AreEqual(5, obtainedHistory.Length);
            Assert.AreEqual(new DateTime(2017, 06, 10), obtainedHistory[0].Timestamp);
            Assert.AreEqual(new DateTime(2017, 06, 11), obtainedHistory[1].Timestamp);
            Assert.AreEqual(new DateTime(2017, 06, 12), obtainedHistory[2].Timestamp);
            Assert.AreEqual(new DateTime(2017, 06, 13), obtainedHistory[3].Timestamp);
            Assert.AreEqual(new DateTime(2017, 06, 14), obtainedHistory[4].Timestamp);
        }

        [Ignore]
        [TestMethod]
        public async Task To_many_history_truncated_at_initialization()
        {
            // Arrange
            var history = new[]
            {
                new TestCandle
                {
                    AssetPairId = "EURUSD",
                    PriceType = CandlePriceType.Ask,
                    TimeInterval = CandleTimeInterval.Day,
                    Open = 1.5,
                    Close = 1.6,
                    Low = 1.3,
                    High = 1.8,
                    Timestamp = new DateTime(2017, 06, 10, 0, 0, 0, DateTimeKind.Utc)
                },
                new TestCandle
                {
                    AssetPairId = "EURUSD",
                    PriceType = CandlePriceType.Ask,
                    TimeInterval = CandleTimeInterval.Day,
                    Open = 1.6,
                    Close = 1.7,
                    Low = 1.4,
                    High = 1.9,
                    Timestamp = new DateTime(2017, 06, 11, 0, 0, 0, DateTimeKind.Utc)
                },
                new TestCandle
                {
                    AssetPairId = "EURUSD",
                    PriceType = CandlePriceType.Ask,
                    TimeInterval = CandleTimeInterval.Day,
                    Open = 1.6,
                    Close = 1.2,
                    Low = 1.3,
                    High = 1.5,
                    Timestamp = new DateTime(2017, 06, 12, 0, 0, 0, DateTimeKind.Utc)
                },
                new TestCandle
                {
                    AssetPairId = "EURUSD",
                    PriceType = CandlePriceType.Ask,
                    TimeInterval = CandleTimeInterval.Day,
                    Open = 1.3,
                    Close = 1.6,
                    Low = 1.3,
                    High = 1.8,
                    Timestamp = new DateTime(2017, 06, 13, 0, 0, 0, DateTimeKind.Utc)
                },
                new TestCandle
                {
                    AssetPairId = "EURUSD",
                    PriceType = CandlePriceType.Ask,
                    TimeInterval = CandleTimeInterval.Day,
                    Open = 1.6,
                    Close = 1.6,
                    Low = 1.4,
                    High = 1.7,
                    Timestamp = new DateTime(2017, 06, 14, 0, 0, 0, DateTimeKind.Utc)
                },
                new TestCandle
                {
                    AssetPairId = "EURUSD",
                    PriceType = CandlePriceType.Ask,
                    TimeInterval = CandleTimeInterval.Day,
                    Open = 1.6,
                    Close = 1.6,
                    Low = 1.4,
                    High = 1.7,
                    Timestamp = new DateTime(2017, 06, 15, 0, 0, 0, DateTimeKind.Utc)
                },
            };

            // Act
            await _service.InitializeAsync("EURUSD", CandlePriceType.Ask, CandleTimeInterval.Day, history);

            var obtainedHistory = (await _service.GetCandlesAsync("EURUSD", CandlePriceType.Ask, CandleTimeInterval.Day,
                    new DateTime(2017, 06, 01, 0, 0, 0, DateTimeKind.Utc),
                    new DateTime(2017, 07, 01, 0, 0, 0, DateTimeKind.Utc)))
                .ToArray();

            // Assert
            Assert.AreEqual(5, obtainedHistory.Length);
            Assert.AreEqual(new DateTime(2017, 06, 10), obtainedHistory[0].Timestamp);
            Assert.AreEqual(new DateTime(2017, 06, 11), obtainedHistory[1].Timestamp);
            Assert.AreEqual(new DateTime(2017, 06, 12), obtainedHistory[2].Timestamp);
            Assert.AreEqual(new DateTime(2017, 06, 13), obtainedHistory[3].Timestamp);
            Assert.AreEqual(new DateTime(2017, 06, 14), obtainedHistory[4].Timestamp);
        }

        [Ignore]
        [TestMethod]
        public async Task Not_full_history_initializes()
        {
            // Arrange
            var history = new[]
            {
                new TestCandle
                {
                    AssetPairId = "EURUSD",
                    PriceType = CandlePriceType.Ask,
                    TimeInterval = CandleTimeInterval.Day,
                    Open = 1.5,
                    Close = 1.6,
                    Low = 1.3,
                    High = 1.8,
                    Timestamp = new DateTime(2017, 06, 10, 0, 0, 0, DateTimeKind.Utc)
                },
                new TestCandle
                {
                    AssetPairId = "EURUSD",
                    PriceType = CandlePriceType.Ask,
                    TimeInterval = CandleTimeInterval.Day,
                    Open = 1.6,
                    Close = 1.7,
                    Low = 1.4,
                    High = 1.9,
                    Timestamp = new DateTime(2017, 06, 11, 0, 0, 0, DateTimeKind.Utc)
                },
                new TestCandle
                {
                    AssetPairId = "EURUSD",
                    PriceType = CandlePriceType.Ask,
                    TimeInterval = CandleTimeInterval.Day,
                    Open = 1.6,
                    Close = 1.2,
                    Low = 1.3,
                    High = 1.5,
                    Timestamp = new DateTime(2017, 06, 12, 0, 0, 0, DateTimeKind.Utc)
                },
                new TestCandle
                {
                    AssetPairId = "EURUSD",
                    PriceType = CandlePriceType.Ask,
                    TimeInterval = CandleTimeInterval.Day,
                    Open = 1.3,
                    Close = 1.6,
                    Low = 1.3,
                    High = 1.8,
                    Timestamp = new DateTime(2017, 06, 13, 0, 0, 0, DateTimeKind.Utc)
                },
            };

            // Act
            await _service.InitializeAsync("EURUSD", CandlePriceType.Ask, CandleTimeInterval.Day, history);

            var obtainedHistory = (await _service.GetCandlesAsync("EURUSD", CandlePriceType.Ask, CandleTimeInterval.Day,
                    new DateTime(2017, 06, 01, 0, 0, 0, DateTimeKind.Utc),
                    new DateTime(2017, 07, 01, 0, 0, 0, DateTimeKind.Utc)))
                .ToArray();

            // Assert
            Assert.AreEqual(4, obtainedHistory.Length);
            Assert.AreEqual(new DateTime(2017, 06, 10), obtainedHistory[0].Timestamp);
            Assert.AreEqual(new DateTime(2017, 06, 11), obtainedHistory[1].Timestamp);
            Assert.AreEqual(new DateTime(2017, 06, 12), obtainedHistory[2].Timestamp);
            Assert.AreEqual(new DateTime(2017, 06, 13), obtainedHistory[3].Timestamp);
        }

        #endregion


        #region Candle adding

        [Ignore]
        [TestMethod]
        public async Task Adding_candle_without_history_adds_candle()
        {
            // Arrange
            var candle = new TestCandle
            {
                AssetPairId = "EURUSD",
                PriceType = CandlePriceType.Ask,
                TimeInterval = CandleTimeInterval.Day,
                Timestamp = new DateTime(2017, 06, 23, 0, 0, 0, 0, DateTimeKind.Utc),
                Open = 2,
                Close = 2,
                Low = 2,
                High = 2,
            };

            // Act
            await _service.CacheAsync(candle);

            var candles = (await _service.GetCandlesAsync("EURUSD", CandlePriceType.Ask, CandleTimeInterval.Day,
                    new DateTime(2017, 06, 01, 0, 0, 0, DateTimeKind.Utc),
                    new DateTime(2017, 07, 01, 0, 0, 0, DateTimeKind.Utc)))
                .ToArray();

            // Assert
            Assert.AreEqual(1, candles.Length);
            Assert.AreEqual(2, candles[0].Open);
            Assert.AreEqual(2, candles[0].Close);
            Assert.AreEqual(2, candles[0].Low);
            Assert.AreEqual(2, candles[0].High);
            Assert.AreEqual("EURUSD", candles[0].AssetPairId);
            Assert.AreEqual(CandlePriceType.Ask, candles[0].PriceType);
            Assert.AreEqual(CandleTimeInterval.Day, candles[0].TimeInterval);
            Assert.AreEqual(new DateTime(2017, 06, 23), candles[0].Timestamp);
        }

        [Ignore]
        [TestMethod]
        public async Task Adding_candle_replaces_history()
        {
            // Arrange
            var history = new[]
            {
                new TestCandle
                {
                    AssetPairId = "EURUSD",
                    PriceType = CandlePriceType.Ask,
                    TimeInterval = CandleTimeInterval.Day,
                    Open = 1.5, Close = 1.6, Low = 1.3, High = 1.8,
                    Timestamp = new DateTime(2017, 06, 10, 0, 0, 0, DateTimeKind.Utc),
                    LastUpdateTimestamp = new DateTime(2017, 06, 10, 0, 0, 0, DateTimeKind.Utc)
                },
                new TestCandle
                {
                    AssetPairId = "EURUSD",
                    PriceType = CandlePriceType.Ask,
                    TimeInterval = CandleTimeInterval.Day,
                    Open = 1.6, Close = 1.7, Low = 1.4, High = 1.9,
                    Timestamp = new DateTime(2017, 06, 11, 0, 0, 0, DateTimeKind.Utc),
                    LastUpdateTimestamp = new DateTime(2017, 06, 11, 0, 0, 0, DateTimeKind.Utc)
                },
                new TestCandle
                {
                    AssetPairId = "EURUSD",
                    PriceType = CandlePriceType.Ask,
                    TimeInterval = CandleTimeInterval.Day,
                    Open = 1.6, Close = 1.2, Low = 1.3, High = 1.5,
                    Timestamp = new DateTime(2017, 06, 12, 0, 0, 0, DateTimeKind.Utc),
                    LastUpdateTimestamp = new DateTime(2017, 06, 12, 0, 0, 0, DateTimeKind.Utc)
                },
                new TestCandle
                {
                    AssetPairId = "EURUSD",
                    PriceType = CandlePriceType.Ask,
                    TimeInterval = CandleTimeInterval.Day,
                    Open = 1.3, Close = 1.6, Low = 1.3, High = 1.8,
                    Timestamp = new DateTime(2017, 06, 13, 0, 0, 0, DateTimeKind.Utc),
                    LastUpdateTimestamp = new DateTime(2017, 06, 13, 0, 0, 0, DateTimeKind.Utc)
                },
                new TestCandle
                {
                    AssetPairId = "EURUSD",
                    PriceType = CandlePriceType.Ask,
                    TimeInterval = CandleTimeInterval.Day,
                    Open = 1.6, Close = 1.6, Low = 1.4, High = 1.7,
                    Timestamp = new DateTime(2017, 06, 14, 0, 0, 0, DateTimeKind.Utc),
                    LastUpdateTimestamp = new DateTime(2017, 06, 14, 0, 0, 10, DateTimeKind.Utc)
                }
            };
            var candle = new TestCandle
            {
                AssetPairId = "EURUSD",
                PriceType = CandlePriceType.Ask,
                TimeInterval = CandleTimeInterval.Day,
                Open = 2,
                Close = 2,
                Low = 2,
                High = 2,
                Timestamp = new DateTime(2017, 06, 14, 0, 0, 0, 0, DateTimeKind.Utc),
                LastUpdateTimestamp = new DateTime(2017, 06, 14, 0, 0, 20, DateTimeKind.Utc)
            };

            await _service.InitializeAsync("EURUSD", CandlePriceType.Ask, CandleTimeInterval.Day, history);

            // Act
            await _service.CacheAsync(candle);

            var candles = (await _service.GetCandlesAsync("EURUSD", CandlePriceType.Ask, CandleTimeInterval.Day,
                    new DateTime(2017, 06, 01, 0, 0, 0, DateTimeKind.Utc),
                    new DateTime(2017, 07, 01, 0, 0, 0, DateTimeKind.Utc)))
                .ToArray();

            // Assert
            Assert.AreEqual(5, candles.Length);

            Assert.AreEqual(2, candles[4].Open);
            Assert.AreEqual(2, candles[4].Close);
            Assert.AreEqual(2, candles[4].Low);
            Assert.AreEqual(2, candles[4].High);
            Assert.AreEqual("EURUSD", candles[4].AssetPairId);
            Assert.AreEqual(CandlePriceType.Ask, candles[4].PriceType);
            Assert.AreEqual(CandleTimeInterval.Day, candles[4].TimeInterval);
            Assert.AreEqual(new DateTime(2017, 06, 14), candles[4].Timestamp);
        }

        [Ignore]
        [TestMethod]
        public async Task Sequental_candles_addition_without_history_replaces_candles()
        {
            // Arrange
            var candle1 = new TestCandle
            {
                AssetPairId = "EURUSD",
                PriceType = CandlePriceType.Ask,
                TimeInterval = CandleTimeInterval.Day,
                Timestamp = new DateTime(2017, 06, 23, 0, 0, 0, 0, DateTimeKind.Utc),
                LastUpdateTimestamp = new DateTime(2017, 06, 23, 0, 0, 0, 10, DateTimeKind.Utc),
                Open = 2,
                Close = 2,
                Low = 2,
                High = 2
            };
            var candle2 = new TestCandle
            {
                AssetPairId = "EURUSD",
                PriceType = CandlePriceType.Ask,
                TimeInterval = CandleTimeInterval.Day,
                Timestamp = new DateTime(2017, 06, 23, 0, 0, 0, 0, DateTimeKind.Utc),
                LastUpdateTimestamp = new DateTime(2017, 06, 23, 0, 0, 0, 20, DateTimeKind.Utc),
                Open = 4,
                Close = 4,
                Low = 4,
                High = 4,
            };

            // Act
            await _service.CacheAsync(candle1);
            await _service.CacheAsync(candle2);

            var candles = (await _service.GetCandlesAsync("EURUSD", CandlePriceType.Ask, CandleTimeInterval.Day,
                    new DateTime(2017, 06, 01, 0, 0, 0, DateTimeKind.Utc),
                    new DateTime(2017, 07, 01, 0, 0, 0, DateTimeKind.Utc)))
                .ToArray();

            // Assert
            Assert.AreEqual(1, candles.Length);
            Assert.AreEqual(4, candles[0].Open);
            Assert.AreEqual(4, candles[0].Close);
            Assert.AreEqual(4, candles[0].Low);
            Assert.AreEqual(4, candles[0].High);
            Assert.AreEqual("EURUSD", candles[0].AssetPairId);
            Assert.AreEqual(CandlePriceType.Ask, candles[0].PriceType);
            Assert.AreEqual(CandleTimeInterval.Day, candles[0].TimeInterval);
            Assert.AreEqual(new DateTime(2017, 06, 23), candles[0].Timestamp);
        }

        [Ignore]
        [TestMethod]
        public async Task Outdated_candle_addition_ignored()
        {
            // Arrange
            var candle1 = new TestCandle
            {
                AssetPairId = "EURUSD",
                PriceType = CandlePriceType.Ask,
                TimeInterval = CandleTimeInterval.Day,
                Timestamp = new DateTime(2017, 06, 23, 0, 0, 0, 0, DateTimeKind.Utc),
                LastUpdateTimestamp = new DateTime(2017, 06, 23, 0, 0, 0, 10, DateTimeKind.Utc),
                Open = 2,
                Close = 2,
                Low = 2,
                High = 2
            };
            var candle2 = new TestCandle
            {
                AssetPairId = "EURUSD",
                PriceType = CandlePriceType.Ask,
                TimeInterval = CandleTimeInterval.Day,
                Timestamp = new DateTime(2017, 06, 23, 0, 0, 0, 0, DateTimeKind.Utc),
                LastUpdateTimestamp = new DateTime(2017, 06, 23, 0, 0, 0, 05, DateTimeKind.Utc),
                Open = 4,
                Close = 4,
                Low = 4,
                High = 4,
            };

            // Act
            await _service.CacheAsync(candle1);
            await _service.CacheAsync(candle2);

            var candles = (await _service.GetCandlesAsync("EURUSD", CandlePriceType.Ask, CandleTimeInterval.Day,
                    new DateTime(2017, 06, 01, 0, 0, 0, DateTimeKind.Utc),
                    new DateTime(2017, 07, 01, 0, 0, 0, DateTimeKind.Utc)))
                .ToArray();

            // Assert
            Assert.AreEqual(1, candles.Length);
            Assert.AreEqual(2, candles[0].Open);
            Assert.AreEqual(2, candles[0].Close);
            Assert.AreEqual(2, candles[0].Low);
            Assert.AreEqual(2, candles[0].High);
            Assert.AreEqual("EURUSD", candles[0].AssetPairId);
            Assert.AreEqual(CandlePriceType.Ask, candles[0].PriceType);
            Assert.AreEqual(CandleTimeInterval.Day, candles[0].TimeInterval);
            Assert.AreEqual(new DateTime(2017, 06, 23), candles[0].Timestamp);
        }

        [Ignore]
        [TestMethod]
        public async Task To_old_candles_evicts()
        {
            // Arrange
            var candle1 = new TestCandle
            {
                AssetPairId = "EURUSD",
                PriceType = CandlePriceType.Ask,
                TimeInterval = CandleTimeInterval.Sec,
                Timestamp = new DateTime(2017, 06, 23, 13, 49, 23, 0, DateTimeKind.Utc),
                Open = 2,
                Close = 2,
                Low = 2,
                High = 2
            };
            var candle2 = new TestCandle
            {
                AssetPairId = "EURUSD",
                PriceType = CandlePriceType.Ask,
                TimeInterval = CandleTimeInterval.Sec,
                Timestamp = new DateTime(2017, 06, 23, 13, 49, 24, 0, DateTimeKind.Utc),
                Open = 1,
                Close = 1,
                Low = 1,
                High = 1,
            };
            var candle3 = new TestCandle
            {
                AssetPairId = "EURUSD",
                PriceType = CandlePriceType.Ask,
                TimeInterval = CandleTimeInterval.Sec,
                Timestamp = new DateTime(2017, 06, 23, 13, 49, 25, 0, DateTimeKind.Utc),
                Open = 5,
                Close = 5,
                Low = 5,
                High = 5,
            };
            var candle4 = new TestCandle
            {
                AssetPairId = "EURUSD",
                PriceType = CandlePriceType.Ask,
                TimeInterval = CandleTimeInterval.Sec,
                Timestamp = new DateTime(2017, 06, 23, 13, 49, 26, 0, DateTimeKind.Utc),
                Open = 4,
                Close = 4,
                Low = 4,
                High = 4,
            };
            var candle5 = new TestCandle
            {
                AssetPairId = "EURUSD",
                PriceType = CandlePriceType.Ask,
                TimeInterval = CandleTimeInterval.Sec,
                Timestamp = new DateTime(2017, 06, 23, 13, 49, 27, 0, DateTimeKind.Utc),
                Open = 3,
                Close = 3,
                Low = 3,
                High = 3,
            };
            var candle6 = new TestCandle
            {
                AssetPairId = "EURUSD",
                PriceType = CandlePriceType.Ask,
                TimeInterval = CandleTimeInterval.Sec,
                Timestamp = new DateTime(2017, 06, 23, 13, 49, 28, 0, DateTimeKind.Utc),
                Open = 7,
                Close = 7,
                Low = 7,
                High = 7,
            };

            // Act
            await _service.CacheAsync(candle1);
            await _service.CacheAsync(candle2);
            await _service.CacheAsync(candle3);
            await _service.CacheAsync(candle4);
            await _service.CacheAsync(candle5);
            await _service.CacheAsync(candle6);

            var candles = (await _service.GetCandlesAsync("EURUSD", CandlePriceType.Ask, CandleTimeInterval.Sec,
                    new DateTime(2017, 06, 01, 0, 0, 0, DateTimeKind.Utc),
                    new DateTime(2017, 07, 01, 0, 0, 0, DateTimeKind.Utc)))
                .ToArray();

            // Assert
            Assert.AreEqual(5, candles.Length);

            Assert.AreEqual(1, candles[0].Open);
            Assert.AreEqual(1, candles[0].Close);
            Assert.AreEqual(1, candles[0].Low);
            Assert.AreEqual(1, candles[0].High);
            Assert.AreEqual("EURUSD", candles[0].AssetPairId);
            Assert.AreEqual(CandlePriceType.Ask, candles[0].PriceType);
            Assert.AreEqual(CandleTimeInterval.Sec, candles[0].TimeInterval);
            Assert.AreEqual(new DateTime(2017, 06, 23, 13, 49, 24), candles[0].Timestamp);

            Assert.AreEqual(5, candles[1].Open);
            Assert.AreEqual(5, candles[1].Close);
            Assert.AreEqual(5, candles[1].Low);
            Assert.AreEqual(5, candles[1].High);
            Assert.AreEqual("EURUSD", candles[0].AssetPairId);
            Assert.AreEqual(CandlePriceType.Ask, candles[0].PriceType);
            Assert.AreEqual(CandleTimeInterval.Sec, candles[0].TimeInterval);
            Assert.AreEqual(new DateTime(2017, 06, 23, 13, 49, 25), candles[1].Timestamp);

            Assert.AreEqual(4, candles[2].Open);
            Assert.AreEqual(4, candles[2].Close);
            Assert.AreEqual(4, candles[2].Low);
            Assert.AreEqual(4, candles[2].High);
            Assert.AreEqual("EURUSD", candles[0].AssetPairId);
            Assert.AreEqual(CandlePriceType.Ask, candles[0].PriceType);
            Assert.AreEqual(CandleTimeInterval.Sec, candles[0].TimeInterval);
            Assert.AreEqual(new DateTime(2017, 06, 23, 13, 49, 26), candles[2].Timestamp);

            Assert.AreEqual(3, candles[3].Open);
            Assert.AreEqual(3, candles[3].Close);
            Assert.AreEqual(3, candles[3].Low);
            Assert.AreEqual(3, candles[3].High);
            Assert.AreEqual("EURUSD", candles[0].AssetPairId);
            Assert.AreEqual(CandlePriceType.Ask, candles[0].PriceType);
            Assert.AreEqual(CandleTimeInterval.Sec, candles[0].TimeInterval);
            Assert.AreEqual(new DateTime(2017, 06, 23, 13, 49, 27), candles[3].Timestamp);

            Assert.AreEqual(7, candles[4].Open);
            Assert.AreEqual(7, candles[4].Close);
            Assert.AreEqual(7, candles[4].Low);
            Assert.AreEqual(7, candles[4].High);
            Assert.AreEqual("EURUSD", candles[0].AssetPairId);
            Assert.AreEqual(CandlePriceType.Ask, candles[0].PriceType);
            Assert.AreEqual(CandleTimeInterval.Sec, candles[0].TimeInterval);
            Assert.AreEqual(new DateTime(2017, 06, 23, 13, 49, 28), candles[4].Timestamp);
        }

        #endregion


        #region Candles getting

        [TestMethod]
        public async Task Getting_empty_history_returns_empty_enumerable()
        {
            // Act
            var candles = await _service.GetCandlesAsync("EURUSD", CandlePriceType.Ask, CandleTimeInterval.Day,
                new DateTime(2017, 06, 01, 0, 0, 0, DateTimeKind.Utc),
                new DateTime(2017, 07, 01, 0, 0, 0, DateTimeKind.Utc));

            // Assert
            Assert.IsNotNull(candles);
            Assert.IsFalse(candles.Any());
        }

        [Ignore]
        [TestMethod]
        public async Task Getting_filtered_by_from_and_to_moments_candles_returns_suitable_candles()
        {
            // Arrange
            var history = new[]
            {
                new TestCandle
                {
                    AssetPairId = "EURUSD",
                    PriceType = CandlePriceType.Ask,
                    TimeInterval = CandleTimeInterval.Day,
                    Open = 1.5, Close = 1.6, Low = 1.3, High = 1.8,
                    Timestamp = new DateTime(2017, 06, 10, 0, 0, 0, DateTimeKind.Utc)
                },
                new TestCandle
                {
                    AssetPairId = "EURUSD",
                    PriceType = CandlePriceType.Ask,
                    TimeInterval = CandleTimeInterval.Day,
                    Open = 1.6, Close = 1.7, Low = 1.4, High = 1.9,
                    Timestamp = new DateTime(2017, 06, 11, 0, 0, 0, DateTimeKind.Utc)
                },
                new TestCandle
                {
                    AssetPairId = "EURUSD",
                    PriceType = CandlePriceType.Ask,
                    TimeInterval = CandleTimeInterval.Day,
                    Open = 1.6, Close = 1.2, Low = 1.3, High = 1.5,
                    Timestamp = new DateTime(2017, 06, 12, 0, 0, 0, DateTimeKind.Utc)
                },
                new TestCandle
                {
                    AssetPairId = "EURUSD",
                    PriceType = CandlePriceType.Ask,
                    TimeInterval = CandleTimeInterval.Day,
                    Open = 1.3, Close = 1.6, Low = 1.3, High = 1.8,
                    Timestamp = new DateTime(2017, 06, 13, 0, 0, 0, DateTimeKind.Utc)
                },
                new TestCandle
                {
                    AssetPairId = "EURUSD",
                    PriceType = CandlePriceType.Ask,
                    TimeInterval = CandleTimeInterval.Day,
                    Open = 1.6, Close = 1.6, Low = 1.4, High = 1.7,
                    Timestamp = new DateTime(2017, 06, 14, 0, 0, 0, DateTimeKind.Utc)
                },
            };

            await _service.InitializeAsync("EURUSD", CandlePriceType.Ask, CandleTimeInterval.Day, history);

            // Act
            var candles = (await _service.GetCandlesAsync("EURUSD", CandlePriceType.Ask, CandleTimeInterval.Day,
                    new DateTime(2017, 06, 11, 0, 0, 0, DateTimeKind.Utc),
                    new DateTime(2017, 06, 14, 0, 0, 0, DateTimeKind.Utc)))
                .ToArray();

            // Assert
            Assert.AreEqual(3, candles.Length);
            Assert.AreEqual(new DateTime(2017, 06, 11), candles[0].Timestamp);
            Assert.AreEqual(new DateTime(2017, 06, 12), candles[1].Timestamp);
            Assert.AreEqual(new DateTime(2017, 06, 13), candles[2].Timestamp);
        }

        [TestMethod]
        public async Task Getting_filtered_by_from_and_to_moments_candles_returns_no_candles()
        {
            // Arrange
            var history = new[]
            {
                new TestCandle
                {
                    AssetPairId = "EURUSD",
                    PriceType = CandlePriceType.Ask,
                    TimeInterval = CandleTimeInterval.Day,
                    Open = 1.5, Close = 1.6, Low = 1.3, High = 1.8,
                    Timestamp = new DateTime(2017, 06, 10, 0, 0, 0, DateTimeKind.Utc)
                },
                new TestCandle
                {
                    AssetPairId = "EURUSD",
                    PriceType = CandlePriceType.Ask,
                    TimeInterval = CandleTimeInterval.Day,
                    Open = 1.6, Close = 1.7, Low = 1.4, High = 1.9,
                    Timestamp = new DateTime(2017, 06, 11, 0, 0, 0, DateTimeKind.Utc)
                },
                new TestCandle
                {
                    AssetPairId = "EURUSD",
                    PriceType = CandlePriceType.Ask,
                    TimeInterval = CandleTimeInterval.Day,
                    Open = 1.6, Close = 1.2, Low = 1.3, High = 1.5,
                    Timestamp = new DateTime(2017, 06, 12, 0, 0, 0, DateTimeKind.Utc)
                },
                new TestCandle
                {
                    AssetPairId = "EURUSD",
                    PriceType = CandlePriceType.Ask,
                    TimeInterval = CandleTimeInterval.Day,
                    Open = 1.3, Close = 1.6, Low = 1.3, High = 1.8,
                    Timestamp = new DateTime(2017, 06, 13, 0, 0, 0, DateTimeKind.Utc)
                },
                new TestCandle
                {
                    AssetPairId = "EURUSD",
                    PriceType = CandlePriceType.Ask,
                    TimeInterval = CandleTimeInterval.Day,
                    Open = 1.6, Close = 1.6, Low = 1.4, High = 1.7,
                    Timestamp = new DateTime(2017, 06, 14, 0, 0, 0, DateTimeKind.Utc)
                },
            };

            await _service.InitializeAsync("EURUSD", CandlePriceType.Ask, CandleTimeInterval.Day, history);

            // Act
            var candles1 = await _service.GetCandlesAsync("EURUSD", CandlePriceType.Ask, CandleTimeInterval.Day, new DateTime(2017, 05, 11, 0, 0, 0, DateTimeKind.Utc), new DateTime(2017, 05, 13, 0, 0, 0, DateTimeKind.Utc));
            var candles2 = await _service.GetCandlesAsync("EURUSD", CandlePriceType.Ask, CandleTimeInterval.Day, new DateTime(2017, 07, 11, 0, 0, 0, DateTimeKind.Utc), new DateTime(2017, 07, 13, 0, 0, 0, DateTimeKind.Utc));

            // Assert
            Assert.IsFalse(candles1.Any());
            Assert.IsFalse(candles2.Any());
        }

        [Ignore]
        [TestMethod]
        public async Task Getting_candles_with_another_asset_pair_or_price_type_or_interval_returns_no_candles()
        {
            // Arrange
            var history = new[]
            {
                new TestCandle
                {
                    AssetPairId = "EURUSD",
                    PriceType = CandlePriceType.Ask,
                    TimeInterval = CandleTimeInterval.Day,
                    Open = 1.5, Close = 1.6, Low = 1.3, High = 1.8,
                    Timestamp = new DateTime(2017, 06, 10, 0, 0, 0, DateTimeKind.Utc)
                },
                new TestCandle
                {
                    AssetPairId = "EURUSD",
                    PriceType = CandlePriceType.Ask,
                    TimeInterval = CandleTimeInterval.Day,
                    Open = 1.6, Close = 1.7, Low = 1.4, High = 1.9,
                    Timestamp = new DateTime(2017, 06, 11, 0, 0, 0, DateTimeKind.Utc)
                },
                new TestCandle
                {
                    AssetPairId = "EURUSD",
                    PriceType = CandlePriceType.Ask,
                    TimeInterval = CandleTimeInterval.Day,
                    Open = 1.6, Close = 1.2, Low = 1.3, High = 1.5,
                    Timestamp = new DateTime(2017, 06, 12, 0, 0, 0, DateTimeKind.Utc)
                },
                new TestCandle
                {
                    AssetPairId = "EURUSD",
                    PriceType = CandlePriceType.Ask,
                    TimeInterval = CandleTimeInterval.Day,
                    Open = 1.3, Close = 1.6, Low = 1.3, High = 1.8,
                    Timestamp = new DateTime(2017, 06, 13, 0, 0, 0, DateTimeKind.Utc)
                },
                new TestCandle
                {
                    AssetPairId = "EURUSD",
                    PriceType = CandlePriceType.Ask,
                    TimeInterval = CandleTimeInterval.Day,
                    Open = 1.6, Close = 1.6, Low = 1.4, High = 1.7,
                    Timestamp = new DateTime(2017, 06, 14, 0, 0, 0, DateTimeKind.Utc)
                },
            };

            var candle = new TestCandle
            {
                AssetPairId = "EURUSD",
                PriceType = CandlePriceType.Ask,
                TimeInterval = CandleTimeInterval.Day,
                Timestamp = new DateTime(2017, 06, 14, 0, 0, 0, 0, DateTimeKind.Utc),
                Open = 2,
                Close = 2,
                Low = 2,
                High = 2
            };

            await _service.InitializeAsync("EURUSD", CandlePriceType.Ask, CandleTimeInterval.Day, history);
            await _service.CacheAsync(candle);

            // Act
            var candles1 = await _service.GetCandlesAsync("USDCHF", CandlePriceType.Ask, CandleTimeInterval.Day, new DateTime(2017, 06, 01, 0, 0, 0, DateTimeKind.Utc), new DateTime(2017, 07, 01, 0, 0, 0, DateTimeKind.Utc));
            var candles2 = await _service.GetCandlesAsync("EURUSD", CandlePriceType.Bid, CandleTimeInterval.Day, new DateTime(2017, 06, 01, 0, 0, 0, DateTimeKind.Utc), new DateTime(2017, 07, 01, 0, 0, 0, DateTimeKind.Utc));
            var candles3 = await _service.GetCandlesAsync("EURUSD", CandlePriceType.Ask, CandleTimeInterval.Sec, new DateTime(2017, 06, 01, 0, 0, 0, DateTimeKind.Utc), new DateTime(2017, 07, 01, 0, 0, 0, DateTimeKind.Utc));

            // Assert
            Assert.IsFalse(candles1.Any());
            Assert.IsFalse(candles2.Any());
            Assert.IsFalse(candles3.Any());
        }

        #endregion
    }
}
