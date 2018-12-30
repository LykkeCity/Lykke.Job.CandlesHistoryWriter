using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.Candles;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.HistoryMigration.HistoryProviders.TradesSQLHistory;
using Lykke.Job.CandlesHistoryWriter.Services.HistoryMigration;
using Lykke.Job.CandlesProducer.Contract;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Lykke.Job.CandlesHistoryWriter.Tests.HistoryMigration.Trades
{
    [TestClass]
    public class TradesCandleBatchTest
    {
        private TradeHistoryItem[] _oneByOneTrades;
        private TradeHistoryItem[] _oneByTwoTrades;
        private TradeHistoryItem[] _oneByManyTrades;

        // ReSharper disable once UnusedMember.Local
        private const string AssetPairId = "LTCUSD";
        private const string AssetToken  = "663a1d65-cb66-4e8c-b51a-5b7f0f4817ecUSD";
        // ReSharper disable once UnusedMember.Local
        private const string ReverseAssetToken = "USD663a1d65-cb66-4e8c-b51a-5b7f0f4817ec"; // Reserved
        private const double PriceEpsilon = 0.00001; // Price accuracy for LTCUSD asset pair.
        private const double VolumeEpsilon = 0.0000000001; // Common volume accuracy (supposed to be good enougth).
        private const int PersistenceQueueLimit = 10_000;

        #region Initialization

        [TestInitialize]
        public void InitializeTest()
        {
            _oneByOneTrades = new[]
            {
                new TradeHistoryItem
                {
                    Id = 1037222, AssetToken = "663a1d65-cb66-4e8c-b51a-5b7f0f4817ecUSD", DateTime = DateTime.Parse("2018-02-05T09:58:27.4200000"), Direction = TradeDirection.Sell, Price = 1_000M,
                    OppositeOrderId = Guid.Parse("b18b46be-e8f9-43f2-a129-c028c8c72a13"), OppositeVolume = 1_000M, OrderId = Guid.Parse("bfaad618-359a-4403-9ccf-2f58009c9076"), Volume = 1M,
                    TradeId = "b18b46be-e8f9-43f2-a129-c028c8c72a13_bfaad618-359a-4403-9ccf-2f58009c9076", IsStraight = true
                },
                new TradeHistoryItem
                {
                    Id = 1037223, AssetToken = "USD663a1d65-cb66-4e8c-b51a-5b7f0f4817ec", DateTime = DateTime.Parse("2018-02-05T09:58:27.4200000"), Direction = TradeDirection.Buy, Price = 1_000M,
                    OppositeOrderId = Guid.Parse("bfaad618-359a-4403-9ccf-2f58009c9076"), OppositeVolume = 1M, OrderId = Guid.Parse("b18b46be-e8f9-43f2-a129-c028c8c72a13"), Volume = 1_000M,
                    TradeId = "b18b46be-e8f9-43f2-a129-c028c8c72a13_bfaad618-359a-4403-9ccf-2f58009c9076", IsStraight = false
                },
                new TradeHistoryItem
                {
                    Id = 1037224, AssetToken = "USD663a1d65-cb66-4e8c-b51a-5b7f0f4817ec", DateTime = DateTime.Parse("2018-02-05T09:58:27.4200000"), Direction = TradeDirection.Sell, Price = 1_000M,
                    OppositeOrderId = Guid.Parse("bfaad618-359a-4403-9ccf-2f58009c9076"), OppositeVolume = 1M, OrderId = Guid.Parse("b18b46be-e8f9-43f2-a129-c028c8c72a13"), Volume = 1_000M,
                    TradeId = "b18b46be-e8f9-43f2-a129-c028c8c72a13_bfaad618-359a-4403-9ccf-2f58009c9076", IsStraight = false
                },
                new TradeHistoryItem
                {
                    Id = 1037225, AssetToken = "663a1d65-cb66-4e8c-b51a-5b7f0f4817ecUSD", DateTime = DateTime.Parse("2018-02-05T09:58:27.4200000"), Direction = TradeDirection.Buy, Price = 1_000M,
                    OppositeOrderId = Guid.Parse("bfaad618-359a-4403-9ccf-2f58009c9076"), OppositeVolume = 1_000M, OrderId = Guid.Parse("b18b46be-e8f9-43f2-a129-c028c8c72a13"), Volume = 1M,
                    TradeId = "b18b46be-e8f9-43f2-a129-c028c8c72a13_bfaad618-359a-4403-9ccf-2f58009c9076", IsStraight = true
                }
            };

            _oneByTwoTrades = new[]
            {
                new TradeHistoryItem
                {
                    Id = 1036970, AssetToken = "663a1d65-cb66-4e8c-b51a-5b7f0f4817ecUSD", DateTime = DateTime.Parse("2018-02-05T08:44:15.8930000"), Direction = TradeDirection.Sell, Price = 100_000M,
                    OppositeOrderId = Guid.Parse("176a881e-c09b-42b6-b83a-017b2d1bed8e"), OppositeVolume = 0.1M, OrderId = Guid.Parse("1dfbcebd-0312-4d98-927e-c654ad4ce1d3"), Volume = 0.000001M,
                    TradeId = "176a881e-c09b-42b6-b83a-017b2d1bed8e_1dfbcebd-0312-4d98-927e-c654ad4ce1d3", IsStraight = true
                },
                new TradeHistoryItem
                {
                    Id = 1036971, AssetToken = "USD663a1d65-cb66-4e8c-b51a-5b7f0f4817ec", DateTime = DateTime.Parse("2018-02-05T08:44:15.8930000"), Direction = TradeDirection.Buy, Price = 100_000M,
                    OppositeOrderId = Guid.Parse("176a881e-c09b-42b6-b83a-017b2d1bed8e"), OppositeVolume = 0.000001M, OrderId = Guid.Parse("1dfbcebd-0312-4d98-927e-c654ad4ce1d3"), Volume = 0.1M,
                    TradeId = "176a881e-c09b-42b6-b83a-017b2d1bed8e_1dfbcebd-0312-4d98-927e-c654ad4ce1d3", IsStraight = false
                },
                new TradeHistoryItem
                {
                    Id = 1036972, AssetToken = "663a1d65-cb66-4e8c-b51a-5b7f0f4817ecUSD", DateTime = DateTime.Parse("2018-02-05T08:44:15.8930000"), Direction = TradeDirection.Sell, Price = 100_000M,
                    OppositeOrderId = Guid.Parse("e2e4d68c-6180-49a1-82de-8f366d0aad09"), OppositeVolume = 0.1M, OrderId = Guid.Parse("1dfbcebd-0312-4d98-927e-c654ad4ce1d3"), Volume = 0.000001M,
                    TradeId = "1dfbcebd-0312-4d98-927e-c654ad4ce1d3_e2e4d68c-6180-49a1-82de-8f366d0aad09", IsStraight = true
                },
                new TradeHistoryItem
                {
                    Id = 1036973, AssetToken = "USD663a1d65-cb66-4e8c-b51a-5b7f0f4817ec", DateTime = DateTime.Parse("2018-02-05T08:44:15.8930000"), Direction = TradeDirection.Buy, Price = 100_000M,
                    OppositeOrderId = Guid.Parse("e2e4d68c-6180-49a1-82de-8f366d0aad09"), OppositeVolume = 0.000001M, OrderId = Guid.Parse("1dfbcebd-0312-4d98-927e-c654ad4ce1d3"), Volume = 0.1M,
                    TradeId = "1dfbcebd-0312-4d98-927e-c654ad4ce1d3_e2e4d68c-6180-49a1-82de-8f366d0aad09", IsStraight = false
                },
                new TradeHistoryItem
                {
                    Id = 1036974, AssetToken = "USD663a1d65-cb66-4e8c-b51a-5b7f0f4817ec", DateTime = DateTime.Parse("2018-02-05T08:44:15.8930000"), Direction = TradeDirection.Sell, Price = 100_000M,
                    OppositeOrderId = Guid.Parse("1dfbcebd-0312-4d98-927e-c654ad4ce1d3"), OppositeVolume = 0.000001M, OrderId = Guid.Parse("176a881e-c09b-42b6-b83a-017b2d1bed8e"), Volume = 0.1M,
                    TradeId = "176a881e-c09b-42b6-b83a-017b2d1bed8e_1dfbcebd-0312-4d98-927e-c654ad4ce1d3", IsStraight = false
                },
                new TradeHistoryItem
                {
                    Id = 1036975, AssetToken = "663a1d65-cb66-4e8c-b51a-5b7f0f4817ecUSD", DateTime = DateTime.Parse("2018-02-05T08:44:15.8930000"), Direction = TradeDirection.Buy, Price = 100_000M,
                    OppositeOrderId = Guid.Parse("1dfbcebd-0312-4d98-927e-c654ad4ce1d3"), OppositeVolume = 0.1M, OrderId = Guid.Parse("176a881e-c09b-42b6-b83a-017b2d1bed8e"), Volume = 0.000001M,
                    TradeId = "176a881e-c09b-42b6-b83a-017b2d1bed8e_1dfbcebd-0312-4d98-927e-c654ad4ce1d3", IsStraight = true
                },
                new TradeHistoryItem
                {
                    Id = 1036976, AssetToken = "USD663a1d65-cb66-4e8c-b51a-5b7f0f4817ec", DateTime = DateTime.Parse("2018-02-05T08:44:15.8930000"), Direction = TradeDirection.Sell, Price = 100_000M,
                    OppositeOrderId = Guid.Parse("1dfbcebd-0312-4d98-927e-c654ad4ce1d3"), OppositeVolume = 0.000001M, OrderId = Guid.Parse("e2e4d68c-6180-49a1-82de-8f366d0aad09"), Volume = 0.1M,
                    TradeId = "1dfbcebd-0312-4d98-927e-c654ad4ce1d3_e2e4d68c-6180-49a1-82de-8f366d0aad09", IsStraight = false
                },
                new TradeHistoryItem
                {
                    Id = 1036977, AssetToken = "663a1d65-cb66-4e8c-b51a-5b7f0f4817ecUSD", DateTime = DateTime.Parse("2018-02-05T08:44:15.8930000"), Direction = TradeDirection.Buy, Price = 100_000M,
                    OppositeOrderId = Guid.Parse("1dfbcebd-0312-4d98-927e-c654ad4ce1d3"), OppositeVolume = 0.1M, OrderId = Guid.Parse("e2e4d68c-6180-49a1-82de-8f366d0aad09"), Volume = 0.000001M,
                    TradeId = "1dfbcebd-0312-4d98-927e-c654ad4ce1d3_e2e4d68c-6180-49a1-82de-8f366d0aad09", IsStraight = true
                }
            };

            _oneByManyTrades = new[]
            {
                new TradeHistoryItem
                {
                    Id = 1037382, AssetToken = "USD663a1d65-cb66-4e8c-b51a-5b7f0f4817ec", DateTime = DateTime.Parse("2018-02-05T10:22:34.8770000"), Direction = TradeDirection.Sell, Price = 10M,
                    OppositeOrderId = Guid.Parse("ff50b843-f2fa-495f-89d4-3da3b96e64d6"), OppositeVolume = 0.1M, OrderId = Guid.Parse("c2d7e028-ab96-4c22-a1df-d372c74535c8"), Volume = 1M,
                    TradeId = "c2d7e028-ab96-4c22-a1df-d372c74535c8_ff50b843-f2fa-495f-89d4-3da3b96e64d6", IsStraight = false
                },
                new TradeHistoryItem
                {
                    Id = 1037383, AssetToken = "663a1d65-cb66-4e8c-b51a-5b7f0f4817ecUSD", DateTime = DateTime.Parse("2018-02-05T10:22:34.8770000"), Direction = TradeDirection.Buy, Price = 10M,
                    OppositeOrderId = Guid.Parse("ff50b843-f2fa-495f-89d4-3da3b96e64d6"), OppositeVolume = 1M, OrderId = Guid.Parse("c2d7e028-ab96-4c22-a1df-d372c74535c8"), Volume = 0.1M,
                    TradeId = "c2d7e028-ab96-4c22-a1df-d372c74535c8_ff50b843-f2fa-495f-89d4-3da3b96e64d6", IsStraight = true
                },
                new TradeHistoryItem
                {
                    Id = 1037384, AssetToken = "USD663a1d65-cb66-4e8c-b51a-5b7f0f4817ec", DateTime = DateTime.Parse("2018-02-05T10:22:34.8770000"), Direction = TradeDirection.Sell, Price = 10M,
                    OppositeOrderId = Guid.Parse("49e000b7-aeca-4517-8ba4-bc2e974aa0f3"), OppositeVolume = 0.2M, OrderId = Guid.Parse("c2d7e028-ab96-4c22-a1df-d372c74535c8"), Volume = 2M,
                    TradeId = "49e000b7-aeca-4517-8ba4-bc2e974aa0f3_c2d7e028-ab96-4c22-a1df-d372c74535c8", IsStraight = false
                },
                new TradeHistoryItem
                {
                    Id = 1037385, AssetToken = "663a1d65-cb66-4e8c-b51a-5b7f0f4817ecUSD", DateTime = DateTime.Parse("2018-02-05T10:22:34.8770000"), Direction = TradeDirection.Buy, Price = 10M,
                    OppositeOrderId = Guid.Parse("49e000b7-aeca-4517-8ba4-bc2e974aa0f3"), OppositeVolume = 2M, OrderId = Guid.Parse("c2d7e028-ab96-4c22-a1df-d372c74535c8"), Volume = 0.2M,
                    TradeId = "49e000b7-aeca-4517-8ba4-bc2e974aa0f3_c2d7e028-ab96-4c22-a1df-d372c74535c8", IsStraight = true
                },
                new TradeHistoryItem
                {
                    Id = 1037386, AssetToken = "USD663a1d65-cb66-4e8c-b51a-5b7f0f4817ec", DateTime = DateTime.Parse("2018-02-05T10:22:34.8770000"), Direction = TradeDirection.Sell, Price = 10M,
                    OppositeOrderId = Guid.Parse("a18d8d2c-103e-4599-97be-114f6b3adb7a"), OppositeVolume = 0.5M, OrderId = Guid.Parse("c2d7e028-ab96-4c22-a1df-d372c74535c8"), Volume = 5M,
                    TradeId = "a18d8d2c-103e-4599-97be-114f6b3adb7a_c2d7e028-ab96-4c22-a1df-d372c74535c8", IsStraight = false
                },
                new TradeHistoryItem
                {
                    Id = 10373867, AssetToken = "663a1d65-cb66-4e8c-b51a-5b7f0f4817ecUSD", DateTime = DateTime.Parse("2018-02-05T10:22:34.8770000"), Direction = TradeDirection.Buy, Price = 10M,
                    OppositeOrderId = Guid.Parse("a18d8d2c-103e-4599-97be-114f6b3adb7a"), OppositeVolume = 5M, OrderId = Guid.Parse("c2d7e028-ab96-4c22-a1df-d372c74535c8"), Volume = 0.5M,
                    TradeId = "a18d8d2c-103e-4599-97be-114f6b3adb7a_c2d7e028-ab96-4c22-a1df-d372c74535c8", IsStraight = true
                },
                new TradeHistoryItem
                {
                    Id = 1037388, AssetToken = "663a1d65-cb66-4e8c-b51a-5b7f0f4817ecUSD", DateTime = DateTime.Parse("2018-02-05T10:22:34.8770000"), Direction = TradeDirection.Sell, Price = 10M,
                    OppositeOrderId = Guid.Parse("c2d7e028-ab96-4c22-a1df-d372c74535c8"), OppositeVolume = 1.5M, OrderId = Guid.Parse("ff50b843-f2fa-495f-89d4-3da3b96e64d6"), Volume = 0.15M,
                    TradeId = "c2d7e028-ab96-4c22-a1df-d372c74535c8_ff50b843-f2fa-495f-89d4-3da3b96e64d6", IsStraight = true
                },
                new TradeHistoryItem
                {
                    Id = 1037389, AssetToken = "USD663a1d65-cb66-4e8c-b51a-5b7f0f4817ec", DateTime = DateTime.Parse("2018-02-05T10:22:34.8770000"), Direction = TradeDirection.Buy, Price = 10M,
                    OppositeOrderId = Guid.Parse("c2d7e028-ab96-4c22-a1df-d372c74535c8"), OppositeVolume = 0.15M, OrderId = Guid.Parse("ff50b843-f2fa-495f-89d4-3da3b96e64d6"), Volume = 1.5M,
                    TradeId = "c2d7e028-ab96-4c22-a1df-d372c74535c8_ff50b843-f2fa-495f-89d4-3da3b96e64d6", IsStraight = false
                },
                new TradeHistoryItem
                {
                    Id = 1037390, AssetToken = "663a1d65-cb66-4e8c-b51a-5b7f0f4817ecUSD", DateTime = DateTime.Parse("2018-02-05T10:22:34.8770000"), Direction = TradeDirection.Sell, Price = 10M,
                    OppositeOrderId = Guid.Parse("c2d7e028-ab96-4c22-a1df-d372c74535c8"), OppositeVolume = 6M, OrderId = Guid.Parse("49e000b7-aeca-4517-8ba4-bc2e974aa0f3"), Volume = 0.6M,
                    TradeId = "49e000b7-aeca-4517-8ba4-bc2e974aa0f3_c2d7e028-ab96-4c22-a1df-d372c74535c8", IsStraight = true
                },
                new TradeHistoryItem
                {
                    Id = 1037391, AssetToken = "USD663a1d65-cb66-4e8c-b51a-5b7f0f4817ec", DateTime = DateTime.Parse("2018-02-05T10:22:34.8770000"), Direction = TradeDirection.Buy, Price = 10M,
                    OppositeOrderId = Guid.Parse("c2d7e028-ab96-4c22-a1df-d372c74535c8"), OppositeVolume = 0.6M, OrderId = Guid.Parse("49e000b7-aeca-4517-8ba4-bc2e974aa0f3"), Volume = 6M,
                    TradeId = "49e000b7-aeca-4517-8ba4-bc2e974aa0f3_c2d7e028-ab96-4c22-a1df-d372c74535c8", IsStraight = false
                },
                new TradeHistoryItem
                {
                    Id = 1037392, AssetToken = "663a1d65-cb66-4e8c-b51a-5b7f0f4817ecUSD", DateTime = DateTime.Parse("2018-02-05T10:22:34.8770000"), Direction = TradeDirection.Sell, Price = 10M,
                    OppositeOrderId = Guid.Parse("c2d7e028-ab96-4c22-a1df-d372c74535c8"), OppositeVolume = 1M, OrderId = Guid.Parse("a18d8d2c-103e-4599-97be-114f6b3adb7a"), Volume = 0.1M,
                    TradeId = "a18d8d2c-103e-4599-97be-114f6b3adb7a_c2d7e028-ab96-4c22-a1df-d372c74535c8", IsStraight = true
                },
                new TradeHistoryItem
                {
                    Id = 1037393, AssetToken = "USD663a1d65-cb66-4e8c-b51a-5b7f0f4817ec", DateTime = DateTime.Parse("2018-02-05T10:22:34.8770000"), Direction = TradeDirection.Buy, Price = 10M,
                    OppositeOrderId = Guid.Parse("c2d7e028-ab96-4c22-a1df-d372c74535c8"), OppositeVolume = 0.1M, OrderId = Guid.Parse("a18d8d2c-103e-4599-97be-114f6b3adb7a"), Volume = 1M,
                    TradeId = "a18d8d2c-103e-4599-97be-114f6b3adb7a_c2d7e028-ab96-4c22-a1df-d372c74535c8", IsStraight = false
                }
            };

            /*
             * new TradeHistoryItem
                {
                    Id = , AssetToken = "", DateTime = DateTime.Parse(""), Direction = TradeDirection, Price = M,
                    OppositeOrderId = Guid.Parse(""), OppositeVolume = M, OrderId = Guid.Parse(""), Volume = M,
                    TradeId = ""
                }
             */
        }

        #endregion

        [TestCategory("TradesMigration")]
        [TestMethod]
        public void Building_candles_from_one_by_one_trades_batch()
        {
            // Arrange
            var healthService = new TradesMigrationHealthService();
            healthService.Prepare(10_000, null);
            var repo = new CandlesHistoryRepositoryMock();
            var historyMaker = new TradesMigrationService.TradesProcessor(healthService, repo, AssetToken, PersistenceQueueLimit, null);

            // Act
            historyMaker.ProcessTradesBatch(_oneByOneTrades).GetAwaiter().GetResult();
            historyMaker.FlushCandlesIfAny().Wait();

            // Assert
            Assert.AreEqual(repo.Candles.Count, 6);

            var secondsCandle = repo.Candles[CandleTimeInterval.Sec].Single();

            Assert.IsTrue(Math.Abs(secondsCandle.Open  - 1_000) < PriceEpsilon);
            Assert.IsTrue(Math.Abs(secondsCandle.Close - 1_000) < PriceEpsilon);
            Assert.IsTrue(Math.Abs(secondsCandle.High  - 1_000) < PriceEpsilon);
            Assert.IsTrue(Math.Abs(secondsCandle.Low   - 1_000) < PriceEpsilon);

            Assert.IsTrue(Math.Abs(secondsCandle.TradingVolume         - 1)     < VolumeEpsilon);
            Assert.IsTrue(Math.Abs(secondsCandle.TradingOppositeVolume - 1_000) < VolumeEpsilon);
        }

        [TestCategory("TradesMigration")]
        [TestMethod]
        public void Building_candles_from_one_by_two_trades_batch()
        {
            // Arrange
            var healthService = new TradesMigrationHealthService();
            healthService.Prepare(10_000, null);
            var repo = new CandlesHistoryRepositoryMock();
            var historyMaker = new TradesMigrationService.TradesProcessor(healthService, repo, AssetToken, PersistenceQueueLimit, null);

            // Act
            historyMaker.ProcessTradesBatch(_oneByTwoTrades).GetAwaiter().GetResult();
            historyMaker.FlushCandlesIfAny().Wait();

            // Assert
            Assert.AreEqual(repo.Candles.Count, 6);

            var secondsCandle = repo.Candles[CandleTimeInterval.Sec].Single();

            Assert.IsTrue(Math.Abs(secondsCandle.Open  - 100_000) < PriceEpsilon);
            Assert.IsTrue(Math.Abs(secondsCandle.Close - 100_000) < PriceEpsilon);
            Assert.IsTrue(Math.Abs(secondsCandle.High  - 100_000) < PriceEpsilon);
            Assert.IsTrue(Math.Abs(secondsCandle.Low   - 100_000) < PriceEpsilon);

            Assert.IsTrue(Math.Abs(secondsCandle.TradingVolume         - 0.000002) < VolumeEpsilon);
            Assert.IsTrue(Math.Abs(secondsCandle.TradingOppositeVolume - 0.2)      < VolumeEpsilon);
        }

        [TestCategory("TradesMigration")]
        [TestMethod]
        public void Building_candles_from_one_by_many_trades_batch()
        {
            // Arrange
            var healthService = new TradesMigrationHealthService();
            healthService.Prepare(10_000, null);
            var repo = new CandlesHistoryRepositoryMock();
            var historyMaker = new TradesMigrationService.TradesProcessor(healthService, repo, AssetToken, PersistenceQueueLimit, null);

            // Act
            historyMaker.ProcessTradesBatch(_oneByManyTrades).GetAwaiter().GetResult();
            historyMaker.FlushCandlesIfAny().Wait();

            // Assert
            Assert.AreEqual(repo.Candles.Count, 6);

            var secondsCandle = repo.Candles[CandleTimeInterval.Sec].Single();

            Assert.IsTrue(Math.Abs(secondsCandle.Open  - 10) < PriceEpsilon);
            Assert.IsTrue(Math.Abs(secondsCandle.Close - 10) < PriceEpsilon);
            Assert.IsTrue(Math.Abs(secondsCandle.High  - 10) < PriceEpsilon);
            Assert.IsTrue(Math.Abs(secondsCandle.Low   - 10) < PriceEpsilon);

            Assert.IsTrue(Math.Abs(secondsCandle.TradingVolume         - 0.825) < VolumeEpsilon);
            Assert.IsTrue(Math.Abs(secondsCandle.TradingOppositeVolume - 8.25) < VolumeEpsilon);
        }
    }

    #region Mocks

    public class CandlesHistoryRepositoryMock : ICandlesHistoryRepository
    {
        public readonly Dictionary<CandleTimeInterval, List<ICandle>> Candles;

        public CandlesHistoryRepositoryMock()
        {
            Candles = new Dictionary<CandleTimeInterval, List<ICandle>>();
            foreach (var si in Services.Candles.Constants.StoredIntervals)
                Candles.Add(si, new List<ICandle>());
        }

        public async Task<int> ReplaceCandlesAsync(IReadOnlyList<ICandle> candlesToReplace)
        {
            Candles[candlesToReplace.First().TimeInterval].AddRange(candlesToReplace);

            await Task.Delay(10);
            return 0;
        }

        #region NotImplemented

        public async Task<IReadOnlyCollection<ICandle>> GetExactCandlesAsync(string assetPairId, CandleTimeInterval interval, CandlePriceType priceType, DateTime to,
            int candlesCount)
        {
            throw new NotImplementedException();
        }

        public bool CanStoreAssetPair(string assetPairId)
        {
            throw new NotImplementedException();
        }

        public Task<int> DeleteCandlesAsync(IReadOnlyList<ICandle> candlesToDelete)
        {
            throw new NotImplementedException();
        }

        public Task<IEnumerable<ICandle>> GetCandlesAsync(string assetPairId, CandleTimeInterval interval, CandlePriceType priceType, DateTime from, DateTime to)
        {
            throw new NotImplementedException();
        }

        public IReadOnlyList<string> GetStoredAssetPairs()
        {
            throw new NotImplementedException();
        }

        public Task InsertOrMergeAsync(IEnumerable<ICandle> candles, string assetPairId, CandlePriceType priceType, CandleTimeInterval timeInterval)
        {
            throw new NotImplementedException();
        }

        public Task<ICandle> TryGetFirstCandleAsync(string assetPairId, CandleTimeInterval interval, CandlePriceType priceType)
        {
            throw new NotImplementedException();
        }

        #endregion
    }

    #endregion
}
