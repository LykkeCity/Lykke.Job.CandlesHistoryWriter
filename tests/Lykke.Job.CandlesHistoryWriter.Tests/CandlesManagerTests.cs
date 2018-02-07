using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using Lykke.Job.CandlesProducer.Contract;
using Lykke.Service.Assets.Client.Custom;
using Lykke.Service.Assets.Client.Models;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.Candles;
using Lykke.Job.CandlesHistoryWriter.Core.Services.Assets;
using Lykke.Job.CandlesHistoryWriter.Core.Services.Candles;
using Lykke.Job.CandlesHistoryWriter.Services.Candles;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;

namespace Lykke.Job.CandlesHistoryWriter.Tests
{
    [TestClass]
    public class CandlesManagerTests
    {
        private static readonly ImmutableArray<CandleTimeInterval> StoredIntervals = ImmutableArray.Create
        (
            CandleTimeInterval.Sec,
            CandleTimeInterval.Minute,
            CandleTimeInterval.Min30,
            CandleTimeInterval.Hour,
            CandleTimeInterval.Day,
            CandleTimeInterval.Week,
            CandleTimeInterval.Month
        );

        private List<IAssetPair> _assetPairs;

        private Mock<ICandlesCacheService> _cacheServiceMock;
        private Mock<ICandlesHistoryRepository> _historyRepositoryMock;
        private Mock<IAssetPairsManager> _assetPairsManagerMock;
        private ICandlesManager _manager;

        private Mock<ICandlesPersistenceQueue> _persistenceQueueMock;

        [TestInitialize]
        public void InitializeTest()
        {
            _cacheServiceMock = new Mock<ICandlesCacheService>();
            _historyRepositoryMock = new Mock<ICandlesHistoryRepository>();
            _assetPairsManagerMock = new Mock<IAssetPairsManager>();
            _persistenceQueueMock = new Mock<ICandlesPersistenceQueue>();

            _assetPairs = new List<IAssetPair>
            {
                new AssetPairResponseModel {Id = "EURUSD", Accuracy = 3},
                new AssetPairResponseModel {Id = "USDCHF", Accuracy = 2},
                new AssetPairResponseModel {Id = "EURRUB", Accuracy = 2}
            };

            _assetPairsManagerMock
                .Setup(m => m.GetAllEnabledAsync())
                .ReturnsAsync(() => _assetPairs);
            _assetPairsManagerMock
                .Setup(m => m.TryGetEnabledPairAsync(It.IsAny<string>()))
                .ReturnsAsync((string assetPairId) => _assetPairs.SingleOrDefault(a => a.Id == assetPairId));
            _historyRepositoryMock
                .Setup(m => m.CanStoreAssetPair(It.IsAny<string>()))
                .Returns((string assetPairId) => new[] { "EURUSD", "USDCHF", "USDRUB" }.Contains(assetPairId));

            _manager = new CandlesManager(
                _cacheServiceMock.Object,
                _historyRepositoryMock.Object,
                _persistenceQueueMock.Object);
        }


        #region Candle processing

        [TestMethod]
        public async Task Only_candle_for_asset_pairs_with_connection_string_are_processed()
        {
            // Arrange
            var quote1 = new TestCandle { AssetPairId = "EURUSD", TimeInterval = StoredIntervals.First() };
            var quote3 = new TestCandle { AssetPairId = "EURRUB", TimeInterval = StoredIntervals.First() };

            // Act
            await _manager.ProcessCandleAsync(quote1);
            await _manager.ProcessCandleAsync(quote3);

            // Assert
            _cacheServiceMock.Verify(s => s.CacheAsync(It.Is<ICandle>(c => c.AssetPairId == "EURUSD")), Times.Once);
            _persistenceQueueMock.Verify(s => s.EnqueueCandle(It.Is<ICandle>(c => c.AssetPairId == "EURUSD")), Times.Once);

            _cacheServiceMock.Verify(s => s.CacheAsync(It.Is<ICandle>(c => c.AssetPairId == "EURRUB")), Times.Never, "No connection string");

            _persistenceQueueMock.Verify(s => s.EnqueueCandle(It.Is<ICandle>(c => c.AssetPairId == "EURRUB")), Times.Never, "No connection string");
        }

        #endregion
        
    }
}
