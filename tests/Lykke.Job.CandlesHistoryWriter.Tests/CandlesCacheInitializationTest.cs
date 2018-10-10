using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using Common.Log;
using Lykke.Job.CandlesProducer.Contract;
using Lykke.Service.Assets.Client.Models;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.Candles;
using Lykke.Job.CandlesHistoryWriter.Core.Services;
using Lykke.Job.CandlesHistoryWriter.Core.Services.Assets;
using Lykke.Job.CandlesHistoryWriter.Core.Services.Candles;
using Lykke.Job.CandlesHistoryWriter.Services.Candles;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;

namespace Lykke.Job.CandlesHistoryWriter.Tests
{
    [TestClass]
    public class CandlesCacheInitializationTest
    {
        private static readonly ImmutableArray<CandleTimeInterval> StoredIntervals = ImmutableArray.Create
        (
            CandleTimeInterval.Sec,
            CandleTimeInterval.Minute,
            CandleTimeInterval.Hour,
            CandleTimeInterval.Day,
            CandleTimeInterval.Week,
            CandleTimeInterval.Month
        );

        private static readonly ImmutableArray<CandlePriceType> StoredPriceTypes = ImmutableArray.Create
        (
            CandlePriceType.Ask,
            CandlePriceType.Bid,
            CandlePriceType.Mid
        );

        private const int AmountOfCandlesToStore = 5;
        private const MarketType MarketType = Core.Domain.Candles.MarketType.Spot;

        private ICandlesCacheInitalizationService _service;
        private Mock<IClock> _dateTimeProviderMock;
        private Mock<ICandlesCacheService> _cacheServiceMock;
        private Mock<ICandlesHistoryRepository> _historyRepositoryMock;
        private Mock<IAssetPairsManager> _assetPairsManagerMock;
        private List<AssetPair> _assetPairs;

        [TestInitialize]
        public void InitializeTest()
        {
            var logMock = new Mock<ILog>();

            _dateTimeProviderMock = new Mock<IClock>();
            _cacheServiceMock = new Mock<ICandlesCacheService>();
            _historyRepositoryMock = new Mock<ICandlesHistoryRepository>();
            _assetPairsManagerMock = new Mock<IAssetPairsManager>();

            _assetPairs = new List<AssetPair>
            {
                new AssetPair {Id = "EURUSD", Accuracy = 3},
                new AssetPair {Id = "USDCHF", Accuracy = 2},
                new AssetPair {Id = "EURRUB", Accuracy = 2}
            };

            _assetPairsManagerMock
                .Setup(m => m.GetAllEnabledAsync())
                .ReturnsAsync(() => _assetPairs);
            _assetPairsManagerMock
                .Setup(m => m.TryGetEnabledPairAsync(It.IsAny<string>()))
                .ReturnsAsync((string assetPairId) => _assetPairs.SingleOrDefault(a => a.Id == assetPairId));

            _service = new CandlesCacheInitalizationService(
                new CandlesCacheSemaphore(), 
                logMock.Object,
                _assetPairsManagerMock.Object,
                _dateTimeProviderMock.Object,
                _cacheServiceMock.Object,
                _historyRepositoryMock.Object,
                AmountOfCandlesToStore,
                MarketType);
        }

        [TestMethod]
        public async Task Initialization_caches_each_asset_pairs_in_each_stored_interval_and_in_each_stored_price_type_from_persistent_storage()
        {
            // Arrange
            var now = new DateTime(2017, 06, 23, 15, 35, 20, DateTimeKind.Utc);

            _dateTimeProviderMock.SetupGet(p => p.UtcNow).Returns(now);
            _historyRepositoryMock
                .Setup(r => r.GetCandlesAsync(
                    It.IsAny<string>(), 
                    It.IsAny<CandleTimeInterval>(), 
                    It.IsAny<CandlePriceType>(), 
                    It.IsAny<DateTime>(), 
                    It.IsAny<DateTime>()))
                .ReturnsAsync((string a, CandleTimeInterval i, CandlePriceType p, DateTime f, DateTime t) => 
                    new[]
                    {
                        new TestCandle(),
                        new TestCandle()
                    });
            _historyRepositoryMock
                .Setup(r => r.CanStoreAssetPair(It.Is<string>(a => new[] {"EURUSD", "USDCHF"}.Contains(a))))
                .Returns<string>(a => true);

            // Act
            await _service.InitializeCacheAsync();

            // Assert
            foreach (var interval in StoredIntervals)
            {
                foreach (var priceType in StoredPriceTypes)
                {
                    foreach (var assetPairId in new[] { "EURUSD", "USDCHF" })
                    {
                        _historyRepositoryMock.Verify(r =>
                                r.GetCandlesAsync(
                                    It.Is<string>(a => a == assetPairId),
                                    It.Is<CandleTimeInterval>(i => i == interval),
                                    It.Is<CandlePriceType>(p => p == priceType),
                                    It.Is<DateTime>(d => d == now.TruncateTo(interval).AddIntervalTicks(-AmountOfCandlesToStore, interval)),
                                    It.Is<DateTime>(d => d == now.TruncateTo(interval).AddIntervalTicks(1, interval))),
                            Times.Once);

                        _cacheServiceMock.Verify(s =>
                                s.InitializeAsync(
                                    It.Is<string>(a => a == assetPairId),
                                    It.Is<CandlePriceType>(p => p == priceType),
                                    It.Is<CandleTimeInterval>(i => i == interval),
                                    It.Is<IReadOnlyCollection<ICandle>>(c => c.Count == 2),
                                    It.IsAny<SlotType>()),
                            Times.Once);
                    }
                }
            }

            _historyRepositoryMock.Verify(r =>
                    r.GetCandlesAsync(
                        It.Is<string>(a => !new[] { "EURUSD", "USDCHF" }.Contains(a)),
                        It.IsAny<CandleTimeInterval>(),
                        It.IsAny<CandlePriceType>(),
                        It.IsAny<DateTime>(),
                        It.IsAny<DateTime>()),
                Times.Never);

            _cacheServiceMock.Verify(s =>
                    s.InitializeAsync(
                        It.Is<string>(a => !new[] { "EURUSD", "USDCHF" }.Contains(a)),
                        It.IsAny<CandlePriceType>(),
                        It.IsAny<CandleTimeInterval>(),
                        It.IsAny<IReadOnlyCollection<ICandle>>(),
                        It.IsAny<SlotType>()),
                Times.Never);
        }
    }
}
