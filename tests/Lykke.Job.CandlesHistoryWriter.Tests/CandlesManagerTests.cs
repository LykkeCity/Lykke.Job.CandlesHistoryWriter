using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using Lykke.Job.CandlesProducer.Contract;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.Candles;
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

        private Mock<ICandlesCacheService> _cacheServiceMock;
        private ICandlesManager _manager;

        private Mock<ICandlesPersistenceQueue> _persistenceQueueMock;

        [TestInitialize]
        public void InitializeTest()
        {
            _cacheServiceMock = new Mock<ICandlesCacheService>();
            _persistenceQueueMock = new Mock<ICandlesPersistenceQueue>();

            _manager = new CandlesManager(
                _cacheServiceMock.Object,
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
            await _manager.ProcessCandlesAsync(new []{quote1, quote3});

            // Assert
            _cacheServiceMock.Verify(s => s.CacheAsync(It.Is<IReadOnlyList<ICandle>>(candles => 
                candles.Count(c => c.AssetPairId == "EURUSD") == 1 &&
                candles.Count(c => c.AssetPairId == "EURRUB") == 1)), Times.Once);
            _persistenceQueueMock.Verify(s => s.EnqueueCandle(It.Is<ICandle>(c => c.AssetPairId == "EURUSD")), Times.Once);
            _persistenceQueueMock.Verify(s => s.EnqueueCandle(It.Is<ICandle>(c => c.AssetPairId == "EURRUB")), Times.Once);

            _cacheServiceMock.Verify(s => s.CacheAsync(It.IsAny<IReadOnlyList<ICandle>>()), Times.Once);
            _persistenceQueueMock.Verify(s => s.EnqueueCandle(It.IsAny<ICandle>()), Times.Exactly(2));
        }

        #endregion
        
    }
}
