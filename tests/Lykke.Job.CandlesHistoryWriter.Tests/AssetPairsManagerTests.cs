using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Common.Log;
using Lykke.Service.Assets.Client;
using Lykke.Service.Assets.Client.Models;
using Lykke.Job.CandlesHistoryWriter.Core.Services.Assets;
using Lykke.Job.CandlesHistoryWriter.Services.Assets;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;

namespace Lykke.Job.CandlesHistoryWriter.Tests
{
    [TestClass]
    public class AssetPairsManagerTests
    {
        private IAssetPairsManager _manager;
        private Mock<IAssetsServiceWithCache> _assetsServiceMock;
        
        [TestInitialize]
        public void InitializeTest()
        {
            _assetsServiceMock = new Mock<IAssetsServiceWithCache>();

            _manager = new AssetPairsManager(new LogToMemory(), _assetsServiceMock.Object);
        }

        #region Getting enabled pair

        [TestMethod]
        public async Task Getting_enabled_pair_returns_enabled_pair()
        {
            // Arrange
            _assetsServiceMock
                .Setup(s => s.TryGetAssetPairAsync(It.Is<string>(a => a == "EURUSD"), It.IsAny<CancellationToken>()))
                .ReturnsAsync((string a, CancellationToken t) => new AssetPair { Id = a, IsDisabled = false });

            // Act
            var pair = await _manager.TryGetEnabledPairAsync("EURUSD");

            // Assert
            Assert.IsNotNull(pair);
            Assert.AreEqual("EURUSD", pair.Id);
        }

        [TestMethod]
        public async Task Getting_enabled_pair_not_returns_disabled_pair()
        {
            // Arrange
            _assetsServiceMock
                .Setup(s => s.TryGetAssetPairAsync(It.Is<string>(a => a == "EURUSD"), It.IsAny<CancellationToken>()))
                .ReturnsAsync((string a, CancellationToken t) => new AssetPair { Id = a, IsDisabled = true });

            // Act
            var pair = await _manager.TryGetEnabledPairAsync("EURUSD");

            // Assert
            Assert.IsNull(pair);
        }

        [TestMethod]
        public async Task Getting_enabled_pair_not_returns_missing_pair()
        {
            // Arrange
            _assetsServiceMock
                .Setup(s => s.TryGetAssetPairAsync(It.Is<string>(a => a == "EURUSD"), It.IsAny<CancellationToken>()))
                .ReturnsAsync((string a, CancellationToken t) => null);

            // Act
            var pair = await _manager.TryGetEnabledPairAsync("EURUSD");

            // Assert
            Assert.IsNull(pair);
        }

        #endregion


        #region Getting all enabled pairs

        [TestMethod]
        public async Task Getting_all_pairs_returns_empty_enumerable_if_no_enabled_pairs()
        {
            // Arrange
            _assetsServiceMock
                .Setup(s => s.GetAllAssetPairsAsync(It.IsAny<CancellationToken>()))
                .ReturnsAsync((CancellationToken t) => new[]
                {
                    new AssetPair {Id = "USDRUB", IsDisabled = true}
                });

            // Act
            var pairs = await _manager.GetAllEnabledAsync();

            // Assert
            Assert.IsNotNull(pairs);
            Assert.IsFalse(pairs.Any());
        }

        [TestMethod]
        public async Task Getting_all_pairs_returns_only_enabled_pairs()
        {
            // Arrange
            _assetsServiceMock
                .Setup(s => s.GetAllAssetPairsAsync(It.IsAny<CancellationToken>()))
                .ReturnsAsync((CancellationToken t) => new[]
                {
                    new AssetPair { Id = "EURUSD", IsDisabled = false },
                    new AssetPair { Id = "USDRUB", IsDisabled = true },
                    new AssetPair { Id = "USDCHF", IsDisabled = false }
                });

            // Act
            var pairs = (await _manager.GetAllEnabledAsync()).ToArray();

            // Assert
            Assert.AreEqual(2, pairs.Length);
            Assert.AreEqual("EURUSD", pairs[0].Id);
            Assert.AreEqual("USDCHF", pairs[1].Id);
        }

        #endregion
    }
}
