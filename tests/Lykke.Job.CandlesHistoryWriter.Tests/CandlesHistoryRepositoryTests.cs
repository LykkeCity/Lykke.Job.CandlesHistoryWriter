using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using AzureStorage.Tables;
using Lykke.Job.CandleHistoryWriter.Repositories.Candles;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.Candles;
using Lykke.Job.CandlesHistoryWriter.Core.Services;
using Lykke.Job.CandlesProducer.Contract;
using Lykke.Logs;
using Lykke.SettingsReader.ReloadingManager;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;

namespace Lykke.Job.CandlesHistoryWriter.Tests
{
    [TestClass]
    public class CandlesHistoryRepositoryTests
    {
        private CandlesHistoryRepository _repository;
        private AssetPairCandlesHistoryRepository _assetPairCandleHistoryRepository;

        [TestInitialize]
        public void InitializeTest()
        {
            var health = new Mock<IHealthService>().Object;
            _assetPairCandleHistoryRepository = new AssetPairCandlesHistoryRepository(
                health,
                EmptyLogFactory.Instance,
                "BTCUSD",
                CandleTimeInterval.Minute,
                new NoSqlTableInMemory<CandleHistoryEntity>()
            );
            
            _repository = new CandlesHistoryRepository(
                health, 
                EmptyLogFactory.Instance,
                ConstantReloadingManager.From(new Dictionary<string, string>{{"BTCUSD", "UseDevelopmentStorage=True"}}),
                new ConcurrentDictionary<string, AssetPairCandlesHistoryRepository>(new Dictionary<string, AssetPairCandlesHistoryRepository>
                {
                    {"BTCUSD_minute", _assetPairCandleHistoryRepository}
                }), DateTime.MinValue);
        }

        [TestMethod]
        public async Task Test()
        {
            const string assetPairId = "BTCUSD";
            var now = new DateTime(2018, 12, 15, 10, 0, 0);
            
            //candles with gaps
            var candles = new List<ICandle>
            {
                Candle.Create(assetPairId, CandlePriceType.Ask, CandleTimeInterval.Minute, now.AddMinutes(-1), 1, 2, 2, 1, 1, 1, 1, now.AddMinutes(-1)),
                Candle.Create(assetPairId, CandlePriceType.Ask, CandleTimeInterval.Minute, now.AddMinutes(-4), 1, 2, 2, 1, 1, 1, 1, now.AddMinutes(-4)),
                Candle.Create(assetPairId, CandlePriceType.Ask, CandleTimeInterval.Minute, now.AddMinutes(-8), 1, 2, 2, 1, 1, 1, 1, now.AddMinutes(-8)),
                Candle.Create(assetPairId, CandlePriceType.Ask, CandleTimeInterval.Minute, now.AddMinutes(-20), 1, 2, 2, 1, 1, 1, 1, now.AddMinutes(-20)),
                Candle.Create(assetPairId, CandlePriceType.Ask, CandleTimeInterval.Minute, now.AddMinutes(-30), 1, 2, 2, 1, 1, 1, 1, now.AddMinutes(-30)),
                Candle.Create(assetPairId, CandlePriceType.Ask, CandleTimeInterval.Minute, now.AddMinutes(-35), 1, 2, 2, 1, 1, 1, 1, now.AddMinutes(-35)),
                Candle.Create(assetPairId, CandlePriceType.Ask, CandleTimeInterval.Minute, now.AddMinutes(-38), 1, 2, 2, 1, 1, 1, 1, now.AddMinutes(-38))
            };
            
            await _assetPairCandleHistoryRepository.InsertOrMergeAsync(candles, CandlePriceType.Ask);

            var data = await _repository.GetExactCandlesAsync(assetPairId, CandleTimeInterval.Minute, CandlePriceType.Ask, now, 3);
            
            Assert.AreEqual(3, data.Count());
            
            data = await _repository.GetExactCandlesAsync(assetPairId, CandleTimeInterval.Minute, CandlePriceType.Ask, now, 5);
            
            Assert.AreEqual(5, data.Count());
            
            data = await _repository.GetExactCandlesAsync(assetPairId, CandleTimeInterval.Minute, CandlePriceType.Ask, now, 7);
            
            Assert.AreEqual(7, data.Count());
        }
    }
}
