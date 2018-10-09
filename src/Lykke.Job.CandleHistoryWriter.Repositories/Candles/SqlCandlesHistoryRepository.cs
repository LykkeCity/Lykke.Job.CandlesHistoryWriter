using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Common.Log;
using JetBrains.Annotations;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.Candles;
using Lykke.Job.CandlesHistoryWriter.Core.Services;
using Lykke.Job.CandlesProducer.Contract;
using Lykke.SettingsReader;

namespace Lykke.Job.CandleHistoryWriter.Repositories.Candles
{
    [UsedImplicitly(ImplicitUseKindFlags.InstantiatedNoFixedConstructorSignature)]
    public class SqlCandlesHistoryRepository : ICandlesHistoryRepository
    {
        private readonly IHealthService _healthService;
        private readonly ILog _log;
        private readonly IReloadingManager<string> _assetConnectionString;

        private readonly ConcurrentDictionary<string, SqlAssetPairCandlesHistoryRepository> _sqlAssetPairRepositories;

        public SqlCandlesHistoryRepository(IHealthService healthService, ILog log, 
            IReloadingManager<string> assetConnectionString)
        {
            _healthService = healthService;
            _log = log;
            _assetConnectionString = assetConnectionString;

            _sqlAssetPairRepositories = new ConcurrentDictionary<string, SqlAssetPairCandlesHistoryRepository>();
        }

        public async Task InsertOrMergeAsync(IEnumerable<ICandle> candles, string assetPairId, CandlePriceType priceType, CandleTimeInterval timeInterval)
        {
            var repo = GetRepo(assetPairId);

            await repo.InsertOrMergeAsync(candles);

        }

        public async Task<IEnumerable<ICandle>> GetCandlesAsync(string assetPairId, CandleTimeInterval interval,
            CandlePriceType priceType, DateTime from, DateTime to)
        {
            var repo = GetRepo(assetPairId);

            return await repo.GetCandlesAsync(priceType, interval, from, to);

        }

        public async Task<ICandle> TryGetFirstCandleAsync(string assetPairId, CandleTimeInterval interval, CandlePriceType priceType)
        {
            var repo = GetRepo(assetPairId);

            return await repo.TryGetFirstCandleAsync(priceType, interval);

        }

        public async Task<int> DeleteCandlesAsync(IReadOnlyList<ICandle> candlesToDelete)
        {
            // ReSharper disable once PossibleMultipleEnumeration
            var (assetPairId, interval, priceType) = PreEvaluateInputCandleSet(candlesToDelete);

            var repo = GetRepo(assetPairId);
            return
                // ReSharper disable once PossibleMultipleEnumeration
                await repo.DeleteCandlesAsync(candlesToDelete, priceType);
        }

        public async Task<int> ReplaceCandlesAsync(IReadOnlyList<ICandle> candlesToReplace)
        {
            // ReSharper disable once PossibleMultipleEnumeration
            var (assetPairId, interval, priceType) = PreEvaluateInputCandleSet(candlesToReplace);

            var repo = GetRepo(assetPairId);
            return
                // ReSharper disable once PossibleMultipleEnumeration
                await repo.ReplaceCandlesAsync(candlesToReplace, priceType);

        }

        private SqlAssetPairCandlesHistoryRepository GetRepo(string assetPairId) =>
            _sqlAssetPairRepositories.GetOrAdd(assetPairId,
                new SqlAssetPairCandlesHistoryRepository(assetPairId, _assetConnectionString.CurrentValue, _log));

        private (string assetPairId, CandleTimeInterval interval, CandlePriceType priceType) PreEvaluateInputCandleSet(
            IEnumerable<ICandle> candlesToCheck)
        {
            // ReSharper disable once PossibleMultipleEnumeration
            var firstCandle = candlesToCheck?.FirstOrDefault();
            if (firstCandle == null)
                throw new ArgumentException("The input candle set is null or empty.");

            var assetPairId = firstCandle.AssetPairId;
            var interval = firstCandle.TimeInterval;
            var priceType = firstCandle.PriceType;

            // ReSharper disable once PossibleMultipleEnumeration
            if (candlesToCheck.Any(c =>
                c.AssetPairId != firstCandle.AssetPairId ||
                c.TimeInterval != firstCandle.TimeInterval ||
                c.PriceType != firstCandle.PriceType))
                throw new ArgumentException("The input set contains candles with different asset pair IDs, time intervals and/or price types.");

            return (assetPairId: assetPairId,
                interval: interval,
                priceType: priceType);
        }
    }
}
