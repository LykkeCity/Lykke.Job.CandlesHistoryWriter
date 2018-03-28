using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using AzureStorage;
using AzureStorage.Tables;
using Common.Log;
using JetBrains.Annotations;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.Candles;
using Lykke.Job.CandlesHistoryWriter.Core.Services;
using Lykke.Job.CandlesProducer.Contract;
using Lykke.SettingsReader;

namespace Lykke.Job.CandleHistoryWriter.Repositories.Candles
{
    [UsedImplicitly(ImplicitUseKindFlags.InstantiatedNoFixedConstructorSignature)]
    public class CandlesHistoryRepository : ICandlesHistoryRepository
    {
        private readonly IHealthService _healthService;
        private readonly ILog _log;
        private readonly IReloadingManager<Dictionary<string, string>> _assetConnectionStrings;

        private readonly ConcurrentDictionary<string, AssetPairCandlesHistoryRepository> _assetPairRepositories;

        private Dictionary<string, string> _extremeCandlesContinuationTokens;

        public CandlesHistoryRepository(IHealthService healthService, ILog log, IReloadingManager<Dictionary<string, string>> assetConnectionStrings)
        {
            _healthService = healthService;
            _log = log;
            _assetConnectionStrings = assetConnectionStrings;

            _assetPairRepositories = new ConcurrentDictionary<string, AssetPairCandlesHistoryRepository>();
        }

        public bool CanStoreAssetPair(string assetPairId)
        {
            return _assetConnectionStrings.CurrentValue.ContainsKey(assetPairId);
        }

        /// <summary>
        /// Insert or merge candles. Assumed that all candles have the same AssetPairId, PriceType, Timeinterval
        /// </summary>
        public async Task InsertOrMergeAsync(IEnumerable<ICandle> candles, string assetPairId, CandlePriceType priceType, CandleTimeInterval timeInterval)
        {
            var repo = GetRepo(assetPairId, timeInterval);
            try
            {
                await repo.InsertOrMergeAsync(candles, priceType);
            }
            catch
            {
                ResetRepo(assetPairId, timeInterval);
                throw;
            }
        }

        /// <summary>
        /// Returns buy or sell candle values for the specified interval from the specified time range.
        /// </summary>
        public async Task<IEnumerable<ICandle>> GetCandlesAsync(string assetPairId, CandleTimeInterval interval, CandlePriceType priceType, DateTime from, DateTime to)
        {
            var repo = GetRepo(assetPairId, interval);
            try
            {
                return await repo.GetCandlesAsync(priceType, interval, from, to);
            }
            catch
            {
                ResetRepo(assetPairId, interval);
                throw;
            }
        }

        public async Task<ICandle> TryGetFirstCandleAsync(string assetPairId, CandleTimeInterval interval, CandlePriceType priceType)
        {
            var repo = GetRepo(assetPairId, interval);
            try
            {
                return await repo.TryGetFirstCandleAsync(priceType, interval);
            }
            catch
            {
                ResetRepo(assetPairId, interval);
                throw;
            }
        }

        public async Task<int> DeleteCandlesAsync(IEnumerable<ICandle> candlesToDelete)
        {
            CheckupInputCandleSet(candlesToDelete, out var assetPairId, out var interval, out var priceType);

            var repo = GetRepo(assetPairId, interval);
            try
            {
                return 
                    await repo.DeleteCandlesAsync(candlesToDelete, priceType);
            }
            catch
            {
                ResetRepo(assetPairId, interval);
                throw;
            }
        }

        public async Task<int> ReplaceCandlesAsync(IEnumerable<ICandle> candlesToReplace)
        {
            CheckupInputCandleSet(candlesToReplace, out var assetPairId, out var interval, out var priceType);

            var repo = GetRepo(assetPairId, interval);
            try
            {
                return 
                    await repo.ReplaceCandlesAsync(candlesToReplace, priceType);
            }
            catch
            {
                ResetRepo(assetPairId, interval);
                throw;
            }
        }

        private void CheckupInputCandleSet(
            IEnumerable<ICandle> candlesToCheck, 
            out string assetPairId,
            out CandleTimeInterval interval, 
            out CandlePriceType priceType)
        {
            var firstCandle = candlesToCheck?.FirstOrDefault();
            if (firstCandle == null)
                throw new ArgumentException("The input candle set is null or empty.");

            assetPairId = firstCandle.AssetPairId;
            interval = firstCandle.TimeInterval;
            priceType = firstCandle.PriceType;

            if (candlesToCheck.Any(c =>
                c.AssetPairId != firstCandle.AssetPairId ||
                c.TimeInterval != firstCandle.TimeInterval ||
                c.PriceType != firstCandle.PriceType))
                throw new ArgumentException("The input set contains candles with different asset pair IDs, time intervals and/or price types.");
        }

        private void ResetRepo(string assetPairId, CandleTimeInterval interval)
        {
            var tableName = interval.ToString().ToLowerInvariant();
            var key = assetPairId.ToLowerInvariant() + "_" + tableName;

            _assetPairRepositories[key] = null;
        }

        private AssetPairCandlesHistoryRepository GetRepo(string assetPairId, CandleTimeInterval timeInterval)
        {
            var tableName = timeInterval.ToString().ToLowerInvariant();
            var key = $"{assetPairId.ToLowerInvariant()}_{tableName}";

            if (!_assetPairRepositories.TryGetValue(key, out AssetPairCandlesHistoryRepository repo) || repo == null)
            {
                return _assetPairRepositories.AddOrUpdate(
                    key: key,
                    addValueFactory: k => new AssetPairCandlesHistoryRepository(_healthService, _log, assetPairId, timeInterval, CreateStorage(assetPairId, tableName)),
                    updateValueFactory: (k, oldRepo) => oldRepo ?? new AssetPairCandlesHistoryRepository(_healthService, _log, assetPairId, timeInterval, CreateStorage(assetPairId, tableName)));
            }

            return repo;
        }

        private INoSQLTableStorage<CandleHistoryEntity> CreateStorage(string assetPairId, string tableName)
        {
            if (!_assetConnectionStrings.CurrentValue.TryGetValue(assetPairId, out var assetConnectionString) ||
                string.IsNullOrEmpty(assetConnectionString))
            {
                throw new ConfigurationException($"Connection string for asset pair '{assetPairId}' is not specified.");
            }

            var storage = AzureTableStorage<CandleHistoryEntity>.Create(
                _assetConnectionStrings.ConnectionString(x => x[assetPairId]), 
                tableName, 
                _log,
                maxExecutionTimeout: TimeSpan.FromMinutes(1),
                onGettingRetryCount: 10,
                onModificationRetryCount: 10,
                retryDelay: TimeSpan.FromSeconds(1));

            // Create and preload table info
            storage.GetDataAsync(assetPairId, "1900-01-01").Wait();

            return storage;
        }

        public IReadOnlyList<string> GetStoredAssetPairs()
        {
            return _assetConnectionStrings.CurrentValue.Keys.ToList();
        }
    }
}
