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

        public async Task<bool> DeleteCandlesAsync(string assetPairId, CandleTimeInterval interval, CandlePriceType priceType, DateTime? fromIncluding = null, DateTime? toNotIncluding = null)
        {
            var repo = GetRepo(assetPairId, interval);
            try
            {
                var firstCandle = await repo.TryGetFirstCandleAsync(priceType, interval);
                // Return if there are no candles.
                if (firstCandle == null)
                    return false;

                var realUpperLimit = toNotIncluding ?? DateTime.UtcNow.TruncateTo(interval).AddIntervalTicks(1, interval); // For being able to delete all the history till UtcNow.

                // Also return if there are no candles before toNotIncluding.
                if (realUpperLimit <= firstCandle.Timestamp)
                    return false;

                DateTime realLowLimit;

                if (fromIncluding == null || fromIncluding < firstCandle.Timestamp)
                    realLowLimit = firstCandle.Timestamp;
                else realLowLimit = fromIncluding.Value;

                // And the last check
                if (realLowLimit >= realUpperLimit)
                    return false;

                // Well, here we go)
                await repo.DeleteAsync(priceType, interval, realLowLimit, realUpperLimit);

                return true;
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

        public async Task<int> DeleteCandlesAsync(IReadOnlyList<ICandle> candlesToDelete)
        {
            // ReSharper disable once PossibleMultipleEnumeration
            (var assetPairId, var interval, var priceType) = PreEvaluateInputCandleSet(candlesToDelete);

            var repo = GetRepo(assetPairId, interval);
            try
            {
                return 
                    // ReSharper disable once PossibleMultipleEnumeration
                    await repo.DeleteCandlesAsync(candlesToDelete, priceType);
            }
            catch
            {
                ResetRepo(assetPairId, interval);
                throw;
            }
        }

        public async Task<int> ReplaceCandlesAsync(IReadOnlyList<ICandle> candlesToReplace)
        {
            // ReSharper disable once PossibleMultipleEnumeration
            (var assetPairId, var interval, var priceType) = PreEvaluateInputCandleSet(candlesToReplace);

            var repo = GetRepo(assetPairId, interval);
            try
            {
                return 
                    // ReSharper disable once PossibleMultipleEnumeration
                    await repo.ReplaceCandlesAsync(candlesToReplace, priceType);
            }
            catch
            {
                ResetRepo(assetPairId, interval);
                throw;
            }
        }

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

        private void ResetRepo(string assetPairId, CandleTimeInterval interval)
        {
            var tableName = interval.ToString().ToLowerInvariant();
            var key = assetPairId + "_" + tableName;

            _assetPairRepositories[key] = null;
        }

        private AssetPairCandlesHistoryRepository GetRepo(string assetPairId, CandleTimeInterval timeInterval)
        {
            var tableName = timeInterval.ToString().ToLowerInvariant();
            var key = $"{assetPairId}_{tableName}";

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
