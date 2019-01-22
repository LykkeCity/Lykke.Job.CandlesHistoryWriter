using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using AzureStorage;
using AzureStorage.Tables;
using JetBrains.Annotations;
using Lykke.Common.Log;
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
        private readonly ILogFactory _logFactory;
        private readonly IReloadingManager<Dictionary<string, string>> _assetConnectionStrings;
        private readonly DateTime _minDate;
        private const int MaxEmptyIntervalsCount = 10;
        private const int MaxIntervalsCount = 20;

        private readonly ConcurrentDictionary<string, AssetPairCandlesHistoryRepository> _assetPairRepositories;

        public CandlesHistoryRepository(IHealthService healthService, ILogFactory logFactory, IReloadingManager<Dictionary<string, string>> assetConnectionStrings, DateTime minDate)
        {
            _healthService = healthService;
            _logFactory = logFactory;
            _assetConnectionStrings = assetConnectionStrings;
            _minDate = minDate;

            _assetPairRepositories = new ConcurrentDictionary<string, AssetPairCandlesHistoryRepository>();
        }
        
        public CandlesHistoryRepository(IHealthService healthService, ILogFactory logFactory, IReloadingManager<Dictionary<string, string>> assetConnectionStrings,
            ConcurrentDictionary<string, AssetPairCandlesHistoryRepository> repositories, DateTime minDate)
        {
            _healthService = healthService;
            _logFactory = logFactory;
            _assetConnectionStrings = assetConnectionStrings;

            _assetPairRepositories = repositories;
            _minDate = minDate;
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

        public async Task<IReadOnlyCollection<ICandle>> GetExactCandlesAsync(string assetPairId, CandleTimeInterval timeInterval, CandlePriceType priceType, DateTime to, int candlesCount)
        {
            var candlesToCache = new List<ICandle>();

            var intervalMultiplier = GetIntervalMultiplier(timeInterval);
            
            var alignedToDate = to.TruncateTo(timeInterval).AddIntervalTicks(1, timeInterval);
            var alignedFromDate = alignedToDate.AddIntervalTicks(-candlesCount * intervalMultiplier - 1, timeInterval);

            if (alignedFromDate < _minDate)
                alignedFromDate = _minDate;
            
            var repo = GetRepo(assetPairId, timeInterval);
            int emptyIntervals = 0;
            
            do
            {
                var candles = (await repo.GetCandlesAsync(priceType, timeInterval, alignedFromDate, alignedToDate)).ToList();
                
                if (candles.Any())
                {
                    emptyIntervals = 0;
                    candlesToCache.InsertRange(0, candles);

                    if (candlesToCache.Count >= candlesCount)
                    {
                        candlesToCache = candlesToCache
                            .Skip(candlesToCache.Count - candlesCount)
                            .ToList();

                        break;
                    }
                }
                else
                {
                    emptyIntervals++;
                }

                if (alignedFromDate <= _minDate || emptyIntervals > MaxEmptyIntervalsCount)
                    break;

                var maxIntervals = MaxIntervalsCount;
                var needIntervals = candles.Any()
                    ? candlesCount / candles.Count
                    : maxIntervals;
                
                if (needIntervals > maxIntervals)
                {
                    needIntervals = maxIntervals;
                }
                
                alignedToDate = alignedFromDate.AddIntervalTicks(1, timeInterval);

                try
                {
                    alignedFromDate = alignedToDate.AddIntervalTicks(-candlesCount * intervalMultiplier * needIntervals - 1, timeInterval);
                }
                catch (ArgumentOutOfRangeException)
                {
                    alignedFromDate = alignedToDate.AddIntervalTicks(-needIntervals, timeInterval);
                }

                if (alignedFromDate < _minDate)
                    alignedFromDate = _minDate;
            } while (true);

            return candlesToCache;
        }

        private int GetIntervalMultiplier(CandleTimeInterval timeInterval)
        {
            switch (timeInterval)
            {
                case CandleTimeInterval.Minute:
                case CandleTimeInterval.Hour:
                    return 2;
                case CandleTimeInterval.Day:
                case CandleTimeInterval.Week:
                case CandleTimeInterval.Month:
                    return 1;
                default:
                    throw new ArgumentOutOfRangeException(nameof(timeInterval), timeInterval, null);
            }
        }

        private int GetTotalIntervalsCount(TimeSpan period, CandleTimeInterval timeInterval)
        {
            switch (timeInterval)
            {
                case CandleTimeInterval.Minute:
                    return (int)period.TotalMinutes;
                case CandleTimeInterval.Hour:
                    return (int)period.TotalHours;
                case CandleTimeInterval.Day:
                    return (int)period.TotalDays;
                case CandleTimeInterval.Week:
                    return (int)period.TotalDays / 7;
                case CandleTimeInterval.Month:
                    return (int)period.TotalDays/30;
                default:
                    throw new ArgumentOutOfRangeException(nameof(timeInterval), timeInterval, null);
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
                    addValueFactory: k => new AssetPairCandlesHistoryRepository(_healthService, _logFactory, assetPairId, timeInterval, CreateStorage(assetPairId, tableName)),
                    updateValueFactory: (k, oldRepo) => oldRepo ?? new AssetPairCandlesHistoryRepository(_healthService, _logFactory, assetPairId, timeInterval, CreateStorage(assetPairId, tableName)));
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
                _logFactory,
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
