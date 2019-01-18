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
        private const int MaxEmptyIntervalsCount = 20;

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
            var alignedToDate = to.TruncateTo(timeInterval).AddIntervalTicks(1, timeInterval);
            var alignedFromDate = alignedToDate.AddIntervalTicks(-candlesCount * 2 - 1, timeInterval);
            
            var repo = GetRepo(assetPairId, timeInterval);
            var candleInterval = alignedToDate - alignedFromDate;
            int emptyIntervals = 0;
            
            int processedInvervals = 1;
            
            do
            {
                var candles = (await repo.GetCandlesAsync(priceType, timeInterval, alignedFromDate, alignedToDate)).ToList();
                
                if (candles.Count > candlesCount * 3)
                {
                    Console.WriteLine($"{priceType} {timeInterval} {assetPairId}: Got {candles.Count} candles! (period from {alignedFromDate} to {alignedToDate}");
                }

                if (candles.Any())
                {
                    emptyIntervals = 0;
                    candlesToCache.InsertRange(0, candles);

                    if (candlesToCache.Count >= candlesCount)
                    {
                        if (processedInvervals > 1)
                            Console.WriteLine($"{priceType} {timeInterval} {assetPairId}: {processedInvervals} invervals prccessed");
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

                if (alignedFromDate < _minDate || emptyIntervals > MaxEmptyIntervalsCount)
                {
                    if (emptyIntervals > MaxEmptyIntervalsCount)
                        Console.WriteLine($"{priceType} {timeInterval} {assetPairId}: {MaxEmptyIntervalsCount} empty intervals reached");

                    break;
                }
                
                alignedToDate = alignedFromDate;
                var maxIntervals = GetMaxIntervalsCount(timeInterval);
                var needIntervals = candles.Any()
                    ? candlesCount / candles.Count
                    : maxIntervals;
                
                if (needIntervals > maxIntervals)
                {
                    needIntervals = maxIntervals;
                }

                try
                {
                    alignedFromDate = alignedToDate.AddMilliseconds(-(candleInterval * needIntervals * (emptyIntervals + 1)).TotalMilliseconds).TruncateTo(timeInterval);
                }
                catch (ArgumentOutOfRangeException)
                {
                    alignedFromDate = alignedToDate.AddIntervalTicks(-needIntervals, timeInterval);
                }

                if (alignedFromDate < _minDate)
                    alignedToDate = _minDate;

                processedInvervals++;
            } while (true);

            return candlesToCache;
        }

        private int GetMaxIntervalsCount(CandleTimeInterval interval)
        {
            switch (interval)
            {
                case CandleTimeInterval.Sec:
                    return 50;
                case CandleTimeInterval.Minute:
                case CandleTimeInterval.Min5:
                case CandleTimeInterval.Min15:
                case CandleTimeInterval.Min30:
                    return 10;
                case CandleTimeInterval.Hour:
                case CandleTimeInterval.Hour4:
                case CandleTimeInterval.Hour6:
                case CandleTimeInterval.Hour12:
                case CandleTimeInterval.Day:
                case CandleTimeInterval.Week:
                case CandleTimeInterval.Month:
                    return 1;
                default:
                    throw new ArgumentOutOfRangeException(nameof(interval), interval, null);
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
