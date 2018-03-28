using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using AzureStorage;
using Common.Log;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.Candles;
using Lykke.Job.CandlesHistoryWriter.Core.Services;
using Lykke.Job.CandlesProducer.Contract;
using Microsoft.WindowsAzure.Storage.Table;
using MoreLinq;
using Polly;

namespace Lykke.Job.CandleHistoryWriter.Repositories.Candles
{
    internal sealed class AssetPairCandlesHistoryRepository
    {
        private readonly IHealthService _healthService;
        private readonly ILog _log;
        private readonly string _assetPairId;
        private readonly CandleTimeInterval _timeInterval;
        private readonly INoSQLTableStorage<CandleHistoryEntity> _tableStorage;

        public AssetPairCandlesHistoryRepository(
            IHealthService healthService,
            ILog log,
            string assetPairId,
            CandleTimeInterval timeInterval,
            INoSQLTableStorage<CandleHistoryEntity> tableStorage)
        {
            _healthService = healthService;
            _log = log;
            _assetPairId = assetPairId;
            _timeInterval = timeInterval;
            _tableStorage = tableStorage;
        }

        /// <summary>
        /// Assumed that all candles have the same AssetPair, PriceType, and Timeinterval
        /// </summary>
        public async Task InsertOrMergeAsync(IEnumerable<ICandle> candles, CandlePriceType priceType)
        {
            var partitionKey = CandleHistoryEntity.GeneratePartitionKey(priceType);

            // Despite of AzureTableStorage already split requests to chunks,
            // splits to the chunks here to reduse cost of operation timeout

            var candleByRowsChunks = candles
                .GroupBy(candle => CandleHistoryEntity.GenerateRowKey(candle.Timestamp, _timeInterval))
                .Batch(100);

            foreach (var candleByRowsChunk in candleByRowsChunks)
            {
                // If we can't store the candles, we can't do anything else, so just retries until success
                await Policy
                    .Handle<Exception>()
                    .WaitAndRetryForeverAsync(
                        retryAttempt => TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)),
                        (exception, timeSpan) =>
                        {
                            var context = $"{_assetPairId}-{priceType}-{_timeInterval}";

                            return _log.WriteErrorAsync("Persist candle rows chunk with retries", context, exception);
                        })
                    .ExecuteAsync(() => SaveCandlesBatchAsync(candleByRowsChunk, partitionKey));
            }
        }

        private async Task SaveCandlesBatchAsync(IEnumerable<IGrouping<string, ICandle>> candleByRowsChunk, string partitionKey)
        {
            var candleByRows = candleByRowsChunk.ToDictionary(g => g.Key, g => g.AsEnumerable());

            // updates existing entities

            var existingEntities = (await _tableStorage.GetDataAsync(partitionKey, candleByRows.Keys)).ToArray();

            foreach (var entity in existingEntities)
            {
                entity.MergeCandles(candleByRows[entity.RowKey], _assetPairId, _timeInterval);
            }

            // creates new entities

            var newEntityKeys = candleByRows.Keys.Except(existingEntities.Select(e => e.RowKey));
            var newEntities = newEntityKeys.Select(k => new CandleHistoryEntity(partitionKey, k)).ToArray();

            foreach (var entity in newEntities)
            {
                entity.MergeCandles(candleByRows[entity.RowKey], _assetPairId, _timeInterval);
            }

            // save changes

            _healthService.TraceCandleRowsPersisted(existingEntities.Length + newEntities.Length);

            await _tableStorage.InsertOrReplaceBatchAsync(existingEntities.Concat(newEntities));
        }

        public async Task<int> DeleteCandlesAsync(IEnumerable<ICandle> candlesToDelete, CandlePriceType priceType)
        {
            if (candlesToDelete == null || !candlesToDelete.Any())
                throw new ArgumentException("Candles set should not be empty.");

            var partitionKey = CandleHistoryEntity.GeneratePartitionKey(priceType);

            // Splitting to chunks, just like in InsertOrMergeAsync

            var candleByRowsChunks = candlesToDelete
                .GroupBy(candle => CandleHistoryEntity.GenerateRowKey(candle.Timestamp, _timeInterval))
                .Batch(100);

            int deletedCandlesCount = 0;

            foreach (var candleByRowsChunk in candleByRowsChunks)
            {
                var candleByRows = candleByRowsChunk.ToDictionary(g => g.Key, g => g.AsEnumerable());

                var existingEntities = (await _tableStorage.GetDataAsync(partitionKey, candleByRows.Keys)).ToArray();

                foreach (var entity in existingEntities)
                {
                    deletedCandlesCount += entity.DeleteCandles(candleByRows[entity.RowKey]);
                }

                // No _healthService trackig here. Monitoring of candles deletion is performed on upper layers of logic.

                await _tableStorage.InsertOrReplaceBatchAsync(existingEntities); // For we do not have a ReplaceBatchAsync method in AzureTableStorage yet.
            }

            return deletedCandlesCount;
        }

        public async Task<int> ReplaceCandlesAsync(IEnumerable<ICandle> candlesToReplace, CandlePriceType priceType)
        {
            if (candlesToReplace == null || !candlesToReplace.Any())
                throw new ArgumentException("Candles set should not be empty.");

            var partitionKey = CandleHistoryEntity.GeneratePartitionKey(priceType);

            // Splitting to chunks, just like in InsertOrMergeAsync

            var candleByRowsChunks = candlesToReplace
                .GroupBy(candle => CandleHistoryEntity.GenerateRowKey(candle.Timestamp, _timeInterval))
                .Batch(100);

            int replacedCandlesCount = 0;

            foreach (var candleByRowsChunk in candleByRowsChunks)
            {
                var candleByRows = candleByRowsChunk.ToDictionary(g => g.Key, g => g.AsEnumerable());

                var existingEntities = (await _tableStorage.GetDataAsync(partitionKey, candleByRows.Keys)).ToArray();

                foreach (var entity in existingEntities)
                {
                    replacedCandlesCount += entity.ReplaceCandles(candleByRows[entity.RowKey]);
                }

                // No _healthService trackig here. Monitoring of candles deletion is performed on upper layers of logic.

                await _tableStorage.InsertOrReplaceBatchAsync(existingEntities); // For we do not have a ReplaceBatchAsync method in AzureTableStorage yet.
            }

            return replacedCandlesCount;
        }

        public async Task<IEnumerable<ICandle>> GetCandlesAsync(CandlePriceType priceType, CandleTimeInterval interval, DateTime from, DateTime to)
        {
            if (priceType == CandlePriceType.Unspecified)
            {
                throw new ArgumentException(nameof(priceType));
            }

            var query = GetTableQuery(priceType, interval, from, to);
            var entities = await _tableStorage.WhereAsync(query);
            var candles = entities
                .SelectMany(e => e.Candles.Select(ci => ci.ToCandle(_assetPairId, e.PriceType, e.DateTime, interval)));

            return candles.Where(c => c.Timestamp >= from && c.Timestamp < to);
        }

        public async Task<ICandle> TryGetFirstCandleAsync(CandlePriceType priceType, CandleTimeInterval timeInterval)
        {
            var candleEntity = await _tableStorage.GetTopRecordAsync(CandleHistoryEntity.GeneratePartitionKey(priceType));

            return candleEntity
                ?.Candles
                .First()
                .ToCandle(_assetPairId, priceType, candleEntity.DateTime, timeInterval);
        }

        private static TableQuery<CandleHistoryEntity> GetTableQuery(
            CandlePriceType priceType,
            CandleTimeInterval interval,
            DateTime from,
            DateTime to)
        {
            var partitionKey = CandleHistoryEntity.GeneratePartitionKey(priceType);
            var rowKeyFrom = CandleHistoryEntity.GenerateRowKey(from, interval);
            var rowKeyTo = CandleHistoryEntity.GenerateRowKey(to, interval);

            var pkeyFilter = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionKey);

            var rowkeyFromFilter = TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.GreaterThanOrEqual, rowKeyFrom);
            var rowkeyToFilter = TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.LessThanOrEqual, rowKeyTo);
            var rowkeyFilter = TableQuery.CombineFilters(rowkeyFromFilter, TableOperators.And, rowkeyToFilter);

            return new TableQuery<CandleHistoryEntity>
            {
                FilterString = TableQuery.CombineFilters(pkeyFilter, TableOperators.And, rowkeyFilter)
            };
        }
    }
}
