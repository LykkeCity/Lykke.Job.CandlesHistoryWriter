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

        #region CreateUpdate

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

        #endregion

        #region Get

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

        #endregion

        #region Delete

        public async Task DeleteAsync(CandlePriceType priceType, CandleTimeInterval interval, DateTime fromIncluding, DateTime toNotIncluding)
        {
            // Candles are stored in rows (each of which is represented by CandlesHistoryEntity) grouping them by the bigger time interval.
            // Thus, second candles are stored in rows containing all the candles for the given minute. Minute candles are grouped in the
            // row with all the minute candles for the same hour. Hours - in day rows. Days - in months. Weeks and months itself - in years.
            // So, first of all, we need to adjust the given date\time limits to the corresponding row time period. It will enable us to
            // iterate the rows in table for the given time interval and make a decision: if we can delete the whole row, or only some of
            // its candles and the update the row.

            var partitionKey = CandleHistoryEntity.GeneratePartitionKey(priceType);
            // The first-going row key and date:
            var rowKey = CandleHistoryEntity.GenerateRowKey(fromIncluding, interval);
            var rowKeyDate = DateTime.Parse(rowKey);
            // For future use:
            DateTime nextRowKeyDate;

            while (rowKeyDate < toNotIncluding)
            {
                // On every iteration we have the current row's begining date\time and the next row's begining date\time.
                switch (interval)
                {
                    case CandleTimeInterval.Sec:
                        nextRowKeyDate = rowKeyDate.AddMinutes(1);
                        break;

                    case CandleTimeInterval.Minute:
                        nextRowKeyDate = rowKeyDate.AddHours(1);
                        break;

                    case CandleTimeInterval.Hour:
                        nextRowKeyDate = rowKeyDate.AddDays(1);
                        break;

                    case CandleTimeInterval.Day:
                        nextRowKeyDate = rowKeyDate.AddMonths(1);
                        break;

                    default: // Week or Month
                        nextRowKeyDate = rowKeyDate.AddYears(1);
                        break;
                }

                // Delete the whole row if it is inside [fromIncluding; toNotIncluding).
                if (rowKeyDate >= fromIncluding &&
                    nextRowKeyDate < toNotIncluding)
                    await _tableStorage.DeleteIfExistAsync(partitionKey, rowKey); 
                else
                {
                    // Otherwise, we need to make a decision about every Entity: which candles are to be deleted.
                    var entity = await _tableStorage.GetDataAsync(partitionKey, rowKey);
                    if (entity.Candles.Any())
                    {
                        entity.Candles.RemoveAll(c =>
                            c.LastUpdateTimestamp.TruncateTo(interval) >= fromIncluding &&
                            c.LastUpdateTimestamp.TruncateTo(interval) < toNotIncluding);

                        // If there are still any candles, we update the entity.
                        if (entity.Candles.Any())
                            await _tableStorage.InsertOrReplaceAsync(entity);
                        else
                            await _tableStorage.DeleteIfExistAsync(partitionKey, rowKey); // Otherwise, remove the entity to avoid it remaining empty in storage.
                    }
                }

                // For the next iteration:
                rowKeyDate = nextRowKeyDate;
                rowKey = CandleHistoryEntity.GenerateRowKey(rowKeyDate, interval);
            }
        }

        #endregion

        #region Private

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

        #endregion
    }
}
