using Common.Log;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.Candles;
using Lykke.Job.CandlesHistoryWriter.Core.Services.HistoryMigration;
using Lykke.Job.CandlesProducer.Contract;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Lykke.Job.CandleHistoryWriter.Repositories.HistoryMigration.HistoryProviders.TradesSQLHistory;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.HistoryMigration;
using System.Linq;
using JetBrains.Annotations;

namespace Lykke.Job.CandlesHistoryWriter.Services.HistoryMigration
{
    [UsedImplicitly]
    public class TradesMigrationService : ITradesMigrationService
    {
        private readonly ICandlesHistoryRepository _candlesHistoryRepository;
        private readonly TradesMigrationHealthService _tradesMigrationHealthService;
        private readonly ILog _log;

        private readonly string _sqlConnString;
        private readonly int _sqlQueryBatchSize;
        private readonly TimeSpan _sqlTimeout;

        public TradesMigrationService(
            ICandlesHistoryRepository candlesHistoryRepository,
            TradesMigrationHealthService tradesMigrationHealthService,
            ILog log,
            string sqlConnString,
            int sqlQueryBatchSize,
            TimeSpan sqlTimeout
            )
        {
            _candlesHistoryRepository = candlesHistoryRepository ?? throw new ArgumentNullException(nameof(candlesHistoryRepository));
            _tradesMigrationHealthService = tradesMigrationHealthService ?? throw new ArgumentNullException(nameof(tradesMigrationHealthService));
            _log = log ?? throw new ArgumentNullException(nameof(log));

            _sqlConnString = sqlConnString;
            _sqlQueryBatchSize = sqlQueryBatchSize;
            _sqlTimeout = sqlTimeout;
        }

        #region Public

        /// <inheritdoc cref="ITradesMigrationService"/>
        public async Task MigrateTradesCandlesAsync(bool preliminaryRemoval, DateTime? migrateByDate, List<(string AssetPairId, string SearchToken, string ReverseSearchToken)> assetSearchTokens)
        {
            foreach (var searchToken in assetSearchTokens)
            {
                var sqlRepo = new TradesSqlHistoryRepository(_sqlConnString, _sqlQueryBatchSize, _sqlTimeout, _log,
                    migrateByDate, searchToken.AssetPairId, searchToken.SearchToken);
                
                await _log.WriteInfoAsync(nameof(TradesMigrationManager), nameof(MigrateTradesCandlesAsync),
                    $"Starting trades migration for {searchToken.AssetPairId}.");

                try
                {
                    // Removing old candles first, is it is requested to do.
                    if (preliminaryRemoval)
                    {
                        await _log.WriteInfoAsync(nameof(TradesMigrationManager), nameof(MigrateTradesCandlesAsync),
                            $"Starting preliminary trade candles removal...");

                        await RemoveTradesCandlesAsync(searchToken.AssetPairId, migrateByDate);

                        await _log.WriteInfoAsync(nameof(TradesMigrationManager), nameof(MigrateTradesCandlesAsync),
                            migrateByDate.HasValue ? $"Trade candles are removed till {migrateByDate}" : "All trade candles are removed. Going to migrate...");
                    }

                    // And now migrate trades.
                    while (true)
                    {
                        var tradesBatch = await sqlRepo.GetNextBatchAsync();
                        var batchCount = tradesBatch.Count;

                        if (batchCount == 0)
                            break;

                        TradesCandleBatch smallerCandles = null;

                        var batchInsertTasks = new List<Task>();

                        // Now we can build sequentally candle batches for all the stored time intervals.
                        // To avoid looking through the whole trades batch several times, we derive the
                        // bigger time intervals from the smaller ones. For example, let's assume that we
                        // generate 1000 sec candles from our batch of 2381 trades. Then, for calculating
                        // minute candles we need to iterate only 1000 second candles, and this will give
                        // us 16 minute candles. Next, we iterate 16 minute candles to get 1 hour candle,
                        // and so on, instead of looking through 2381 trades for each time interval.
                        foreach (var interval in Candles.Constants.StoredIntervals)
                        {
                            // It's important for Constants.StoredIntervals to be ordered by time period increase,
                            // because we will calculate candles for each period based on previous period candles.
                            var currentCandles = interval == CandleTimeInterval.Sec
                                ? new TradesCandleBatch(searchToken.AssetPairId, interval, tradesBatch)
                                : new TradesCandleBatch(searchToken.AssetPairId, interval, smallerCandles);

                            ExtendStoredCandles(currentCandles);

                            _tradesMigrationHealthService[searchToken.AssetPairId].SummarySavedCandles +=
                                currentCandles.Candles.Count;

                            // We can not derive month candles from weeks for they may lay out of the borders
                            // of the month. While generating a month candles, we should use day candles as a
                            // data source instead.
                            if (interval != CandleTimeInterval.Week)
                                smallerCandles = currentCandles;

                            // Preffered to perform inserting in parralel style.
                            batchInsertTasks.Add(
                                _candlesHistoryRepository.InsertOrMergeAsync(
                                    currentCandles.Candles.Values,
                                    currentCandles.AssetId,
                                    TradesCandleBatch.PriceType,
                                    currentCandles.TimeInterval));
                        }

                        _tradesMigrationHealthService[searchToken.AssetPairId].SummaryFetchedTrades += batchCount;

                        await Task.WhenAll(batchInsertTasks.ToArray());

                        await _log.WriteInfoAsync(nameof(TradesMigrationManager), nameof(MigrateTradesCandlesAsync),
                            $"Batch of {batchCount} records for {searchToken.AssetPairId} processed.");
                    }

                    await _log.WriteInfoAsync(nameof(TradesMigrationManager), nameof(MigrateTradesCandlesAsync),
                        $"Migration for {searchToken.AssetPairId} finished. Total records migrated: {_tradesMigrationHealthService[searchToken.AssetPairId].SummaryFetchedTrades}, " +
                        $"total candles stored: {_tradesMigrationHealthService[searchToken.AssetPairId].SummarySavedCandles}.");
                }
                catch (Exception ex)
                {
                    _tradesMigrationHealthService.State = TradesMigrationState.Error;
                    await _log.WriteErrorAsync(nameof(TradesMigrationManager), nameof(MigrateTradesCandlesAsync), ex);
                    await _log.WriteInfoAsync(nameof(TradesMigrationManager), nameof(MigrateTradesCandlesAsync),
                        $"Migration for {searchToken.AssetPairId} interrupted due to error. Please, check error log for details.");
                    return;
                }
            }

            _tradesMigrationHealthService.State = TradesMigrationState.Finished;
        }

        #endregion

        #region Private

        /// <summary>
        /// Parallel trades candles removal from all the time interval tables for the specified asset pair.
        /// </summary>
        private Task RemoveTradesCandlesAsync(string assetPairId, DateTime? removeByDate)
        {
            var removalTasks = new List<Task>();

            foreach (var interval in Candles.Constants.StoredIntervals)
                removalTasks.Add(_candlesHistoryRepository.DeleteCandlesAsync(assetPairId, interval, CandlePriceType.Trades, null, removeByDate)); // Remove since the oldest candles till removeByDate.

            return Task.WhenAll(removalTasks);
        }

        /// <summary>
        /// Tries to load candles from the storage and then extends em by the given newly-calculated candles batch for the same time stamps.
        /// </summary>
        private void ExtendStoredCandles(TradesCandleBatch current)
        {
            var storedCandles =
                _candlesHistoryRepository.GetCandlesAsync(current.AssetId, current.TimeInterval, TradesCandleBatch.PriceType, current.MinTimeStamp,
                    current.MaxTimeStamp.AddSeconds((int)current.TimeInterval)).GetAwaiter().GetResult().ToList();
            if (!storedCandles.Any())
                return;

            foreach (var candle in current.Candles.Values)
            {
                var timestamp = candle.Timestamp;
                var stored = storedCandles.FirstOrDefault(s => s.Timestamp == timestamp);
                if (stored != null)
                {
                    current.Candles[timestamp] = stored.ExtendBy(candle);
                    storedCandles.Remove(stored);
                }
            }
        }

        #endregion
    }
}
