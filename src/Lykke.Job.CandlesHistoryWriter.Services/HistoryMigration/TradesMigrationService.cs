using Common.Log;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.Candles;
using Lykke.Job.CandlesHistoryWriter.Services.Candles;
using Lykke.Job.CandlesHistoryWriter.Core.Services.HistoryMigration;
using Lykke.Job.CandlesProducer.Contract;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Lykke.Job.CandleHistoryWriter.Repositories.HistoryMigration.HistoryProviders.TradesSQLHistory;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.HistoryMigration;
using System.Linq;

namespace Lykke.Job.CandlesHistoryWriter.Services.HistoryMigration
{
    public class TradesMigrationService : ITradesMigrationService
    {
        private readonly ICandlesHistoryRepository _candlesHistoryRepository;
        private readonly TradesMigrationHealthService _tradesMigrationHealthService;
        private readonly ILog _log;

        private readonly string _sqlConnString;
        private readonly int _sqlQueryBatchSize;
        
        public TradesMigrationService(
            ICandlesHistoryRepository candlesHistoryRepository,
            ILog log,
            string sqlConnString,
            int sqlQueryBatchSize)
        {
            _candlesHistoryRepository = candlesHistoryRepository ?? throw new ArgumentNullException("candlesHistoryRepository");
            _log = log ?? throw new ArgumentNullException("log");

            _sqlConnString = sqlConnString;
            _sqlQueryBatchSize = sqlQueryBatchSize;
        }

        /// <inheritdoc />
        public void RemoveTradesCandlesAsync(string assetPairId, DateTime removeByDate)
        {
            var removalTasks = new List<Task>();

            foreach (var interval in Candles.Constants.StoredIntervals)
                removalTasks.Add(_candlesHistoryRepository.DeleteCandlesAsync(assetPairId, interval, CandlePriceType.Trades, null, removeByDate)); // Remove from the oldest candles to removeByDate.

            Task.WaitAll(removalTasks.ToArray());
        }

        public async Task MigrateTradesCandlesAsync(DateTime migrateByDate, List<(string AssetPairId, string SearchToken, string ReverseSearchToken)> assetSearchTokens)
        {
            // Creating a blank health report
            //_tradesMigrationHealthService.Prepare(_sqlQueryBatchSize, preliminaryRemoval, removeByDate);

            foreach (var searchToken in assetSearchTokens)
            {
                using (var sqlRepo = new TradesSqlHistoryRepository(_sqlConnString, _sqlQueryBatchSize, _log,
                    migrateByDate, searchToken.AssetPairId, searchToken.SearchToken, searchToken.ReverseSearchToken))
                {
                    await _log.WriteInfoAsync(nameof(TradesMigrationManager), nameof(MigrateTradesCandlesAsync),
                        $"Starting trades migration for {searchToken.AssetPairId}.");

                    try
                    {
                        while (true)
                        {
                            var tradesBatch = await sqlRepo.GetNextBatchAsync();
                            var batchCount = tradesBatch.Count();

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

                                ExtendStoredCandles(ref currentCandles);

                                _tradesMigrationHealthService[searchToken.AssetPairId].SummarySavedCandles +=
                                    currentCandles.CandlesCount;

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

                            Task.WaitAll(batchInsertTasks.ToArray());

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
            }

            _tradesMigrationHealthService.State = TradesMigrationState.Finished;
        }

        private void ExtendStoredCandles(ref TradesCandleBatch current)
        {
            var storedCandles =
                _candlesHistoryRepository.GetCandlesAsync(current.AssetId, current.TimeInterval, TradesCandleBatch.PriceType, current.MinTimeStamp,
                    current.MaxTimeStamp.AddSeconds((int)current.TimeInterval)).GetAwaiter().GetResult().ToList();
            if (!storedCandles.Any())
                return;

            for (int i = 0; i < current.CandlesCount; i++)
            {
                var timestamp = current.Candles.Values.ElementAt(i).Timestamp;
                var stored = storedCandles.FirstOrDefault(s => s.Timestamp == timestamp);
                if (stored != null)
                {
                    current.Candles[timestamp.ToFileTimeUtc()] =
                        stored.ExtendBy(current.Candles[timestamp.ToFileTimeUtc()]);
                    storedCandles.Remove(stored);
                }
            }
        }
    }
}
