﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Common.Log;
using JetBrains.Annotations;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.Candles;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.HistoryMigration.HistoryProviders.TradesSQLHistory;
using Lykke.Job.CandleHistoryWriter.Repositories.HistoryMigration.HistoryProviders.TradesSQLHistory;
using Lykke.Job.CandlesProducer.Contract;
using Constants = Lykke.Job.CandlesHistoryWriter.Services.Candles.Constants;

namespace Lykke.Job.CandlesHistoryWriter.Services.HistoryMigration
{
    [UsedImplicitly]
    public class TradesMigrationManager
    {
        private readonly ICandlesHistoryRepository _candlesHistoryRepository;
        private readonly ILog _log;

        private readonly string _sqlConnString;
        private readonly int _sqlQueryBatchSize;

        public TradesMigrationHealthReport Health;

        public TradesMigrationManager(
            ICandlesHistoryRepository candlesHistoryRepository,
            ILog log,
            string sqlConnString,
            int sqlQueryBatchSize
            )
        {
            _candlesHistoryRepository = candlesHistoryRepository;
            _log = log;

            _sqlConnString = sqlConnString;
            _sqlQueryBatchSize = sqlQueryBatchSize;

            Health = null;
        }

        public void Migrate(ITradesMigrationRequest request)
        {
            Task.Run(() => DoMigrateAsync(request).Wait());
        }

        private async Task DoMigrateAsync(ITradesMigrationRequest request)
        {
            Health = new TradesMigrationHealthReport(_sqlQueryBatchSize);

            foreach (var migrationItem in request.MigrationItems)
            {
                Health.AssetReportItems[migrationItem.AssetId] = new TradesMigrationHealthReportItem(migrationItem.OffsetFromTop);

                using (var sqlRepo = new TradesSqlHistoryRepository(_sqlConnString, _sqlQueryBatchSize, _log,
                    migrationItem.OffsetFromTop, migrationItem.AssetId))
                {
                    await _log.WriteInfoAsync(nameof(TradesMigrationManager), nameof(Migrate),
                        $"Starting migration for {migrationItem.AssetId}, except firts {migrationItem.OffsetFromTop} records.");

                    while (true)
                    {
                        var tradesBatch = await sqlRepo.GetNextBatchAsync();
                        var batchCount = tradesBatch.Count();

                        if (batchCount == 0) break;

                        TradesCandleBatch smallerCandles = null;

                        var batchInsertTasks = new List<Task>();

                        // Now we can build sequentally candle batches for all the stored time intervals.
                        // To avoid looking through the whole trades batch several times, we derive the
                        // bigger time intervals from the smaller ones. For example, let's assume that we
                        // generate 1000 sec candles from our batch of 2381 trades. Then, for calculating
                        // minute candles we need to iterate only 1000 second candles, and this will give
                        // us 16 minute candles. Next, we iterate 16 minute candles to get 1 hour candle,
                        // and so on, instead of looking through 2381 trades for each time interval.
                        foreach (var interval in Constants.StoredIntervals)
                        {
                            // It's important for Constants.StoredIntervals to be ordered by time period increase,
                            // because we will calculate candles for each period based on previous period candles.
                            var currentCandles = interval == CandleTimeInterval.Sec
                                ? new TradesCandleBatch(migrationItem.AssetId, interval, tradesBatch)
                                : new TradesCandleBatch(migrationItem.AssetId, interval, smallerCandles);

                            Health.AssetReportItems[migrationItem.AssetId].SummarySavedCandles += currentCandles.CandlesCount;

                            smallerCandles = currentCandles;

                            // Preffered to perform inserting in parralel style.
                            batchInsertTasks.Add(
                                _candlesHistoryRepository.InsertOrMergeAsync(
                                    currentCandles.Candles.Values,
                                    currentCandles.AssetId,
                                    TradesCandleBatch.PriceType,
                                    currentCandles.TimeInterval));
                        }

                        Health.AssetReportItems[migrationItem.AssetId].SummaryFetchedTrades += batchCount;

                        Task.WaitAll(batchInsertTasks.ToArray());

                        await _log.WriteInfoAsync(nameof(TradesMigrationManager), nameof(Migrate),
                            $"Batch of {batchCount} records for {migrationItem.AssetId} processed.");
                    }

                    Health.State = TradesMigrationState.Finished;

                    await _log.WriteInfoAsync(nameof(TradesMigrationManager), nameof(Migrate),
                        $"Migration for {migrationItem.AssetId} finished. Total records migrated: {Health.AssetReportItems[migrationItem.AssetId].SummaryFetchedTrades}, total candles stored: {Health.AssetReportItems[migrationItem.AssetId].SummarySavedCandles}.");
                }
            }
        }


    }
}