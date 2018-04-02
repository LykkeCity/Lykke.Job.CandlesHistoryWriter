using System;
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
using Lykke.Job.CandlesHistoryWriter.Core.Services.Assets;

namespace Lykke.Job.CandlesHistoryWriter.Services.HistoryMigration
{
    [UsedImplicitly]
    public class TradesMigrationManager
    {
        private readonly IAssetPairsManager _assetPairsManager;
        private readonly ICandlesHistoryRepository _candlesHistoryRepository;
        private readonly ILog _log;

        private readonly string _sqlConnString;
        private readonly int _sqlQueryBatchSize;

        public TradesMigrationHealthReport Health;

        public bool MigrationEnabled { get; }

        public TradesMigrationManager(
            IAssetPairsManager assetPairsManager,
            ICandlesHistoryRepository candlesHistoryRepository,
            ILog log,
            string sqlConnString,
            int sqlQueryBatchSize,
            bool migrationEnabled
            )
        {
            _assetPairsManager = assetPairsManager;
            _candlesHistoryRepository = candlesHistoryRepository;
            _log = log;

            _sqlConnString = sqlConnString;
            _sqlQueryBatchSize = sqlQueryBatchSize;

            Health = null;

            MigrationEnabled = migrationEnabled;
        }

        public bool Migrate(ITradesMigrationRequest request)
        {
            if (!MigrationEnabled)
                return false;

            // We should not run migration multiple times before the first attempt ends.
            if (Health != null && Health.State == TradesMigrationState.InProgress)
                return false;

            Task.Run(() => DoMigrateAsync(request).GetAwaiter().GetResult());
            return true;
        }

        private async Task DoMigrateAsync(ITradesMigrationRequest request)
        {
            Health = new TradesMigrationHealthReport(_sqlQueryBatchSize);

            foreach (var migrationItem in request.MigrationItems)
            {
                // First of all, we will check if we can store the requested asset pair via this instance of the job.
                var storedAssetPair = await _assetPairsManager.TryGetEnabledPairAsync(migrationItem.AssetId);
                if (storedAssetPair == null)
                {
                    await _log.WriteInfoAsync(nameof(TradesMigrationManager), nameof(DoMigrateAsync),
                        $"Asset pair {migrationItem.AssetId} is not currently enabled. Skipping.");
                    continue;
                }

                // The real asset ID + opposite asset ID:
                var assetSearchToken = storedAssetPair.BaseAssetId + storedAssetPair.QuotingAssetId;

                Health.AssetReportItems[migrationItem.AssetId] = new TradesMigrationHealthReportItem(migrationItem.OffsetFromTop);

                using (var sqlRepo = new TradesSqlHistoryRepository(_sqlConnString, _sqlQueryBatchSize, _log,
                    migrationItem.OffsetFromTop, assetSearchToken))
                {
                    await _log.WriteInfoAsync(nameof(TradesMigrationManager), nameof(DoMigrateAsync),
                        $"Starting migration for {migrationItem.AssetId}, except firts {migrationItem.OffsetFromTop} records.");

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
                            foreach (var interval in Constants.StoredIntervals)
                            {
                                // It's important for Constants.StoredIntervals to be ordered by time period increase,
                                // because we will calculate candles for each period based on previous period candles.
                                var currentCandles = interval == CandleTimeInterval.Sec
                                    ? new TradesCandleBatch(migrationItem.AssetId, interval, tradesBatch)
                                    : new TradesCandleBatch(migrationItem.AssetId, interval, smallerCandles);

                                ExtendStoredCandles(ref currentCandles);

                                Health.AssetReportItems[migrationItem.AssetId].SummarySavedCandles +=
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

                            Health.AssetReportItems[migrationItem.AssetId].SummaryFetchedTrades += batchCount;

                            Task.WaitAll(batchInsertTasks.ToArray());

                            await _log.WriteInfoAsync(nameof(TradesMigrationManager), nameof(DoMigrateAsync),
                                $"Batch of {batchCount} records for {migrationItem.AssetId} processed.");
                        }

                        await _log.WriteInfoAsync(nameof(TradesMigrationManager), nameof(DoMigrateAsync),
                            $"Migration for {migrationItem.AssetId} finished. Total records migrated: {Health.AssetReportItems[migrationItem.AssetId].SummaryFetchedTrades}, total candles stored: {Health.AssetReportItems[migrationItem.AssetId].SummarySavedCandles}.");
                    }
                    catch (Exception ex)
                    {
                        Health.State = TradesMigrationState.Error;
                        await _log.WriteErrorAsync(nameof(TradesMigrationManager), nameof(DoMigrateAsync), ex);
                        await _log.WriteInfoAsync(nameof(TradesMigrationManager), nameof(DoMigrateAsync),
                            $"Migration for {migrationItem.AssetId} interrupted due to error. Please, check error log for details.");
                        return;
                    }
                }
            }

            Health.State = TradesMigrationState.Finished;
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
