using System.Linq;
using System.Threading.Tasks;
using Common.Log;
using JetBrains.Annotations;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.Candles;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.HistoryMigration.HistoryProviders.TradesSQLHistory;
using Lykke.Job.CandlesHistoryWriter.Services.Candles;

namespace Lykke.Job.CandlesHistoryWriter.Services.HistoryMigration
{
    [UsedImplicitly]
    public class TradesMigrationManager
    {
        private readonly ITradesSqlHistoryRepository _sqlTradesSqlHistoryRepository;
        private readonly ICandlesHistoryRepository _candlesHistoryRepository;
        private readonly ILog _log;

        public TradesMigrationManager(
            ITradesSqlHistoryRepository sqlTradesSqlHistoryRepository,
            ICandlesHistoryRepository candlesHistoryRepository,
            ILog log
            )
        {
            _sqlTradesSqlHistoryRepository = sqlTradesSqlHistoryRepository;
            _candlesHistoryRepository = candlesHistoryRepository;
            _log = log;
        }

        public async Task MigrateAsync(ITradesMigrationRequest request)
        {
            foreach (var migrationItem in request.MigrationItems)
            {
                await _log.WriteMonitorAsync(nameof(TradesMigrationManager), nameof(MigrateAsync),
                    $"Starting migration for {migrationItem.AssetId}, except firts {migrationItem.OffsetFromTop} records.");

                await _sqlTradesSqlHistoryRepository.InitAsync(migrationItem.OffsetFromTop, migrationItem.AssetId);

                var totalTradesRecordsFetched = 0;
                var totalCandlesStored = 0;

                // It's important for Constants.StoredIntervals to be ordered by time period increase,
                // because we will calculate candles for each period based on previous period candles.
                // We can form the candles stripes set only once per asset pair ID because each stripe 
                // is implicitly cleaned up on the begining of every iteration.
                var candleStripes = (Constants.StoredIntervals
                    .Select(interval => new TradesCandleStripe(migrationItem.AssetId, interval))).ToList();

                while (true)
                {
                    var tradesBatch = await _sqlTradesSqlHistoryRepository.GetNextBatchAsync();
                    var batchCount = tradesBatch.Count();

                    if (batchCount == 0) break;

                    // Now we can build sequentally candle stripes for all the stored time intervals.
                    // To avoid looking through the whole trades batch several times, we derive the
                    // bigger time intervals from the smaller ones. For example, let's assume that we
                    // generate 1000 sec candles from our batch of 2381 trades. Than, for calculating
                    // minute candles we need to iterate only 1000 second candles, and this will give
                    // us 16 minute candles. Next, we iterate 16 minute candles to get 1 hour candle,
                    // and so on, instead of looking through 2381 trades for each time interval.
                    for (var i = 0; i < candleStripes.Count; i++)
                    {
                        if (i == 0) // The first-going time interval IS TO BE "Sec"
                            await candleStripes[i].MakeFromTrades(tradesBatch);
                        else // And all of other intervals MUST BE ordered ascending.
                            await candleStripes[i].DeriveFromSmallerIntervalAsync(candleStripes[i - 1]);

                        await _candlesHistoryRepository.InsertOrMergeAsync(candleStripes[i].Candles.Values, candleStripes[i].AssetId,
                            TradesCandleStripe.PriceType, candleStripes[i].TimeInterval);
                    }

                    totalTradesRecordsFetched += batchCount;

                    await _log.WriteMonitorAsync(nameof(TradesMigrationManager), nameof(MigrateAsync),
                        $"Batch of {batchCount} records for {migrationItem.AssetId} processed.");
                }

                await _log.WriteMonitorAsync(nameof(TradesMigrationManager), nameof(MigrateAsync),
                    $"Migration for {migrationItem.AssetId} finished. Total records migrated: {totalTradesRecordsFetched}, total candles stored: {totalCandlesStored}.");
            }
        }
    }
}
