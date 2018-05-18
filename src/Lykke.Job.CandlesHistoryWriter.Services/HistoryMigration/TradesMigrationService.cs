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
using Lykke.Job.CandlesHistoryWriter.Core.Domain.HistoryMigration.HistoryProviders.TradesSQLHistory;

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
        private readonly int _candlesPersistenceQueueMaxSize;

        public TradesMigrationService(
            ICandlesHistoryRepository candlesHistoryRepository,
            TradesMigrationHealthService tradesMigrationHealthService,
            ILog log,
            string sqlConnString,
            int sqlQueryBatchSize,
            TimeSpan sqlTimeout,
            int candlesPersistenceQueueMaxSize
            )
        {
            _candlesHistoryRepository = candlesHistoryRepository ?? throw new ArgumentNullException(nameof(candlesHistoryRepository));
            _tradesMigrationHealthService = tradesMigrationHealthService ?? throw new ArgumentNullException(nameof(tradesMigrationHealthService));
            _log = log ?? throw new ArgumentNullException(nameof(log));

            _sqlConnString = sqlConnString;
            _sqlQueryBatchSize = sqlQueryBatchSize;
            _sqlTimeout = sqlTimeout;
            _candlesPersistenceQueueMaxSize = candlesPersistenceQueueMaxSize;
        }

        #region Public

        /// <inheritdoc cref="ITradesMigrationService"/>
        public async Task MigrateTradesCandlesAsync(DateTime? migrateByDate, List<(string AssetPairId, string SearchToken, string ReverseSearchToken)> assetSearchTokens)
        {
            foreach (var searchToken in assetSearchTokens)
            {
                var sqlRepo = new TradesSqlHistoryRepository(_sqlConnString, _sqlQueryBatchSize, _sqlTimeout, _log,
                    migrateByDate, searchToken.AssetPairId, searchToken.SearchToken);
                
                await _log.WriteInfoAsync(nameof(TradesMigrationManager), nameof(MigrateTradesCandlesAsync),
                    $"Starting trades migration for {searchToken.AssetPairId}.");

                try
                {
                    var historyMaker = new TradesProcessor(_tradesMigrationHealthService, _candlesHistoryRepository,
                        searchToken.AssetPairId, _candlesPersistenceQueueMaxSize);

                    while (true)
                    {
                        var tradesBatch = await sqlRepo.GetNextBatchAsync();
                        var batchCount = tradesBatch.Count;

                        if (batchCount == 0)
                            break;

                        await historyMaker.ProcessTradesBatch(tradesBatch);

                        await _log.WriteInfoAsync(nameof(TradesMigrationManager), nameof(MigrateTradesCandlesAsync),
                            $"Batch of {batchCount} records for {searchToken.AssetPairId} processed.");
                    }

                    await historyMaker.FlushCandlesIfAny();

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
        
        public class TradesProcessor
        {
            private readonly TradesMigrationHealthService _healthService;
            private readonly ICandlesHistoryRepository _historyRepo;
            private readonly Dictionary<CandleTimeInterval, ICandle> _activeCandles;
            private readonly string _assetPairId;
            private readonly int _persistenceQueueMaxSize;

            // ReSharper disable once MemberCanBePrivate.Global
            public readonly Dictionary<CandleTimeInterval, List<ICandle>> PersistenceCandleQueue; // It's better to make it private, but we need to unit test the class.

            public TradesProcessor(TradesMigrationHealthService healthService, ICandlesHistoryRepository historyRepo, string assetPairId, int persistenceQueueMaxSize)
            {
                _healthService = healthService ?? throw new ArgumentNullException(nameof(healthService));
                _historyRepo = historyRepo ?? throw new ArgumentNullException(nameof(_historyRepo));
                _assetPairId = !string.IsNullOrWhiteSpace(assetPairId)
                    ? assetPairId
                    : throw new ArgumentNullException(nameof(assetPairId));
                _persistenceQueueMaxSize = persistenceQueueMaxSize;

                PersistenceCandleQueue = new Dictionary<CandleTimeInterval, List<ICandle>>();
                foreach (var si in Candles.Constants.StoredIntervals)
                    PersistenceCandleQueue.Add(si, new List<ICandle>());

                _activeCandles = new Dictionary<CandleTimeInterval, ICandle>();
            }

            public async Task ProcessTradesBatch(IReadOnlyCollection<TradeHistoryItem> tradesBatch)
            {
                _healthService[_assetPairId].SummaryFetchedTrades += tradesBatch.Count;
                _healthService[_assetPairId].CurrentTradeBatchBegining = tradesBatch.First().DateTime;
                _healthService[_assetPairId].CurrentTradeBatchEnding = tradesBatch.Last().DateTime;

                foreach (var trade in tradesBatch)
                {
                    var volumeMultiplier = 1.0M / Math.Max(tradesBatch.Count(t => t.TradeId == trade.TradeId), 1.0M);

                    await ProcessTrade(trade, volumeMultiplier);
                }

                // If there still remain any active candles which have not been added to persistent queue till this moment.
                foreach (var interval in Candles.Constants.StoredIntervals)
                {
                    if (!_activeCandles.TryGetValue(interval, out var unsavedActiveCandle)) continue;

                    if (PersistenceCandleQueue[interval].All(c => c.Timestamp != unsavedActiveCandle.Timestamp))
                        PersistenceCandleQueue[interval].Add(unsavedActiveCandle);
                }

                _healthService.Health.PersistenceQueueSize = PersistenceQueueSize;
            }

            public async Task FlushCandlesIfAny()
            {
                foreach (var interval in Candles.Constants.StoredIntervals)
                {
                    

                    if (!PersistenceCandleQueue[interval].Any()) continue;

                    await _historyRepo.ReplaceCandlesAsync(PersistenceCandleQueue[interval]);

                    _healthService[_assetPairId].SummarySavedCandles +=
                        PersistenceCandleQueue[interval].Count;

                    PersistenceCandleQueue[interval].Clear(); // Protection against multiple calls
                }

                _healthService.Health.PersistenceQueueSize = 0;
            }

            private async Task ProcessTrade(TradeHistoryItem trade, decimal volumeMultiplier)
            {
                foreach (var interval in Candles.Constants.StoredIntervals)
                {
                    if (_activeCandles.TryGetValue(interval, out var activeCandle))
                    {
                        if (trade.BelongsTo(activeCandle))
                        {
                            _activeCandles[interval] = activeCandle.ExtendBy(trade, volumeMultiplier);
                        }
                        else
                        {
                            PersistenceCandleQueue[interval].Add(activeCandle);

                            _activeCandles[interval] = trade.CreateCandle(_assetPairId,
                                CandlePriceType.Trades, interval, volumeMultiplier);
                        }
                    }
                    else
                    {
                        _activeCandles[interval] = trade.CreateCandle(_assetPairId,
                            CandlePriceType.Trades, interval, volumeMultiplier);
                        continue;
                    }

                    if (PersistenceCandleQueue[interval].Count < _persistenceQueueMaxSize) continue;

                    await _historyRepo.ReplaceCandlesAsync(PersistenceCandleQueue[interval]);

                    _healthService[_assetPairId].SummarySavedCandles +=
                        PersistenceCandleQueue[interval].Count;

                    PersistenceCandleQueue[interval] = new List<ICandle>();
                }

                _healthService.Health.PersistenceQueueSize = PersistenceQueueSize;
            }

            private int PersistenceQueueSize
            {
                get
                {
                    var count = 0;
                    foreach (var list in PersistenceCandleQueue.Values)
                        count += list.Count;

                    return count;
                }
            }
        }

        #endregion

        #region Private

        #endregion
    }
}
