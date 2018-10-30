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
using Lykke.Common.Log;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.HistoryMigration.HistoryProviders.TradesSQLHistory;

namespace Lykke.Job.CandlesHistoryWriter.Services.HistoryMigration
{
    [UsedImplicitly]
    public class TradesMigrationService : ITradesMigrationService
    {
        private readonly ICandlesHistoryRepository _candlesHistoryRepository;
        private readonly TradesMigrationHealthService _tradesMigrationHealthService;
        private readonly ILogFactory _logFactory;
        private readonly IHealthNotifier _healthNotifier;
        private readonly ILog _log;

        private readonly string _sqlConnString;
        private readonly int _sqlQueryBatchSize;
        private readonly TimeSpan _sqlTimeout;
        private readonly int _candlesPersistenceQueueMaxSize;

        public TradesMigrationService(
            ICandlesHistoryRepository candlesHistoryRepository,
            TradesMigrationHealthService tradesMigrationHealthService,
            ILogFactory logFactory,
            IHealthNotifier healthNotifier,
            string sqlConnString,
            int sqlQueryBatchSize,
            TimeSpan sqlTimeout,
            int candlesPersistenceQueueMaxSize
            )
        {
            _candlesHistoryRepository = candlesHistoryRepository ?? throw new ArgumentNullException(nameof(candlesHistoryRepository));
            _tradesMigrationHealthService = tradesMigrationHealthService ?? throw new ArgumentNullException(nameof(tradesMigrationHealthService));
            _logFactory = logFactory;
            _healthNotifier = healthNotifier;

            if (logFactory == null)
                throw new ArgumentNullException(nameof(logFactory));

            _log = logFactory.CreateLog(this); 

            _sqlConnString = sqlConnString;
            _sqlQueryBatchSize = sqlQueryBatchSize;
            _sqlTimeout = sqlTimeout;
            _candlesPersistenceQueueMaxSize = Convert.ToInt32(candlesPersistenceQueueMaxSize * 1.1);  // Safe reserve
        }

        #region Public

        /// <inheritdoc cref="ITradesMigrationService"/>
        public async Task MigrateTradesCandlesAsync(DateTime? migrateByDate, List<(string AssetPairId, string SearchToken, string ReverseSearchToken)> assetSearchTokens)
        {
            foreach (var searchToken in assetSearchTokens)
            {
                var sqlRepo = new TradesSqlHistoryRepository(_sqlConnString, _sqlQueryBatchSize, _sqlTimeout, _logFactory, _healthNotifier,
                    migrateByDate, searchToken.AssetPairId, searchToken.SearchToken);
                
                _log.Info(nameof(MigrateTradesCandlesAsync),
                    $"Starting trades migration for {searchToken.AssetPairId}.");

                try
                {
                    var historyMaker = new TradesProcessor(_tradesMigrationHealthService, _candlesHistoryRepository,
                        searchToken.AssetPairId, _candlesPersistenceQueueMaxSize, migrateByDate);

                    while (true)
                    {
                        var tradesBatch = await sqlRepo.GetNextBatchAsync();
                        var batchCount = tradesBatch.Count;

                        if (batchCount == 0)
                            break;

                        await historyMaker.ProcessTradesBatch(tradesBatch);

                        _log.Info(nameof(MigrateTradesCandlesAsync),
                            $"Batch of {batchCount} records for {searchToken.AssetPairId} processed.");
                    }

                    await historyMaker.FlushCandlesIfAny();

                    _log.Info(nameof(MigrateTradesCandlesAsync),
                        $"Migration for {searchToken.AssetPairId} finished. Total records migrated: {_tradesMigrationHealthService[searchToken.AssetPairId].SummaryFetchedTrades}, " +
                        $"total candles stored: {_tradesMigrationHealthService[searchToken.AssetPairId].SummarySavedCandles}.");
                }
                catch (Exception ex)
                {
                    _tradesMigrationHealthService.State = TradesMigrationState.Error;
                    _log.Error(nameof(MigrateTradesCandlesAsync), ex);
                    _log.Info(nameof(MigrateTradesCandlesAsync),
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
            private readonly DateTime? _upperDateLimit;
            private readonly Dictionary<CandleTimeInterval, List<ICandle>> _persistenceCandleQueue;

            public TradesProcessor(TradesMigrationHealthService healthService, ICandlesHistoryRepository historyRepo, string assetPairId, int persistenceQueueMaxSize, DateTime? upperDateLimit)
            {
                _healthService = healthService ?? throw new ArgumentNullException(nameof(healthService));
                _historyRepo = historyRepo ?? throw new ArgumentNullException(nameof(_historyRepo));
                _assetPairId = !string.IsNullOrWhiteSpace(assetPairId)
                    ? assetPairId
                    : throw new ArgumentNullException(nameof(assetPairId));
                _persistenceQueueMaxSize = persistenceQueueMaxSize;
                _upperDateLimit = upperDateLimit;

                _persistenceCandleQueue = new Dictionary<CandleTimeInterval, List<ICandle>>();
                foreach (var si in Candles.Constants.StoredIntervals)
                    _persistenceCandleQueue.Add(si, new List<ICandle>(_persistenceQueueMaxSize));

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

                _healthService.Health.PersistenceQueueSize = PersistenceQueueSize;
            }

            public async Task FlushCandlesIfAny()
            {
                var dataWritingTasks = new List<Task>();

                foreach (var interval in Candles.Constants.StoredIntervals)
                {
                    // If there still remain any active candles which have not been added to persistent queue till this moment.
                    if (_activeCandles.TryGetValue(interval, out var unsavedActiveCandle) &&
                        _persistenceCandleQueue[interval].All(c => c.Timestamp != unsavedActiveCandle.Timestamp))
                    {
                        // But we save only candles which are fully completed by the _upperDateLimit moment.
                        // I.e., if we have _upperDateLimit = 2018.03.05 16:00:15, we should store only the:
                        // - second candles with timestamp not later than 16:00:14;
                        // - minute candles not later than 15:59:00;
                        // - hour candles not later than 15:00:00;
                        // - day candle not later than 2018.03.04;
                        // - week candles not later than 2018.02.26;
                        // - and, finally, month candles not later than 2018.02.
                        if (_upperDateLimit == null || 
                            unsavedActiveCandle.Timestamp.AddIntervalTicks(1, unsavedActiveCandle.TimeInterval) <= _upperDateLimit)
                            _persistenceCandleQueue[interval].Add(unsavedActiveCandle);
                    }

                    // And now, we save the resulting candles to storage (if any).
                    if (!_persistenceCandleQueue[interval].Any()) continue;

                    dataWritingTasks.Add(_historyRepo.ReplaceCandlesAsync(_persistenceCandleQueue[interval]));

                    _healthService[_assetPairId].SummarySavedCandles +=
                        _persistenceCandleQueue[interval].Count;

                    _persistenceCandleQueue[interval].Clear(); // Protection against multiple calls
                }

                await Task.WhenAll(dataWritingTasks);

                _healthService.Health.PersistenceQueueSize = 0;
            }

            private async Task ProcessTrade(TradeHistoryItem trade, decimal volumeMultiplier)
            {
                var dataWritingTasks = new List<Task>();

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
                            _persistenceCandleQueue[interval].Add(activeCandle);

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

                    if (_persistenceCandleQueue[interval].Count < _persistenceQueueMaxSize) continue;

                    dataWritingTasks.Add(_historyRepo.ReplaceCandlesAsync(_persistenceCandleQueue[interval]));

                    _healthService[_assetPairId].SummarySavedCandles +=
                        _persistenceCandleQueue[interval].Count;

                    _persistenceCandleQueue[interval] = new List<ICandle>(_persistenceQueueMaxSize);
                }

                await Task.WhenAll(dataWritingTasks);

                _healthService.Health.PersistenceQueueSize = PersistenceQueueSize;
            }

            private int PersistenceQueueSize => 
                _persistenceCandleQueue.Values.Sum(c => c.Count);
        }

        #endregion

        #region Private

        #endregion
    }
}
