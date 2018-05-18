using System;
using System.Collections.Generic;
using System.Linq;
using Common.Log;
using JetBrains.Annotations;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.HistoryMigration;
using Lykke.Job.CandlesHistoryWriter.Core.Services.Assets;
using Lykke.Job.CandlesHistoryWriter.Core.Services.HistoryMigration;

namespace Lykke.Job.CandlesHistoryWriter.Services.HistoryMigration
{
    [UsedImplicitly]
    public class TradesMigrationManager
    {
        private readonly IAssetPairsManager _assetPairsManager;
        private readonly ITradesMigrationService _tradesMigrationService;
        private readonly TradesMigrationHealthService _tradesMigrationHealthService;
        private readonly ILog _log;

        private readonly int _sqlQueryBatchSize;

        public bool MigrationEnabled { get; }

        public TradesMigrationManager(
            IAssetPairsManager assetPairsManager,
            ITradesMigrationService tradesMigrationServicey,
            TradesMigrationHealthService tradesMigrationHealthService,
            ILog log,
            int sqlQueryBatchSize,
            bool migrationEnabled
            )
        {
            _assetPairsManager = assetPairsManager ?? throw new ArgumentNullException(nameof(assetPairsManager));
            _tradesMigrationService = tradesMigrationServicey ?? throw new ArgumentNullException(nameof(tradesMigrationServicey));
            _tradesMigrationHealthService = tradesMigrationHealthService ?? throw new ArgumentNullException(nameof(tradesMigrationHealthService));
            _log = log ?? throw new ArgumentNullException(nameof(log));

            _sqlQueryBatchSize = sqlQueryBatchSize;

            MigrationEnabled = migrationEnabled;
        }

        public bool Migrate(DateTime? removeByDate, string[] assetPairIds)
        {
            if (!MigrationEnabled)
                return false;

            // We should not run migration multiple times before the first attempt ends.
            if (!_tradesMigrationHealthService.CanStartMigration)
                return false;
            
            // First of all, we will check if we can store the requested asset pairs. Additionally, let's
            // generate asset search tokens for using it in TradesSqlHistoryRepository (which has no access
            // to AssetPairsManager).
            var assetSearchTokens = new List<(string AssetPairId, string SearchToken, string ReverseSearchToken)>();
            foreach (var assetPairId in assetPairIds)
            {
                var storedAssetPair = _assetPairsManager.TryGetEnabledPairAsync(assetPairId).GetAwaiter().GetResult();
                if (storedAssetPair == null)
                {
                     _log.WriteInfoAsync(nameof(TradesMigrationManager), nameof(Migrate),
                        $"Trades migration: Asset pair {assetPairId} is not currently enabled. Skipping.").GetAwaiter().GetResult();
                    continue;
                }

                assetSearchTokens.Add((
                    AssetPairId: assetPairId,
                    SearchToken: storedAssetPair.BaseAssetId + storedAssetPair.QuotingAssetId,
                    ReverseSearchToken: storedAssetPair.QuotingAssetId + storedAssetPair.BaseAssetId));
            }

            // Creating a blank health report
            _tradesMigrationHealthService.Prepare(_sqlQueryBatchSize, removeByDate);

            // If there is nothing to migrate, just return "success".
            if (!assetSearchTokens.Any())
            {
                _tradesMigrationHealthService.State = TradesMigrationState.Finished;
                _log.WriteInfoAsync(nameof(TradesMigrationManager), nameof(Migrate),
                        $"Trades migration: None of the requested asset pairs can be stored. Migration canceled.").GetAwaiter().GetResult();
                return true;
            }

            // 1. We do not parallel the migration of different asset pairs consciously.
            // 2. If we have no upper date-time limit for migration, we migrate everything.
            // 3. We do not await while the migration process is finished for it may take a lot of time. Immediately return to a caller code instead.
            _tradesMigrationService.MigrateTradesCandlesAsync(removeByDate, assetSearchTokens); 

            return true;
        }
    }
}
