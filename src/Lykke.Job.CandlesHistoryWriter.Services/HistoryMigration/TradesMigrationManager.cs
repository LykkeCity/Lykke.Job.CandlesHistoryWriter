using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Common.Log;
using JetBrains.Annotations;
using Lykke.Job.CandlesHistoryWriter.Core.Services.Assets;
using Lykke.Job.CandlesHistoryWriter.Core.Services.HistoryMigration;

namespace Lykke.Job.CandlesHistoryWriter.Services.HistoryMigration
{
    [UsedImplicitly]
    public class TradesMigrationManager
    {
        private readonly IAssetPairsManager _assetPairsManager;
        private readonly ITradesMigrationService _tradesMigrationServicey;
        private readonly TradesMigrationHealthService _tradesMigrationHealthService;
        private readonly ILog _log;

        public bool MigrationEnabled { get; }

        public TradesMigrationManager(
            IAssetPairsManager assetPairsManager,
            ITradesMigrationService tradesMigrationServicey,
            TradesMigrationHealthService tradesMigrationHealthService,
            ILog log,
            bool migrationEnabled
            )
        {
            _assetPairsManager = assetPairsManager ?? throw new ArgumentNullException("assetPairsManager");
            _tradesMigrationServicey = tradesMigrationServicey ?? throw new ArgumentNullException("tradesMigrationServicey");
            _tradesMigrationHealthService = tradesMigrationHealthService ?? throw new ArgumentNullException("tradesMigrationHealthService");
            _log = log ?? throw new ArgumentNullException("log");

            MigrationEnabled = migrationEnabled;
        }

        public bool Migrate(bool preliminaryRemoval, DateTime? removeByDate, string[] assetPairIds)
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

            // We do not parallel the migration of different asset pairs consciously.
            // If we have no upper date-time limit for migration, we migrate everything.
            Task.Run(() => 
                _tradesMigrationServicey.MigrateTradesCandlesAsync(removeByDate ?? DateTime.MaxValue, assetSearchTokens)
                    .GetAwaiter()
                    .GetResult()); 
            return true;
        }
    }
}
