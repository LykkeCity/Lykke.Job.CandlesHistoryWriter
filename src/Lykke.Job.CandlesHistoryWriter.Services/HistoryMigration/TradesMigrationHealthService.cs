using Lykke.Job.CandlesHistoryWriter.Core.Domain.HistoryMigration;
using System;

namespace Lykke.Job.CandlesHistoryWriter.Services.HistoryMigration
{
    public class TradesMigrationHealthService
    {
        private TradesMigrationHealthReport _health;

        public TradesMigrationHealthReport Health => _health;

        public bool CanStartMigration =>
            _health == null || _health.State != TradesMigrationState.InProgress;

        public TradesMigrationState State
        {
            get => _health.State;
            set => _health.State = value;
        }

        public void Prepare(int sqlQueryBatchSize, DateTime? removeByDate)
        {
            _health = new TradesMigrationHealthReport(sqlQueryBatchSize, removeByDate);
        }

        public TradesMigrationHealthReportItem this [string key]
        {
            get
            {
                AddItemIfNotExists(key);
                return _health.AssetReportItems[key];
            }
            set => _health.AssetReportItems[key] = value;
        }

        private void AddItemIfNotExists(string assetPairId)
        {
            if (!_health.AssetReportItems.ContainsKey(assetPairId))
                _health.AssetReportItems[assetPairId] = new TradesMigrationHealthReportItem();
        }
    }
}
