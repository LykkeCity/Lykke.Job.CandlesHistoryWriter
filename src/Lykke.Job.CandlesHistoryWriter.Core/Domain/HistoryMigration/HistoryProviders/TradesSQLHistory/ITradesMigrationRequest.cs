using System.Collections.Generic;

namespace Lykke.Job.CandlesHistoryWriter.Core.Domain.HistoryMigration.HistoryProviders.TradesSQLHistory
{
    public interface ITradesMigrationRequest
    {
        IEnumerable<AssetMigrationItem> MigrationItems { get; set; }
    }

    public class AssetMigrationItem
    {
        public string AssetPairId { get; set; }
        public int OffsetFromTop { get; set; }
    }
}
