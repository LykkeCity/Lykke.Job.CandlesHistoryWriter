using System;
using System.Collections.Generic;
using System.Text;

namespace Lykke.Job.CandlesHistoryWriter.Core.Domain.HistoryMigration.HistoryProviders.TradesSQLHistory
{
    public interface ITradesMigrationRequest
    {
        IEnumerable<AssetMigrationItem> MigrationItems { get; set; }
    }

    public class AssetMigrationItem
    {
        public string AssetId { get; set; }
        public int OffsetFromTop { get; set; }
    }
}
