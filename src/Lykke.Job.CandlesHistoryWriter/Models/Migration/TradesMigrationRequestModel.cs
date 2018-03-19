using System.Collections.Generic;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.HistoryMigration.HistoryProviders.TradesSQLHistory;

namespace Lykke.Job.CandlesHistoryWriter.Models.Migration
{
    public class TradesMigrationRequestModel : ITradesMigrationRequest
    {
        public IEnumerable<AssetMigrationItem> MigrationItems { get; set; }
    }
}
