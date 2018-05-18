using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Lykke.Job.CandlesHistoryWriter.Core.Services.HistoryMigration
{
    public interface ITradesMigrationService
    {
        /// <summary>
        /// Performs data migration from Trades SQL source to candles history repository.
        /// </summary>
        /// <param name="migrateByDate">The date and time upper limit (exclusive) for candles migration.</param>
        /// <param name="assetSearchTokens">Search parameters for asset pairs.</param>
        Task MigrateTradesCandlesAsync(DateTime? migrateByDate, List<(string AssetPairId, string SearchToken, string ReverseSearchToken)> assetSearchTokens);
    }
}
