using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Lykke.Job.CandlesHistoryWriter.Core.Services.HistoryMigration
{
    public interface ITradesMigrationService
    {
        /// <summary>
        /// Deletes trades candles from repository for all stored time intervals.
        /// </summary>
        /// <param name="assetPairId">The asset pair ID for.</param>
        /// <param name="removeByDate">The date and time upper limit (exclusive) for candles removal.</param>
        /// <remarks>Candles for all the stored time intervals are being deleted in parallel-style for they are physically stored in different Azure tables.</remarks>
        void RemoveTradesCandlesAsync(string assetPairId, DateTime removeByDate);

        /// <summary>
        /// Performs data migration from Trades SQL source to candles history repository.
        /// </summary>
        /// <param name="migrateByDate">The date and time upper limit (exclusive) for candles migration.</param>
        /// <param name="assetSearchTokens">Search parameters for asset pairs.</param>
        Task MigrateTradesCandlesAsync(DateTime migrateByDate, List<(string AssetPairId, string SearchToken, string ReverseSearchToken)> assetSearchTokens);
    }
}
