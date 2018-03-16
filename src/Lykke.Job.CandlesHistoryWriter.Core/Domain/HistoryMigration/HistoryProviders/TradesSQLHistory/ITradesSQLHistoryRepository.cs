using System.Collections.Generic;
using System.Threading.Tasks;

namespace Lykke.Job.CandlesHistoryWriter.Core.Domain.HistoryMigration.HistoryProviders.TradesSQLHistory
{
    public interface ITradesSqlHistoryRepository
    {
        Task<bool> InitAsync(int startingRowOffset, string assetPairId);
        Task<IEnumerable<TradeHistoryItem>> GetNextBatchAsync();
    }
}
