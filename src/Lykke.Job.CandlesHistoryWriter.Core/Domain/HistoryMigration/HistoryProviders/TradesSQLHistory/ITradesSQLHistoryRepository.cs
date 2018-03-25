using System.Collections.Generic;
using System.Threading.Tasks;

namespace Lykke.Job.CandlesHistoryWriter.Core.Domain.HistoryMigration.HistoryProviders.TradesSQLHistory
{
    public interface ITradesSqlHistoryRepository
    {
        Task<IEnumerable<TradeHistoryItem>> GetNextBatchAsync();
    }
}
