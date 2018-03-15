using System;
using System.Collections.Generic;
using System.Text;

namespace Lykke.Job.CandlesHistoryWriter.Core.Domain.HistoryMigration.HistoryProviders.TradesSQLHistory
{
    public interface ITradesSqlHistoryRepository
    {
        void Init(int startingRowOffset, string assetPairId);
        IEnumerable<TradeHistoryItem> GetNextBatchAsync();
    }
}
