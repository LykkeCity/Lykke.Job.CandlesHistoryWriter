// Copyright (c) 2019 Lykke Corp.
// See the LICENSE file in the project root for more information.

using System.Collections.Generic;
using System.Threading.Tasks;

namespace Lykke.Job.CandlesHistoryWriter.Core.Domain.HistoryMigration.HistoryProviders.TradesSQLHistory
{
    public interface ITradesSqlHistoryRepository
    {
        Task<IReadOnlyCollection<TradeHistoryItem>> GetNextBatchAsync();
    }
}
