using System;
using System.Collections.Generic;
using System.Text;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.HistoryMigration.HistoryProviders.TradesSQLHistory;

namespace Lykke.Job.CandleHistoryWriter.Repositories.HistoryMigration.HistoryProviders.TradesSQLHistory
{
    public class TradesSqlHistoryRepository : ITradesSqlHistoryRepository, IDisposable
    {
        public int StartingRowOffset { get; private set; }
        public string AssetPairId { get; private set; }

        public TradesSqlHistoryRepository()
        {

        }

        public void Init(int startingRowOffset, string assetPairId)
        {
            StartingRowOffset = startingRowOffset;
            AssetPairId = assetPairId;
        }

        public IEnumerable<TradeHistoryItem> GetNextBatchAsync()
        {
            throw new NotImplementedException();
        }

        public void Dispose()
        {
            throw new NotImplementedException();
        }
    }
}
