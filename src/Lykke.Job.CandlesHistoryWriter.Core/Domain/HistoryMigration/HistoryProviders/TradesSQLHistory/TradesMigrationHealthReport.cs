using System.Collections.Generic;

namespace Lykke.Job.CandlesHistoryWriter.Core.Domain.HistoryMigration.HistoryProviders.TradesSQLHistory
{
    public class TradesMigrationHealthReport
    {
        public int SqlQueryBatchSize { get; }
        public IDictionary<string, TradesMigrationHealthReportItem> AssetReportItems { get; }

        public TradesMigrationHealthReport(int sqlQueryBatchSize)
        {
            SqlQueryBatchSize = sqlQueryBatchSize;
            AssetReportItems = new Dictionary<string, TradesMigrationHealthReportItem>();
        }
    }

    public class TradesMigrationHealthReportItem
    {
        public int StartingOffset { get; }
        public int SummaryFetchedTrades { get; set; }
        public int SummarySavedCandles { get; set; }

        public TradesMigrationHealthReportItem(int startingOffset)
        {
            StartingOffset = startingOffset;
            SummaryFetchedTrades = SummarySavedCandles = 0;
        }
    }
}
