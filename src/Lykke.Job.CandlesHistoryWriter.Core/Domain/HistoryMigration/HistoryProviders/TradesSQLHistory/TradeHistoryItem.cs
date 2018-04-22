using System;

namespace Lykke.Job.CandlesHistoryWriter.Core.Domain.HistoryMigration.HistoryProviders.TradesSQLHistory
{
    public class TradeHistoryItem
    {
        public long Id { get; set; }
        public string AssetToken { get; set; }
        public TradeDirection Direction { get; set; }
        public decimal Price { get; set; }
        public decimal Volume { get; set; }
        public decimal OppositeVolume { get; set; }
        public DateTime DateTime { get; set; }
    }

    public enum TradeDirection
    {
        Buy,
        Sell
    }
}
