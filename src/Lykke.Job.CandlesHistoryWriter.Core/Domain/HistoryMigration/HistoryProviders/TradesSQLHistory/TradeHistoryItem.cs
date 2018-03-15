using System;
using System.Collections.Generic;
using System.Text;

namespace Lykke.Job.CandlesHistoryWriter.Core.Domain.HistoryMigration.HistoryProviders.TradesSQLHistory
{
    public class TradeHistoryItem
    {
        public decimal Price { get; set; }
        public decimal Volume { get; set; }
        public decimal OppositeVolume { get; set; }
        public DateTime DateTime { get; set; }
    }
}
