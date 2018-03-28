using System;
using System.Collections.Generic;
using System.Text;

namespace Lykke.Job.CandlesHistoryWriter.Core.Domain.HistoryMigration.Filtration
{
    public interface ICandlesFiltrationRequest
    {
        string AssetId { get; set; }
        double LimitLow { get; set; }
        double LimitHigh { get; set; }
    }
}
