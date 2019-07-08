// Copyright (c) 2019 Lykke Corp.
// See the LICENSE file in the project root for more information.

namespace Lykke.Job.CandlesHistoryWriter.Core.Domain.HistoryMigration.Filtration
{
    public interface ICandlesFiltrationRequest
    {
        string AssetPairId { get; set; }
        double LimitLow { get; set; }
        double LimitHigh { get; set; }
    }
}
