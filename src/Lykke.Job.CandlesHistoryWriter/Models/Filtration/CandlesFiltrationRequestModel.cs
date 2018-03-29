using Lykke.Job.CandlesHistoryWriter.Core.Domain.HistoryMigration.Filtration;

namespace Lykke.Job.CandlesHistoryWriter.Models.Filtration
{
    public class CandlesFiltrationRequestModel : ICandlesFiltrationRequest
    {
        public string AssetId { get; set; }
        public double LimitLow { get; set; }
        public double LimitHigh { get; set; }
    }
}
