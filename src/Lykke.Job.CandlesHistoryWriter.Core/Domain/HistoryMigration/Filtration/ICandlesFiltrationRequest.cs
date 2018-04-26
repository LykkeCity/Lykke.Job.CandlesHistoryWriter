namespace Lykke.Job.CandlesHistoryWriter.Core.Domain.HistoryMigration.Filtration
{
    public interface ICandlesFiltrationRequest
    {
        string AssetPairId { get; set; }
        double LimitLow { get; set; }
        double LimitHigh { get; set; }
    }
}
