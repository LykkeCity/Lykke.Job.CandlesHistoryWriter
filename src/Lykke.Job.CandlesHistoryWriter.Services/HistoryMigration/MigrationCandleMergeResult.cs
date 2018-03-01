using Lykke.Job.CandlesHistoryWriter.Core.Domain.Candles;

namespace Lykke.Job.CandlesHistoryWriter.Services.HistoryMigration
{
    public class MigrationCandleMergeResult
    {
        public ICandle Candle { get; }
        public bool WasChanged { get; }

        public MigrationCandleMergeResult(ICandle candle, bool wasChanged)
        {
            Candle = candle;
            WasChanged = wasChanged;
        }
    }
}
