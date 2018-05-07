using Lykke.Job.CandlesHistoryWriter.Core.Domain.Candles;
using Lykke.Job.CandlesProducer.Contract;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Lykke.Job.CandlesHistoryWriter.Core.Services.HistoryMigration
{
    public interface ICandlesFiltrationService
    {
        Task<IReadOnlyList<ICandle>> TryGetExtremeCandlesAsync(string assetPairId, CandlePriceType priceType, double limitLow, double limitHigh, double epsilon);
        Task<(int deletedCandlesCount, int replacedCandlesCount)> FixExtremeCandlesAsync(IReadOnlyList<ICandle> extremeCandles, CandlePriceType priceType);
    }
}
