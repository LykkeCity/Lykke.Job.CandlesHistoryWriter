using System.Threading.Tasks;
using Lykke.Job.CandlesProducer.Contract;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.Candles;

namespace Lykke.Job.CandlesHistoryWriter.Core.Services.HistoryMigration
{
    public interface ICandlesHistoryMigrationService
    {
        Task<ICandle> GetFirstCandleOfHistoryAsync(string assetPair, CandlePriceType priceType);
    }
}
