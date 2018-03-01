using System.Threading.Tasks;
using JetBrains.Annotations;
using Lykke.Job.CandlesProducer.Contract;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.Candles;
using Lykke.Job.CandlesHistoryWriter.Core.Services.HistoryMigration;

namespace Lykke.Job.CandlesHistoryWriter.Services.HistoryMigration
{
    [UsedImplicitly]
    public class CandlesesHistoryMigrationService : ICandlesHistoryMigrationService
    {
        private readonly ICandlesHistoryRepository _candlesHistoryRepository;

        public CandlesesHistoryMigrationService(ICandlesHistoryRepository candlesHistoryRepository)
        {
            _candlesHistoryRepository = candlesHistoryRepository;
        }
        
        public async Task<ICandle> GetFirstCandleOfHistoryAsync(string assetPair, CandlePriceType priceType)
        {
            var candle = await _candlesHistoryRepository.TryGetFirstCandleAsync(assetPair, CandleTimeInterval.Sec, priceType);

            return candle;
        }
    }
}
