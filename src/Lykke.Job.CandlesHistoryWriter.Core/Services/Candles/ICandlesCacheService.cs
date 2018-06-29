using System.Collections.Generic;
using System.Collections.Immutable;
using System.Threading.Tasks;
using Lykke.Job.CandlesProducer.Contract;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.Candles;

namespace Lykke.Job.CandlesHistoryWriter.Core.Services.Candles
{
    public interface ICandlesCacheService : IHaveState<IImmutableDictionary<string, IImmutableList<ICandle>>>
    {
        Task InitializeAsync(string assetPairId, CandlePriceType priceType, CandleTimeInterval timeInterval, IReadOnlyCollection<ICandle> candles);
        Task CacheAsync(IReadOnlyList<ICandle> candle);
        Task InjectCacheValidityToken();
    }
}
