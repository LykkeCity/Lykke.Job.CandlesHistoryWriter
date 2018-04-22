using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Lykke.Job.CandlesProducer.Contract;

namespace Lykke.Job.CandlesHistoryWriter.Core.Domain.Candles
{
    public interface ICandlesHistoryRepository
    {
        Task InsertOrMergeAsync(IEnumerable<ICandle> candles, string assetPairId, CandlePriceType priceType, CandleTimeInterval timeInterval);
        Task<IEnumerable<ICandle>> GetCandlesAsync(string assetPairId, CandleTimeInterval interval, CandlePriceType priceType, DateTime @from, DateTime to);
        bool CanStoreAssetPair(string assetPairId);
        Task<ICandle> TryGetFirstCandleAsync(string assetPairId, CandleTimeInterval interval, CandlePriceType priceType);
        IReadOnlyList<string> GetStoredAssetPairs();
        Task<bool> DeleteCandlesAsync(string assetPairId, CandleTimeInterval interval, CandlePriceType priceType, DateTime? from = null, DateTime? to = null);
        Task<int> DeleteCandlesAsync(IEnumerable<ICandle> candlesToDelete);
        Task<int> ReplaceCandlesAsync(IEnumerable<ICandle> candlesToReplace);
    }
}
