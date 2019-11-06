// Copyright (c) 2019 Lykke Corp.
// See the LICENSE file in the project root for more information.

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
        Task<IEnumerable<ICandle>> GetLastCandlesAsync(string assetPairId, CandleTimeInterval interval, CandlePriceType priceType, DateTime to, int number);
        Task<ICandle> TryGetFirstCandleAsync(string assetPairId, CandleTimeInterval interval, CandlePriceType priceType);
        Task<int> DeleteCandlesAsync(IReadOnlyList<ICandle> candlesToDelete);
        Task<int> ReplaceCandlesAsync(IReadOnlyList<ICandle> candlesToReplace);
    }
}
