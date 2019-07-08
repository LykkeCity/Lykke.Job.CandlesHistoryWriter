// Copyright (c) 2019 Lykke Corp.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Lykke.Job.CandlesProducer.Contract;

namespace Lykke.Job.CandlesHistoryWriter.Core.Domain.HistoryMigration.HistoryProviders.MeFeedHistory
{
    public interface IFeedHistoryRepository
    {
        Task<IFeedHistory> GetTopRecordAsync(string assetPair, CandlePriceType priceType);
        Task GetCandlesByChunksAsync(string assetPair, CandlePriceType priceType, DateTime endDate, Func<IEnumerable<IFeedHistory>, Task> readChunkFunc);
    }
}
