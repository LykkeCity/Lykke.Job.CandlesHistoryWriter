// Copyright (c) 2019 Lykke Corp.
// See the LICENSE file in the project root for more information.

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
