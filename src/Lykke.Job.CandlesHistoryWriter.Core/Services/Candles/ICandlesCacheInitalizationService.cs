// Copyright (c) 2019 Lykke Corp.
// See the LICENSE file in the project root for more information.

using System.Threading.Tasks;

namespace Lykke.Job.CandlesHistoryWriter.Core.Services.Candles
{
    /// <summary>
    /// Initializes cache from the history storage
    /// </summary>
    public interface ICandlesCacheInitalizationService
    {
        Task InitializeCacheAsync();
    }
}
