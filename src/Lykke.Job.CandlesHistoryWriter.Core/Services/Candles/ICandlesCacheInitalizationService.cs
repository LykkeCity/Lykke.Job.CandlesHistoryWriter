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
