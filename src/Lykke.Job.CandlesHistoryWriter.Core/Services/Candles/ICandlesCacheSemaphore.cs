using System.Threading.Tasks;

namespace Lykke.Job.CandlesHistoryWriter.Core.Services.Candles
{
    public interface ICandlesCacheSemaphore
    {
        Task WaitAsync();
        void Release();
    }
}
