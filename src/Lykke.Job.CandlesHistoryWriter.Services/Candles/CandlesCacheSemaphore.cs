using System.Threading;
using System.Threading.Tasks;
using Lykke.Job.CandlesHistoryWriter.Core.Services.Candles;

namespace Lykke.Job.CandlesHistoryWriter.Services.Candles
{
    /// <summary>
    /// The process-global blocker for different cache operations. Please, use it as a singleton service in IOC.
    /// </summary>
    public class CandlesCacheSemaphore : ICandlesCacheSemaphore
    {
        private readonly SemaphoreSlim _sem; // Not static 'casuse the preffered way of using the class is to register it as a singleton at startup

        public CandlesCacheSemaphore()
        {
            _sem = new SemaphoreSlim(1, 1);
        }

        public void Release()
        {
            _sem.Release();
        }

        public async Task WaitAsync()
        {
            await _sem.WaitAsync();
        }
    }
}
