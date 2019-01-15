using System.Threading.Tasks;
using Lykke.Sdk;

namespace Lykke.Job.CandlesHistoryWriter.Services
{
    public class ShutdownManager : IShutdownManager
    {
        public Task StopAsync()
        {
            return Task.CompletedTask;
        }
    }
}
