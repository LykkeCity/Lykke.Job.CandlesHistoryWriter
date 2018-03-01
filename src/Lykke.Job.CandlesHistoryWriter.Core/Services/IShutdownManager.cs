using System.Threading.Tasks;

namespace Lykke.Job.CandlesHistoryWriter.Core.Services
{
    public interface IShutdownManager
    {
        bool IsShuttedDown { get; }
        bool IsShuttingDown { get; }

        Task ShutdownAsync();
    }
}
