using System.Threading.Tasks;

namespace Lykke.Job.CandlesHistoryWriter.Core.Services
{
    public interface IStartupManager
    {
        Task StartAsync();
    }
}
