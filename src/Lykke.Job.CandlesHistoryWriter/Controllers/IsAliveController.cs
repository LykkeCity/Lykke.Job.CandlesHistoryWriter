using Lykke.Job.CandlesHistoryWriter.Core.Services;
using Lykke.Job.CandlesHistoryWriter.Models.IsAlive;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.PlatformAbstractions;
using Swashbuckle.AspNetCore.SwaggerGen;

namespace Lykke.Job.CandlesHistoryWriter.Controllers
{
    /// <summary>
    /// Controller to test service is alive
    /// </summary>
    [Route("api/[controller]")]
    public class IsAliveController : Controller
    {
        private readonly IHealthService _healthService;
        private readonly IShutdownManager _shutdownManager;

        public IsAliveController(IHealthService healthService, IShutdownManager shutdownManager)
        {
            _healthService = healthService;
            _shutdownManager = shutdownManager;
        }

        /// <summary>
        /// Checks service is alive
        /// </summary>
        [HttpGet]
        [SwaggerOperation("IsAlive")]
        public IsAliveResponse Get()
        {
            return new IsAliveResponse
            {
                Name = PlatformServices.Default.Application.ApplicationName,
                Version = PlatformServices.Default.Application.ApplicationVersion,
                Env = Program.EnvInfo,
                IsShuttingDown = _shutdownManager.IsShuttingDown,
                IsShuttedDown = _shutdownManager.IsShuttedDown,
                Persistence = new IsAliveResponse.PersistenceInfo
                {
                    Times = new IsAliveResponse.Times
                    {
                        AveragePersistTime = _healthService.AveragePersistTime,
                        LastPersistTime = _healthService.LastPersistTime,
                        TotalPersistTime = _healthService.TotalPersistTime,
                    },
                    Throughput = new IsAliveResponse.Throughput
                    {
                        AverageCandlesPersistedPerSecond = _healthService.AverageCandlesPersistedPerSecond,
                        AverageCandleRowsPersistedPerSecond = _healthService.AverageCandleRowsPersistedPerSecond
                    },
                    BatchesToPersistQueueLength = _healthService.BatchesToPersistQueueLength,
                    CandlesToDispatchQueueLength = _healthService.CandlesToDispatchQueueLength,
                    TotalCandlesPersistedCount = _healthService.TotalCandlesPersistedCount,
                    TotalCandleRowsPersistedCount = _healthService.TotalCandleRowsPersistedCount,
                }
            };
        }
    }
}
