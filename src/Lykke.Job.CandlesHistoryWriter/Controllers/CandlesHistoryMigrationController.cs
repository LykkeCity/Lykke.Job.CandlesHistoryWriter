using System.Threading.Tasks;
using Lykke.Job.CandlesHistoryWriter.Core.Services.HistoryMigration.HistoryProviders;
using Lykke.Job.CandlesHistoryWriter.Models.Migration;
using Lykke.Job.CandlesHistoryWriter.Services.HistoryMigration;
using Lykke.Job.CandlesHistoryWriter.Services.HistoryMigration.HistoryProviders.MeFeedHistory;
using Microsoft.AspNetCore.Mvc;
namespace Lykke.Job.CandlesHistoryWriter.Controllers
{
    [Route("api/[controller]")]
    public class CandlesHistoryMigrationController : Controller
    {
        private readonly CandlesMigrationManager _candlesMigrationManager;
        private readonly TradesMigrationManager _tradesMigrationManager;
        private readonly IHistoryProvidersManager _historyProvidersManager;

        public CandlesHistoryMigrationController(
            CandlesMigrationManager candlesMigrationManager, 
            TradesMigrationManager tradesMigrationManager,
            IHistoryProvidersManager historyProvidersManager)
        {
            _candlesMigrationManager = candlesMigrationManager;
            _tradesMigrationManager = tradesMigrationManager;
            _historyProvidersManager = historyProvidersManager;
        }

        [HttpPost]
        [Route("{assetPair}")]
        public async Task<IActionResult> Migrate(string assetPair)
        {
            var result = await _candlesMigrationManager.MigrateAsync(
                assetPair,
                _historyProvidersManager.GetProvider<MeFeedHistoryProvider>());

            return Ok(result);
        }

        [HttpPost]
        [Route("trades")]
        public async Task<IActionResult> MigrateTrades([FromBody] TradesMigrationRequestModel request)
        {
            await _tradesMigrationManager.MigrateAsync(request);

            return Ok();
        }

        [HttpGet]
        [Route("health")]
        public IActionResult Health()
        {
            return Ok(_candlesMigrationManager.Health);
        }

        [HttpGet]
        [Route("health/{assetPair}")]
        public IActionResult Health(string assetPair)
        {
            if (!_candlesMigrationManager.Health.ContainsKey(assetPair))
            {
                return NotFound();
            }

            return Ok(_candlesMigrationManager.Health[assetPair]);
        }
    }
}
