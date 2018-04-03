using System.Threading.Tasks;
using Lykke.Common.Api.Contract.Responses;
using Lykke.Job.CandlesHistoryWriter.Core.Services.HistoryMigration.HistoryProviders;
using Lykke.Job.CandlesHistoryWriter.Models.Filtration;
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
        private readonly CandlesFiltrationManager _candlesFiltrationManager;
        private readonly IHistoryProvidersManager _historyProvidersManager;

        public CandlesHistoryMigrationController(
            CandlesMigrationManager candlesMigrationManager, 
            TradesMigrationManager tradesMigrationManager,
            CandlesFiltrationManager candlesFiltrationManager,
            IHistoryProvidersManager historyProvidersManager)
        {
            _candlesMigrationManager = candlesMigrationManager;
            _tradesMigrationManager = tradesMigrationManager;
            _candlesFiltrationManager = candlesFiltrationManager;
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

        [HttpPost]
        [Route("trades")]
        public IActionResult MigrateTrades([FromBody] TradesMigrationRequestModel request)
        {
            // This method is sync but internally starts a new task and returns
            var migrationStarted = _tradesMigrationManager.Migrate(request);

            if (migrationStarted)
                return Ok();

            return BadRequest(
                ErrorResponse.Create("The previous migration session still has not been finished. Parallel execution is not supported."));
        }

        [HttpGet]
        [Route("trades/health")]
        public IActionResult TradesHealth()
        {
            var healthReport = _tradesMigrationManager.Health;

            // If null, we have not currently been carrying out a trades migration.
            if (healthReport == null)
                return NoContent();

            return Ok(healthReport);
        }

        [HttpPost]
        [Route("extremumFilter")]
        public IActionResult FilterExtremumCandles([FromBody] CandlesFiltrationRequestModel request, bool analyzeOnly)
        {
            var modelErrors = CandlesFiltrationRequestModel.CheckupModel(request);
            if (modelErrors.Count > 0)
            {
                var response = new ErrorResponse();
                foreach (var me in modelErrors)
                    response.AddModelError(me.Key, me.Value);

                return BadRequest(response);
            }

            var filtrationLaunchResult = _candlesFiltrationManager.Filtrate(request, analyzeOnly);

            switch (filtrationLaunchResult)
            {
                case CandlesFiltrationManager.FiltrationLaunchResult.AlreadyInProgress:
                    return BadRequest(
                        ErrorResponse.Create("The previous filtration session still has not been finished. Parallel execution is not supported."));

                case CandlesFiltrationManager.FiltrationLaunchResult.AssetPairNotSupported:
                    return BadRequest(
                        ErrorResponse.Create("The specified asset pair is not supported."));

                default:
                    return Ok();
            }
        }

        [HttpGet]
        [Route("extremumFilter/health")]
        public IActionResult ExtremumFilterHealth()
        {
            var healthReport = _candlesFiltrationManager.Health;

            if (healthReport == null)
                return NoContent();

            return Ok(healthReport);
        }
    }
}
