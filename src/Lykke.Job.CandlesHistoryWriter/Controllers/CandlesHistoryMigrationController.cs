using System;
using System.Threading.Tasks;
using Lykke.Common.Api.Contract.Responses;
using Lykke.Job.CandlesHistoryWriter.Core.Services.HistoryMigration.HistoryProviders;
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
        private readonly TradesMigrationHealthService _tradesMigrationHealthService;

        public CandlesHistoryMigrationController(
            CandlesMigrationManager candlesMigrationManager, 
            TradesMigrationManager tradesMigrationManager,
            IHistoryProvidersManager historyProvidersManager,
            TradesMigrationHealthService tradesMigrationHealthService)
        {
            _candlesMigrationManager = candlesMigrationManager;
            _tradesMigrationManager = tradesMigrationManager;
            _historyProvidersManager = historyProvidersManager;
            _tradesMigrationHealthService = tradesMigrationHealthService;
        }

        [HttpPost]
        [Route("{assetPair}")]
        public async Task<IActionResult> Migrate(string assetPair)
        {
            if (!_candlesMigrationManager.MigrationEnabled)
                return Ok("Migration is currently disabled in application settings.");

            var result = await _candlesMigrationManager.MigrateAsync(
                assetPair,
                _historyProvidersManager.GetProvider<MeFeedHistoryProvider>());

            return Ok(result);
        }

        [HttpGet]
        [Route("health")]
        public IActionResult Health()
        {
            if (!_candlesMigrationManager.MigrationEnabled)
                return Ok("Migration is currently disabled in application settings.");

            return Ok(_candlesMigrationManager.Health);
        }

        [HttpGet]
        [Route("health/{assetPair}")]
        public IActionResult Health(string assetPair)
        {
            if (!_candlesMigrationManager.MigrationEnabled)
                return Ok("Migration is currently disabled in application settings.");

            if (!_candlesMigrationManager.Health.ContainsKey(assetPair))
            {
                return NotFound();
            }

            return Ok(_candlesMigrationManager.Health[assetPair]);
        }

        /// <summary>
        /// Starts execution of the trades-to-candles migration process.
        /// </summary>
        /// <param name="preliminaryRemoval">Use this, if you want to remove the existing trades candles first.</param>
        /// <param name="removeByDate">Optional. The date (and time, if needed) which defines the upper limit (exclusive) for preliminary removal for all asset pairs. It can be empty if
        /// the prelimiary removal is not needed.</param>
        /// <param name="assetPairs">The list of asset pair IDs to migrate.</param>
        /// <returns></returns>
        [HttpPost]
        [Route("trades")]
        public IActionResult MigrateTrades(bool preliminaryRemoval, DateTime? removeByDate, string[] assetPairIds)
        {
            if (!_tradesMigrationManager.MigrationEnabled)
                return Ok("Migration is currently disabled in application settings.");

            if (preliminaryRemoval)
            {
                var errorMessage = "Please, check the remove-by date.";

                if (removeByDate == null)
                    errorMessage = "If you need to preliminary remove the old trades candle data, please, specify the remove-by date (exclusive).";
                else if (removeByDate > DateTime.UtcNow)
                    errorMessage = "Remove-by date should not be in the past.";

                return BadRequest(
                    ErrorResponse.Create(errorMessage));
                
            }

            if (assetPairIds == null || assetPairIds.Length == 0)
                return BadRequest(
                    ErrorResponse.Create("Please, specify at least one asset pair ID to migrate."));

            // This method is sync but internally starts a new task and returns
            var migrationStarted = true;// _tradesMigrationManager.Migrate(request);

            if (migrationStarted)
                return Ok();

            return BadRequest(
                ErrorResponse.Create("The previous migration session still has not been finished. Parallel execution is not supported."));
        }

        [HttpGet]
        [Route("trades/health")]
        public IActionResult TradesHealth()
        {
            if (!_tradesMigrationManager.MigrationEnabled)
                return Ok("Migration is currently disabled in application settings.");

            var healthReport = _tradesMigrationHealthService.Health;

            // If null, we have not currently been carrying out a trades migration.
            if (healthReport == null)
                return NoContent();

            return Ok(healthReport);
        }
    }
}
