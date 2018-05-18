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
        #region PrivateFields

        private readonly CandlesMigrationManager _candlesMigrationManager;
        private readonly TradesMigrationManager _tradesMigrationManager;
        private readonly CandlesFiltrationManager _candlesFiltrationManager;
        private readonly IHistoryProvidersManager _historyProvidersManager;
        private readonly TradesMigrationHealthService _tradesMigrationHealthService;

        #endregion

        #region Init

        public CandlesHistoryMigrationController(
            CandlesMigrationManager candlesMigrationManager, 
            TradesMigrationManager tradesMigrationManager,
            CandlesFiltrationManager candlesFiltrationManager,
            IHistoryProvidersManager historyProvidersManager,
            TradesMigrationHealthService tradesMigrationHealthService)
        {
            _candlesMigrationManager = candlesMigrationManager;
            _tradesMigrationManager = tradesMigrationManager;
            _candlesFiltrationManager = candlesFiltrationManager;
            _historyProvidersManager = historyProvidersManager;
            _tradesMigrationHealthService = tradesMigrationHealthService;
        }

        #endregion

        #region QuotesMigration

        /// <summary>
        /// Initiates a candles history migration session for the given asset pair.
        /// </summary>
        /// <param name="assetPair">Asset pair Id.</param>
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

        /// <summary>
        /// Shows the state of the actual candles migration session for all included asset pairs.
        /// </summary>
        [HttpGet]
        [Route("health")]
        public IActionResult Health()
        {
            if (!_candlesMigrationManager.MigrationEnabled)
                return Ok("Migration is currently disabled in application settings.");

            return Ok(_candlesMigrationManager.Health);
        }

        /// <summary>
        /// Shows the state of the actual candles migration session for the given asset pair.
        /// </summary>
        /// <param name="assetPair">Asset pair Id.</param>
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

        #endregion

        #region TradesMigration

        /// <summary>
        /// Starts execution of the trades-to-candles migration process.
        /// </summary>
        [HttpPost]
        [Route("trades")]
        public IActionResult MigrateTrades([FromBody] TradesMigrationRequestModel request)
        {
            if (request == null)
                return BadRequest("The request data is corrupted - unable to deserialize.");

            if (!request.CheckupModel(out var modelErrors))
            {
                return BadRequest(new ErrorResponse
                    {
                        ErrorMessage = "Inconsistent request.",
                        ModelErrors = modelErrors
                    });
            }

            // ---

            // This method is sync but internally it starts a new task and returns
            var migrationStarted = _tradesMigrationManager.Migrate(request.TimestampUpperLimit, request.AssetPairIds);

            if (migrationStarted)
                return Ok();

            return BadRequest(
                ErrorResponse.Create("The previous migration session still has not been finished. Parallel execution is not supported."));
        }

        /// <summary>
        /// Shows the state of the actual trades migration session for all included asset pairs.
        /// </summary>
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

        #endregion

        #region CandlesFiltration

        /// <summary>
        /// Initiates search and correction of candles with extremal prices session.
        /// </summary>
        /// <param name="request">Filtration session parameters (see model description).</param>
        /// <param name="analyzeOnly">Set this flag to True if it is only needed to estimate the amount of incorrect candles without any correction. Otherwise, set it False.</param>
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

        /// <summary>
        /// Shows the state of the actual candles filtration session.
        /// </summary>
        [HttpGet]
        [Route("extremumFilter/health")]
        public IActionResult ExtremumFilterHealth()
        {
            var healthReport = _candlesFiltrationManager.Health;

            if (healthReport == null)
                return NoContent();

            return Ok(healthReport);
        }

        #endregion
    }
}
