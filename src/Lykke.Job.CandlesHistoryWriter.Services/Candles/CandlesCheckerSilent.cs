using Common.Log;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.Candles;
using Lykke.Job.CandlesHistoryWriter.Core.Services.Candles;

namespace Lykke.Service.CandlesHistory.Services.Candles
{
    public class CandlesCheckerSilent : ICandlesChecker
    {
        protected readonly ILog _log;
        protected readonly ICandlesHistoryRepository _candlesHistoryRepository;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="log">The <see cref="ILog"/> instance.</param>
        /// <param name="historyRep">The <see cref="ICandlesHistoryRepository"/> instance.</param>
        public CandlesCheckerSilent(
            ILog log,
            ICandlesHistoryRepository historyRep)
        {
            _log = log;
            _candlesHistoryRepository = historyRep;
        }

        /// <summary>
        /// Checks if we can handle/store the given asset pair.
        /// </summary>
        /// <param name="assetPairId">Asset pair ID.</param>
        /// <returns>True if repository is able to store such a pair, and false otherwise.</returns>
        public virtual bool CanHandleAssetPair(string assetPairId) => _candlesHistoryRepository.CanStoreAssetPair(assetPairId);
    }
}
