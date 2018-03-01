using Common.Log;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.Candles;
using Lykke.Job.CandlesHistoryWriter.Core.Services.Candles;

namespace Lykke.Job.CandlesHistoryWriter.Services.Candles
{
    public class CandlesCheckerSilent : ICandlesChecker
    {
        protected readonly ILog Log;
        private readonly ICandlesHistoryRepository _candlesHistoryRepository;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="log">The <see cref="ILog"/> instance.</param>
        /// <param name="historyRep">The <see cref="ICandlesHistoryRepository"/> instance.</param>
        // ReSharper disable once MemberCanBeProtected.Global
        public CandlesCheckerSilent(
            ILog log,
            ICandlesHistoryRepository historyRep)
        {
            Log = log;
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
