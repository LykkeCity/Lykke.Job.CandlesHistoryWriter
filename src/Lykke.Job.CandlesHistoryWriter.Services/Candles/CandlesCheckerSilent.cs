using Common.Log;
using Lykke.Common.Log;
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
        /// <param name="logFactory">The <see cref="ILogFactory"/> instance.</param>
        /// <param name="historyRep">The <see cref="ICandlesHistoryRepository"/> instance.</param>
        /// <param name="component">Component name for log.</param>
        // ReSharper disable once MemberCanBeProtected.Global
        public CandlesCheckerSilent(
            ILogFactory logFactory,
            ICandlesHistoryRepository historyRep, string component)
        {
            Log = logFactory.CreateLog(component);
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
