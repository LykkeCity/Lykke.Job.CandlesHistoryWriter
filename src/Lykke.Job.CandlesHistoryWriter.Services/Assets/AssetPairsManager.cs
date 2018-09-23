using System;
using System.Threading.Tasks;
using Lykke.Job.CandlesHistoryWriter.Core.Services.Assets;
using System.Collections.Generic;
using System.Linq;
using Common.Log;
using Lykke.Service.Assets.Client;
using Lykke.Service.Assets.Client.Models;
using Polly;

namespace Lykke.Job.CandlesHistoryWriter.Services.Assets
{
    public class AssetPairsManager : IAssetPairsManager
    {
        private readonly ILog _log;
        private readonly IAssetsServiceWithCache _apiService;

        public AssetPairsManager(ILog log, IAssetsServiceWithCache apiService)
        {
            _log = log;
            _apiService = apiService;
        }

        public Task<AssetPair> TryGetAssetPairAsync(string assetPairId)
        {
            return Policy
                .Handle<Exception>()
                .WaitAndRetryForeverAsync(
                    retryAttempt => TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)),
                    (exception, timespan) => _log.WriteErrorAsync("Get asset pair with retry", assetPairId, exception))
                .ExecuteAsync(() => _apiService.TryGetAssetPairAsync(assetPairId));
        }

        public async Task<AssetPair> TryGetEnabledPairAsync(string assetPairId)
        {
            var pair = await TryGetAssetPairAsync(assetPairId);

            return pair == null || pair.IsDisabled ? null : pair;
        }

        public Task<IEnumerable<AssetPair>> GetAllEnabledAsync()
        {
            return Policy
                .Handle<Exception>()
                .WaitAndRetryForeverAsync(
                    retryAttempt => TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)),
                    (exception, timespan) => _log.WriteErrorAsync("Get all asset pairs with retry", string.Empty, exception))
                .ExecuteAsync(async () => (await _apiService.GetAllAssetPairsAsync()).Where(a => !a.IsDisabled));
        }

        public Task<IEnumerable<AssetPair>> GetAllAsync()
        {
            return Policy
                .Handle<Exception>()
                .WaitAndRetryForeverAsync(
                    retryAttempt => TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)),
                    (exception, timespan) => _log.WriteErrorAsync("Get all asset pairs with retry", string.Empty, exception))
                .ExecuteAsync(async () => (await _apiService.GetAllAssetPairsAsync()).AsEnumerable());
        }
    }
}
