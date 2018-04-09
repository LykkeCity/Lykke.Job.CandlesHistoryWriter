﻿using System;
using System.Threading.Tasks;
using Lykke.Job.CandlesHistoryWriter.Core.Services.Assets;
using System.Collections.Generic;
using System.Linq;
using Common.Log;
using Lykke.Job.CandlesHistoryWriter.Core.Domain;
using Lykke.Service.Assets.Client.Custom;
using Polly;

namespace Lykke.Job.CandlesHistoryWriter.Services.Assets
{
    public class AssetPairsManager : IAssetPairsManager
    {
        private readonly ILog _log;
        private readonly ICachedAssetsService _apiService;

        public AssetPairsManager(ILog log, ICachedAssetsService apiService)
        {
            _log = log;
            _apiService = apiService;
        }

        public async Task<AssetPair> TryGetAssetPairAsync(string assetPairId)
        {
            return Convert(await TryGetAssetPairCoreAsync(assetPairId));
        }

        public async Task<AssetPair> TryGetEnabledPairAsync(string assetPairId)
        {
            var pair = await TryGetAssetPairCoreAsync(assetPairId);
            return pair == null || pair.IsDisabled ? null : Convert(pair);
        }

        public Task<IEnumerable<AssetPair>> GetAllEnabledAsync()
        {
            return Policy
                .Handle<Exception>()
                .WaitAndRetryForeverAsync(
                    retryAttempt => TimeSpan.FromSeconds(Math.Min(60, Math.Pow(2, retryAttempt))),
                    (exception, timespan) =>
                        _log.WriteErrorAsync("Get all asset pairs with retry", string.Empty, exception))
                .ExecuteAsync(async () =>
                    (await _apiService.GetAllAssetPairsAsync()).Where(a => !a.IsDisabled)
                    .Select(Convert));
        }

        private Task<IAssetPair> TryGetAssetPairCoreAsync(string assetPairId)
        {
            return Policy
                .Handle<Exception>()
                .WaitAndRetryForeverAsync(
                    retryAttempt => TimeSpan.FromSeconds(Math.Min(60, Math.Pow(2, retryAttempt))),
                    (exception, timespan) => _log.WriteErrorAsync("Get asset pair with retry", assetPairId, exception))
                .ExecuteAsync(() => _apiService.TryGetAssetPairAsync(assetPairId));
        }

        private static AssetPair Convert(IAssetPair p)
        {
            return new AssetPair(p.Id, p.Accuracy);
        }
    }
}
