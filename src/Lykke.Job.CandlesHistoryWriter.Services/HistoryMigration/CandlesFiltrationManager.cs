// Copyright (c) 2019 Lykke Corp.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Common.Log;
using JetBrains.Annotations;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.Candles;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.HistoryMigration.Filtration;
using Lykke.Job.CandlesHistoryWriter.Core.Services.Assets;
using Lykke.Job.CandlesHistoryWriter.Core.Services.HistoryMigration;
using Lykke.Job.CandlesProducer.Contract;
using Constants = Lykke.Job.CandlesHistoryWriter.Services.Candles.Constants;

namespace Lykke.Job.CandlesHistoryWriter.Services.HistoryMigration
{
    [UsedImplicitly]
    public class CandlesFiltrationManager
    {
        public enum FiltrationLaunchResult
        {
            Started,
            AlreadyInProgress,
            AssetPairNotSupported
        }

        private readonly IAssetPairsManager _assetPairsManager;
        private readonly ICandlesHistoryRepository _candlesHistoryRepository;
        private readonly ICandlesFiltrationService _candlesFiltrationService;
        private readonly ILog _log;

        public CandlesFiltrationHealthReport Health;

        public CandlesFiltrationManager(
            IAssetPairsManager assetPairsManager,
            ICandlesHistoryRepository candlesHistoryRepository,
            ICandlesFiltrationService candlesFiltrationService,
            ILog log
        )
        {
            _assetPairsManager = assetPairsManager;
            _candlesHistoryRepository = candlesHistoryRepository;
            _candlesFiltrationService = candlesFiltrationService;
            _log = log;

            Health = null;
        }

        public FiltrationLaunchResult Filtrate(ICandlesFiltrationRequest request, bool analyzeOnly)
        {
            // We should not run filtration multiple times before the first attempt ends.
            if (Health != null && Health.State == CandlesFiltrationState.InProgress)
                return FiltrationLaunchResult.AlreadyInProgress;

            // And also we should check if the specified asset pair is enabled.
            var storedAssetPair = _assetPairsManager.TryGetEnabledPairAsync(request.AssetPairId).GetAwaiter().GetResult();
            if (storedAssetPair == null)
                return FiltrationLaunchResult.AssetPairNotSupported;

            var epsilon = Math.Pow(10, -storedAssetPair.Accuracy);

            _log.WriteInfo(nameof(CandlesFiltrationManager), nameof(Filtrate),
                $"Starting candles with extreme price filtration for {request.AssetPairId}...");

            Health = new CandlesFiltrationHealthReport(request.AssetPairId, request.LimitLow, request.LimitHigh, analyzeOnly);

            var priceTypeTasks = new List<Task>();
            foreach (var priceType in Constants.StoredPriceTypes)
            {
                priceTypeTasks.Add(
                    DoFiltrateAsync(request.AssetPairId, request.LimitLow, request.LimitHigh, priceType, epsilon, analyzeOnly));
            }

            Task.WhenAll(priceTypeTasks.ToArray()).ContinueWith(t =>
            {
                Health.State = CandlesFiltrationState.Finished;

                if (analyzeOnly)
                    _log.WriteInfo(nameof(CandlesFiltrationManager), nameof(Filtrate),
                        $"Filtration for {request.AssetPairId} finished: analyze only. Total amount of candles to delete: {Health.DeletedCandlesCount.Values.Sum()}, " +
                        $"total amount of candles to replace: {Health.ReplacedCandlesCount.Values.Sum()}. Errors count: {Health.Errors.Count}.");
                else
                    _log.WriteInfo(nameof(CandlesFiltrationManager), nameof(Filtrate),
                        $"Filtration for {request.AssetPairId} finished. Total amount of deleted Sec candles: {Health.DeletedCandlesCount.Values.Sum()}, " +
                        $"total amount of replaced bigger candles: {Health.ReplacedCandlesCount.Values.Sum()}. Errors count: {Health.Errors.Count}.");
            });

            return FiltrationLaunchResult.Started;
        }

        private async Task DoFiltrateAsync(string assetId, double limitLow, double limitHigh, CandlePriceType priceType, double epsilon, bool analyzeOnly)
        {
            try
            {
                await _log.WriteInfoAsync(nameof(CandlesFiltrationManager), nameof(DoFiltrateAsync),
                    $"Starting candles with extreme prices filtration for price type {priceType}...");

                // First of all we need to find out if there are any extreme candles with the given parameters or there are not.
                var extremeCandles = await _candlesFiltrationService.TryGetExtremeCandlesAsync(assetId, priceType, limitLow, limitHigh, epsilon);

                if (!extremeCandles.Any())
                {
                    await _log.WriteInfoAsync(nameof(CandlesFiltrationManager), nameof(DoFiltrateAsync),
                        $"There are no extreme price candles for price type {priceType}. Skipping.");
                    return;
                }

                // If we were asked just to analyze the amount of troubled candles, we need to prepare the answer here
                // and exit.
                if (analyzeOnly)
                {
                    var secondCandlesCount = extremeCandles
                        .Count(c => c.TimeInterval == CandleTimeInterval.Sec);

                    Health.DeletedCandlesCount[priceType] = secondCandlesCount;
                    Health.ReplacedCandlesCount[priceType] = extremeCandles.Count - secondCandlesCount;

                    await _log.WriteInfoAsync(nameof(CandlesFiltrationManager), nameof(DoFiltrateAsync),
                        $"Filtration for price type {priceType} finished: analyze only. Candles to delete: {Health.DeletedCandlesCount[priceType]}, " +
                        $"candles to replace: {Health.ReplacedCandlesCount[priceType]}");
                    return;
                }

                // Otherwise, we need to delete Second candles with extreme prices from repository and re-calculate the
                // corresponding candles of the bigger time intervals.
                var (deletedCandlesCount, replacedCandlesCount) = await _candlesFiltrationService.FixExtremeCandlesAsync(extremeCandles, priceType);

                // Reporting.
                Health.DeletedCandlesCount[priceType] = deletedCandlesCount;
                Health.ReplacedCandlesCount[priceType] = replacedCandlesCount;

                await _log.WriteInfoAsync(nameof(CandlesFiltrationManager), nameof(DoFiltrateAsync),
                    $"Filtration for price type {priceType} finished.");
            }
            catch (Exception ex)
            {
                Health.Errors.Add($"{assetId} - {priceType}: {ex.Message}");
                await _log.WriteErrorAsync(nameof(CandlesFiltrationManager), nameof(DoFiltrateAsync), ex);
            }
        }
    }
}
