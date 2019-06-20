using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Common;
using Common.Log;
using JetBrains.Annotations;
using Lykke.Common.Log;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.Candles;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.HistoryMigration;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.HistoryMigration.Filtration;
using Lykke.Job.CandlesHistoryWriter.Core.Services.Assets;
using Lykke.Job.CandlesHistoryWriter.Core.Services.HistoryMigration;
using Lykke.Job.CandlesProducer.Contract;
using Constants = Lykke.Job.CandlesHistoryWriter.Services.Candles.Constants;

namespace Lykke.Job.CandlesHistoryWriter.Services.HistoryMigration
{
    [UsedImplicitly]
    public class MidPriceFixManager
    {
        private readonly IAssetPairsManager _assetPairsManager;
        private readonly ICandlesHistoryRepository _candlesHistoryRepository;
        private readonly ICandlesFiltrationService _candlesFiltrationService;
        public MidPricesFixHealthReport Health;
        private readonly ILog _log;

        public MidPriceFixManager(
            IAssetPairsManager assetPairsManager,
            ICandlesHistoryRepository candlesHistoryRepository,
            ICandlesFiltrationService candlesFiltrationService,
            ILogFactory logFactory
            )
        {
            _assetPairsManager = assetPairsManager;
            _candlesHistoryRepository = candlesHistoryRepository;
            _candlesFiltrationService = candlesFiltrationService;
            _log = logFactory.CreateLog(this);
        }

        public CandlesFiltrationManager.FiltrationLaunchResult FixMidPrices(string assetPairId, bool analyzeOnly)
        {
            // We should not run filtration multiple times before the first attempt ends.
            if (Health != null && Health.State == CandlesFiltrationState.InProgress)
                return CandlesFiltrationManager.FiltrationLaunchResult.AlreadyInProgress;

            // And also we should check if the specified asset pair is enabled.
            var storedAssetPair = _assetPairsManager.TryGetEnabledPairAsync(assetPairId).GetAwaiter().GetResult();
            if (storedAssetPair == null || !_candlesHistoryRepository.CanStoreAssetPair(assetPairId))
                return CandlesFiltrationManager.FiltrationLaunchResult.AssetPairNotSupported;

            _log.Info(nameof(FixMidPrices), $"Starting mid candles price fix for {assetPairId}...");

            Health = new MidPricesFixHealthReport(assetPairId, analyzeOnly);

            var tasks = new List<Task>
            {
                FixMidPricesAsync(assetPairId, storedAssetPair.Accuracy, analyzeOnly)
            };

            Task.WhenAll(tasks.ToArray()).ContinueWith(t =>
            {
                Health.State = CandlesFiltrationState.Finished;

                if (analyzeOnly)
                    _log.Info(nameof(FixMidPrices),
                        $"Mid candle prices fix for {assetPairId} finished: analyze only. Total amount of corrupted candles: {Health.CorruptedCandlesCount}, " +
                        $" errors count: {Health.Errors.Count}.");
                else
                    _log.Info(nameof(FixMidPrices),
                        $"Mid candle prices fix for {assetPairId} finished. Total amount of corrupted candles: {Health.CorruptedCandlesCount}, " +
                        $"errors count: {Health.Errors.Count}.");
            });

            return CandlesFiltrationManager.FiltrationLaunchResult.Started;
        }

        private async Task FixMidPricesAsync(string assetPairId, int assetPairAccuracy, bool analyzeOnly)
        {
            try
            {
                _log.Info(nameof(FixMidPricesAsync), $"Starting mid candle prices fix for {assetPairId}...");

                Health.Message = "Getting candles to fix...";
                var candles = await TryGetCorruptedCandlesAsync(assetPairId, assetPairAccuracy);

                if (!candles.Any())
                {
                    _log.Info(nameof(FixMidPricesAsync),
                        $"There are no candles to fix for {assetPairId}. Skipping.");
                    return;
                }

                if (analyzeOnly)
                {
                    _log.Info(nameof(FixMidPricesAsync),
                        $"Mid candle prices fix for {assetPairId} finished: analyze only. Candles to fix: {Health.CorruptedCandlesCount}");
                    return;
                }

                //sort by interval
                var candlesByInterval = candles
                    .GroupBy(c => (int)c.TimeInterval)
                    .ToSortedDictionary(g => g.Key);

                Health.Message = "Fixing candles from Sec to bigger intervals...";

                //replace candles from Sec to bigger intervals
                foreach (var candleBatch in candlesByInterval)
                {
                    var candlesToReplace = candleBatch.Value.ToList();
                    await _candlesHistoryRepository.ReplaceCandlesAsync(candlesToReplace);
                    Health.FixedCandlesCount += candlesToReplace.Count;
                }

                _log.Info(nameof(FixMidPricesAsync),
                    $"Mid candle prices fix for {assetPairId} finished.");
            }
            catch (Exception ex)
            {
                Health.Errors.Add($"{assetPairId}: {ex.Message}");
                _log.Error(nameof(FixMidPricesAsync), ex);
            }
        }

        private async Task<IReadOnlyList<ICandle>> TryGetCorruptedCandlesAsync(string assetPairId, int assetPairAccuracy)
        {
            var (dateFrom, dateTo) = await _candlesFiltrationService.GetDateTimeRangeAsync(assetPairId, CandlePriceType.Mid);

            if (dateFrom == DateTime.MinValue || dateTo == DateTime.MinValue)
                return new List<ICandle>();

            var currentMonthBeginingDateTime = dateTo;

            var result = new List<ICandle>();

            var (bidMonthCandles, askMonthCandles, midMonthCandles) = await GetCandlesAsync(assetPairId, CandleTimeInterval.Month, dateFrom, dateTo);

            IReadOnlyList<ICandle> lastCandles = GetCorruptedCandlesWithFixedData(bidMonthCandles, askMonthCandles, midMonthCandles, assetPairAccuracy);

            // There are no incorrect candles at all - returning.
            if (!lastCandles.Any())
                return result;

            result.AddRange(lastCandles);

            Health.CorruptedCandlesCount += lastCandles.Count;

            for (var i = Constants.DbStoredIntervals.Length - 2; i >= 0; i--)
            {
                var currentCandles = new List<ICandle>();
                var interval = Constants.DbStoredIntervals[i];

                Health.CurrentInterval = interval;

                foreach (var candle in lastCandles)
                {
                    dateFrom = candle.Timestamp.TruncateTo(interval); // Truncating is important when searching weeks by months: the first week of month may start earlier than the month. In other cases, TruncateTo is redundant.
                    dateTo = candle.Timestamp.AddIntervalTicks(1, GetBiggerInterval(interval));
                    if (dateTo > currentMonthBeginingDateTime)
                        dateTo = currentMonthBeginingDateTime;

                    Health.DateFrom = dateFrom;
                    Health.DateTo = dateTo;

                    var (bidCandles, askCandles, midCandles) = await GetCandlesAsync(assetPairId, interval, dateFrom, dateTo);

                    var corruptedCandles = GetCorruptedCandlesWithFixedData(bidCandles, askCandles, midCandles, assetPairAccuracy);
                    Health.CorruptedCandlesCount += corruptedCandles.Count;
                    currentCandles.AddRange(corruptedCandles);
                }

                lastCandles = currentCandles;
                result.AddRange(currentCandles);
            }

            return result;
        }

        private List<ICandle> GetCorruptedCandlesWithFixedData(IReadOnlyList<ICandle> bidCandles, IReadOnlyList<ICandle> askCandles,
            IReadOnlyList<ICandle> midCandles, int assetPairAccuracy)
        {
            var result = new List<ICandle>();

            foreach (var midCandle in midCandles)
            {
                var askCandle = askCandles.FirstOrDefault(x => x.Timestamp == midCandle.Timestamp);
                var bidCandle = bidCandles.FirstOrDefault(x => x.Timestamp == midCandle.Timestamp);


                if (askCandle == null || bidCandle == null)
                    continue;

                var fixedCandle = GetCorruptedCandleWithFixedData(midCandle, askCandle, bidCandle, assetPairAccuracy);

                if (fixedCandle == null)
                    continue;

                result.Add(fixedCandle);
            }

            return result;
        }

        private async Task<(IReadOnlyList<ICandle> bidCandles, IReadOnlyList<ICandle> askCandles, IReadOnlyList<ICandle> midCandles)>
            GetCandlesAsync(string assetPairId, CandleTimeInterval interval, DateTime dateFrom, DateTime dateTo)
        {
            var midCandlesTask = _candlesHistoryRepository.GetCandlesAsync(assetPairId, interval,
                CandlePriceType.Mid, dateFrom, dateTo);
            var askCandlesTask = _candlesHistoryRepository.GetCandlesAsync(assetPairId, interval,
                CandlePriceType.Ask, dateFrom, dateTo);
            var bidCandlesTask = _candlesHistoryRepository.GetCandlesAsync(assetPairId, interval,
                CandlePriceType.Bid, dateFrom, dateTo);

            await Task.WhenAll(midCandlesTask, askCandlesTask, bidCandlesTask);

            return (bidCandles: bidCandlesTask.Result.ToList(), askCandles: askCandlesTask.Result.ToList(),
                midCandles: midCandlesTask.Result.ToList());
        }

        private Candle GetCorruptedCandleWithFixedData(ICandle midCandle, ICandle askCandle, ICandle bidCandle, int assetPairAccuracy)
        {
            var openPrice = Math.Round((askCandle.Open + bidCandle.Open) / 2, assetPairAccuracy);
            var closePrice = Math.Round((askCandle.Close + bidCandle.Close) / 2, assetPairAccuracy);
            var highPrice = Math.Round((askCandle.High + bidCandle.High) / 2, assetPairAccuracy);
            var lowPrice = Math.Round((askCandle.Low + bidCandle.Low) / 2, assetPairAccuracy);

            var delta = Math.Pow(10, -assetPairAccuracy);

            bool isCandleCorrupted = Math.Abs(midCandle.Open - openPrice) >= delta || Math.Abs(midCandle.Close - closePrice) >= delta ||
                                     Math.Abs(midCandle.High - highPrice) >= delta || Math.Abs(midCandle.Low - lowPrice) >= delta;

            if (isCandleCorrupted)
            {
                return Candle.Create(midCandle.AssetPairId, midCandle.PriceType, midCandle.TimeInterval, midCandle.Timestamp,
                    openPrice, closePrice, highPrice, lowPrice, midCandle.TradingVolume, midCandle.TradingOppositeVolume,
                    midCandle.LastTradePrice, midCandle.LastUpdateTimestamp);
            }

            return null;
        }

        private static CandleTimeInterval GetBiggerInterval(CandleTimeInterval interval)
        {
            if (!Constants.DbStoredIntervals.Contains(interval))
                throw new ArgumentException($"The candle of the given time interval {interval} can not be stored.");

            if (interval == CandleTimeInterval.Month)
                throw new ArgumentException($"There is no bigger stored candle time interval for {interval}.");

            var index = Constants.DbStoredIntervals.IndexOf(interval);
            return Constants.DbStoredIntervals[index + 1];
        }
    }
}
