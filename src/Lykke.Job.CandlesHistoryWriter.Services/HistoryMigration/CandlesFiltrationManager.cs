using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Common;
using Common.Log;
using JetBrains.Annotations;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.Candles;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.HistoryMigration.Filtration;
using Lykke.Job.CandlesProducer.Contract;
using Constants = Lykke.Job.CandlesHistoryWriter.Services.Candles.Constants;

namespace Lykke.Job.CandlesHistoryWriter.Services.HistoryMigration
{
    [UsedImplicitly]
    public class CandlesFiltrationManager
    {
        private readonly ICandlesHistoryRepository _candlesHistoryRepository;
        private readonly ILog _log;

        public CandlesFiltrationHealthReport Health;

        public CandlesFiltrationManager(
            ICandlesHistoryRepository candlesHistoryRepository,
            ILog log
        )
        {
            _candlesHistoryRepository = candlesHistoryRepository;
            _log = log;

            Health = null;
        }

        public bool Filtrate(ICandlesFiltrationRequest request, bool analyzeOnly)
        {
            // We should not run filtration multiple times before the first attempt ends.
            if (Health != null && Health.State == CandlesFiltrationState.InProgress)
                return false;

            _log.WriteInfo(nameof(CandlesFiltrationManager), nameof(Filtrate),
                $"Starting candles with extreme price filtration for {request.AssetId}...");

            Health = new CandlesFiltrationHealthReport(request.AssetId, request.LimitLow, request.LimitHigh, analyzeOnly);

            var priceTypeTasks = new List<Task>();
            foreach (var priceType in Constants.StoredPriceTypes)
            {
                priceTypeTasks.Add(
                    Task.Run(() => 
                        DoFiltrateAsync(request.AssetId, request.LimitLow, request.LimitHigh, priceType, analyzeOnly)
                            .GetAwaiter()
                            .GetResult()));
            }

            Task.WaitAll(priceTypeTasks.ToArray());

            Health.State = CandlesFiltrationState.Finished;

            _log.WriteInfo(nameof(CandlesFiltrationManager), nameof(Filtrate),
                $"Filtration for {request.AssetId} finished. Total amount of deleted Sec candles: {Health.DeletedCandlesCount.Values.Sum()}, " +
                $"total amount of replaced bigger candles: {Health.ReplacedCandlesCount.Values.Sum()}. Errors count: {Health.Errors.Count}.");

            return true;
        }

        private async Task DoFiltrateAsync(string assetId, double limitLow, double limitHigh, CandlePriceType priceType, bool analyzeOnly)
        {
            try
            {
                await _log.WriteInfoAsync(nameof(CandlesFiltrationManager), nameof(DoFiltrateAsync),
                    $"Starting candles with extreme price filtration for price type {priceType}...");

                // Calculating the earliest and the latest dates for candle fetching
                var firstBiggestCandle =
                    await _candlesHistoryRepository.TryGetFirstCandleAsync(assetId,
                        Constants.StoredIntervals.Last(),
                        priceType);
                if (firstBiggestCandle == null)
                    return;

                var dateFrom = firstBiggestCandle.Timestamp; // The first candle's in storage timestamp
                var dateTo = DateTime.UtcNow.AddDays(-DateTime.UtcNow.Day).TruncateTo(CandleTimeInterval.Day)
                    .AddDays(1); // The last day of the prevous month (since now)
                var prevMonthLastDate = dateTo; // The same, but we will use it several times below

                // The list of extreme candles for all stored time periods (there will be not so disastrous amount of them
                // to run out of memory).
                var extremeCandles = new List<ICandle>();
                List<ICandle> currentCandles = null;
                List<ICandle> lastCandles = null;

                // The predicate we will use to find extreme candles
                var funcIsExtremeCandle = new Func<ICandle, bool>(candleToTest =>
                {
                    if (candleToTest.Open  < Constants.Epsilon ||
                        candleToTest.Close < Constants.Epsilon ||
                        candleToTest.High  < Constants.Epsilon ||
                        candleToTest.Low   < Constants.Epsilon)
                        return true;

                    if (candleToTest.Open > limitHigh   || candleToTest.Open < limitLow)
                        return true;

                    if (candleToTest.Close > limitHigh  || candleToTest.Close < limitLow)
                        return true;

                    if (candleToTest.High > limitHigh   || candleToTest.High < limitLow)
                        return true;

                    return candleToTest.Low > limitHigh || candleToTest.Low < limitLow;
                });

                // Now we will go through the candle storage deeps from the biggest candle time interval to the smallest one.
                // We can take in mind that if there was an extreme quote (or trade price), it will lead to generation of the
                // faulty (extreme) candles from Second to Month. But we have much less month canldes than those for seconds.
                // So, the fastest way to find all the extreme second candles is to find all the extreme month candles first.
                // Then, having the list, we need to iterate it to find an extreme week candles, corresponding to each month,
                // and then a list of day candles, and so on. At the finish, we will obtain all the second candles with wrong
                // prices, and thus, we will be ready to delete 'em and go back through the stored time period list, from the
                // smallest to the biggest, producing the updated (corrected) candles for each of them.

                for (var i = Constants.StoredIntervals.Length - 1; i >= 0; i--)
                {
                    var interval = Constants.StoredIntervals[i];
                    if (i == Constants.StoredIntervals.Length - 1)
                    {
                        var candles = await _candlesHistoryRepository.GetCandlesAsync(assetId, interval,
                            priceType, dateFrom, dateTo);

                        currentCandles = candles
                            .Where(c => funcIsExtremeCandle(c))
                            .ToList();

                        // There are no incorrect candles at all - returning.
                        if (!currentCandles.Any())
                        {
                            await _log.WriteInfoAsync(nameof(CandlesFiltrationManager), nameof(DoFiltrateAsync),
                                $"Filtration for price type {priceType} finished. Nothing to filtrate.");
                            return;
                        }
                    }
                    else
                    {
                        currentCandles = new List<ICandle>();
                        // ReSharper disable once PossibleNullReferenceException
                        foreach (var candle in lastCandles)
                        {
                            dateFrom = candle.Timestamp.TruncateTo(interval);
                            dateTo = candle.LastUpdateTimestamp.TruncateTo(interval).AddSeconds((int) interval);
                            if (dateTo > prevMonthLastDate)
                                dateTo = prevMonthLastDate;

                            var candles = await _candlesHistoryRepository.GetCandlesAsync(assetId, interval,
                                priceType, dateFrom, dateTo);

                            currentCandles.AddRange(
                                candles
                                    .Where(c => funcIsExtremeCandle(c))
                                    .ToList());
                        }
                    }

                    lastCandles = currentCandles;
                    extremeCandles.AddRange(currentCandles);
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

                // Okay, the incorrect candles is listed. Now we need to group it by time intervals and iterate from the
                // smallest interval to the biggest, back. But now we will delete the smallest candles at all, and then
                // recalculate (and replace in storage) the bigger candles.

                var candlesByInterval = extremeCandles
                    .GroupBy(c => c.TimeInterval)
                    .ToSortedDictionary(g => g.Key);
                
                var k = 0;
                foreach (var candleBatch in candlesByInterval)
                {
                    var interval = Constants.StoredIntervals[k++];

                    if (interval == CandleTimeInterval.Sec)
                    {
                        var countDeleted =
                            await _candlesHistoryRepository.DeleteCandlesAsync(candleBatch.Value.AsEnumerable());
                        Health.DeletedCandlesCount[priceType] += countDeleted;
                        continue;
                    }

                    var smallerInterval =
                        Constants.StoredIntervals[k - 2]; // The previuos interval (k was incremented above)

                    // The following is important while calculating month candles: we can't derive em from week ones for week
                    // may start/stop not in borders of the month.
                    if (interval == CandleTimeInterval.Month &&
                        smallerInterval == CandleTimeInterval.Week)
                        smallerInterval = CandleTimeInterval.Day;

                    // My dear friend, who will read or refactor this code in future, please, excuse me for such a terrible
                    // loop cascade. Currently, I just can't imagine how to make it shorter and more comfortable for reading.
                    // ReSharper disable once PossibleNullReferenceException
                    currentCandles.Clear();
                    foreach (var candle in candleBatch.Value.AsEnumerable())
                    {
                        dateFrom = candle.Timestamp;
                        dateTo = candle.LastUpdateTimestamp.TruncateTo(smallerInterval)
                            .AddSeconds((int) smallerInterval);

                        var smallerCandles = await _candlesHistoryRepository.GetCandlesAsync(candle.AssetPairId,
                            smallerInterval, priceType, dateFrom, dateTo);

                        ICandle updatedCandle = null;
                        foreach (var smallerCandle in smallerCandles)
                        {
                            if (updatedCandle == null)
                                updatedCandle = Candle
                                    .Copy(smallerCandle)
                                    .RebaseToInterval(interval);
                            else
                                updatedCandle = updatedCandle
                                    .ExtendBy(smallerCandle
                                        .RebaseToInterval(interval));
                        }

                        if (updatedCandle != null)
                            currentCandles.Add(updatedCandle);
                    }

                    var countReplaced = await _candlesHistoryRepository.ReplaceCandlesAsync(currentCandles);
                    Health.ReplacedCandlesCount[priceType] += countReplaced;
                }

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
