using Common;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.Candles;
using Lykke.Job.CandlesHistoryWriter.Core.Services.HistoryMigration;
using Lykke.Job.CandlesProducer.Contract;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Constants = Lykke.Job.CandlesHistoryWriter.Services.Candles.Constants;

namespace Lykke.Job.CandlesHistoryWriter.Services.HistoryMigration
{
    public class CandlesFiltrationService : ICandlesFiltrationService
    {
        private readonly ICandlesHistoryRepository _candlesHistoryRepository;

        public CandlesFiltrationService(ICandlesHistoryRepository candlesHistoryRepository)
        {
            _candlesHistoryRepository = candlesHistoryRepository;
        }

        public async Task<IReadOnlyList<ICandle>> TryGetExtremeCandlesAsync(string assetPairId, CandlePriceType priceType, double limitLow, double limitHigh, double epsilon)
        {
            var (dateFrom, dateTo) = await GetDateTimeRangeAsync(assetPairId, priceType);

            // May be, we have got fake DateTime range. If so, this means there are no candles for the specified asset
            // pair at all and we should skip its filtration in the caller code. Return null.
            if (dateFrom == DateTime.MinValue || dateTo == DateTime.MinValue)
                return new List<ICandle>();

            var currentMonthBeginingDateTime = dateTo; // We will use it several times below for data query limiting.

            // The list of extreme candles for all stored time periods (there will be not so disastrous amount of them
            // to run out of memory).
            var extremeCandles = new List<ICandle>();
            List<ICandle> lastCandles = null;

            // Now we will go through the candle storage deeps from the biggest candle time interval to the smallest one.
            // We can take in mind that if there was an extreme quote (or trade price), it will lead to generation of the
            // faulty (extreme) candles from Second to Month. But we have much less month candles than those for seconds.
            // So, the fastest way to find all the extreme second candles is to find all the extreme month candles first.
            // Then, having the list, we need to iterate it to find an extreme week candles, corresponding to each month,
            // and then a list of day candles, and so on. At the finish, we will obtain all the second candles with wrong
            // prices, and thus, we will be ready to delete 'em and go back through the stored time period list, from the
            // smallest to the biggest, producing the updated (corrected) candles for each of them.

            for (var i = Constants.StoredIntervals.Length - 1; i >= 0; i--)
            {
                List<ICandle> currentCandles = null;

                var interval = Constants.StoredIntervals[i];
                if (i == Constants.StoredIntervals.Length - 1)
                {
                    var candles = await _candlesHistoryRepository.GetCandlesAsync(assetPairId, interval,
                        priceType, dateFrom, dateTo);

                    currentCandles = candles
                        .Where(c => IsExtremeCandle(c, limitLow, limitHigh, epsilon))
                        .ToList();

                    // There are no incorrect candles at all - returning.
                    if (!currentCandles.Any())
                        return extremeCandles; // Empty collection here.
                }
                else
                {
                    currentCandles = new List<ICandle>();
                    // ReSharper disable once PossibleNullReferenceException
                    foreach (var candle in lastCandles)
                    {
                        dateFrom = candle.Timestamp.TruncateTo(interval); // Truncating is important when searching weeks by months: the first week of month may start earlier than the month. In other cases, TruncateTo is redundant.
                        dateTo = candle.Timestamp.AddIntervalTicks(1, GetBiggerInterval(interval));
                        if (dateTo > currentMonthBeginingDateTime)
                            dateTo = currentMonthBeginingDateTime;

                        var candles = await _candlesHistoryRepository.GetCandlesAsync(assetPairId, interval, priceType, dateFrom, dateTo);

                        currentCandles.AddRange(candles
                            .Where(c => IsExtremeCandle(c, limitLow, limitHigh, epsilon)));
                    }
                }

                lastCandles = currentCandles;
                extremeCandles.AddRange(currentCandles);
            }

            return extremeCandles;
        }

        public async Task<(int deletedCandlesCount, int replacedCandlesCount)> FixExtremeCandlesAsync(IReadOnlyList<ICandle> extremeCandles, CandlePriceType priceType)
        {
            // Okay, the incorrect candles are listed. Now we need to group it by time intervals and iterate from the
            // smallest interval to the biggest, back. But now we will delete the smallest candles at all, and then
            // recalculate (and replace in storage) the bigger candles.

            int deletedCountSummary = 0, replacedCountSummary = 0;
            
            var candlesByInterval = extremeCandles
                .GroupBy(c => (int)c.TimeInterval)
                .ToSortedDictionary(g => g.Key);

            if (candlesByInterval.Count != Constants.StoredIntervals.Length)
                throw new ArgumentException($"Something is wrong: the amount of (unique) time intervals in extreme candles list is not equal to stored intervals array length. " +
                    $"Filtration for {priceType} is impossible.");

            foreach (var candleBatch in candlesByInterval)
            {
                var interval = (CandleTimeInterval)candleBatch.Key;

                // The Second time interval is the smallest, so, we simply delete such a candles from storage and go next.
                if (interval == CandleTimeInterval.Sec)
                {
                    var deletedCountQuant =
                        await _candlesHistoryRepository.DeleteCandlesAsync(candleBatch.Value.ToList());
                    deletedCountSummary += deletedCountQuant;
                    continue;
                }

                var smallerInterval = GetSmallerInterval(interval);

                // My dear friend, who will read or refactor this code in future, please, excuse me for such a terrible
                // loop cascade. Currently, I just can't imagine how to make it shorter and more comfortable for reading.
                // And from the other side, there is no such a necessity to invent an ideal bicycle here.
                // ReSharper disable once PossibleNullReferenceException
                var currentCandlesToReplace = new List<ICandle>();
                var currentCandlesToDelete = new List<ICandle>();
                foreach (var candle in candleBatch.Value.AsEnumerable())
                {
                    var dateFrom = candle.Timestamp;
                    var dateTo = candle.Timestamp.AddIntervalTicks(1, interval);

                    var smallerCandles = await _candlesHistoryRepository.GetCandlesAsync(candle.AssetPairId,
                        smallerInterval, priceType, dateFrom, dateTo);

                    // Trying to reconstruct the candle from the corresponfing smaller interval candles, if any.
                    ICandle updatedCandle = null;
                    foreach (var smallerCandle in smallerCandles)
                    {
                        if (updatedCandle == null)
                            updatedCandle = smallerCandle
                                .RebaseToInterval(interval);
                        else
                            updatedCandle = updatedCandle
                                .ExtendBy(smallerCandle
                                    .RebaseToInterval(interval));
                    }

                    // If the candle is not empty (e.g., there are some smaller interval candles for its construction),
                    // we should update it in storage. Otherwise, it is to be deleted from there.
                    if (updatedCandle != null)
                        currentCandlesToReplace.Add(updatedCandle);
                    else
                        currentCandlesToDelete.Add(candle);
                }

                if (currentCandlesToDelete.Count > 0)
                {
                    var deletedCountQuant = await _candlesHistoryRepository.DeleteCandlesAsync(currentCandlesToDelete);
                    deletedCountSummary += deletedCountQuant;
                }

                if (currentCandlesToReplace.Count > 0)
                {
                    var replacedCountQuant = await _candlesHistoryRepository.ReplaceCandlesAsync(currentCandlesToReplace);
                    replacedCountSummary += replacedCountQuant;
                }
            }

            return (deletedCandlesCount: deletedCountSummary, replacedCandlesCount: replacedCountSummary);
        }

        #region "Private"

        // Calculating the earliest and the latest dates for the biggest interval candles fetching
        private async Task<(DateTime dateFrom, DateTime dateTo)> GetDateTimeRangeAsync(string assetPairId, CandlePriceType priceType)
        {
            var firstBiggestCandle =
                await _candlesHistoryRepository.TryGetFirstCandleAsync(assetPairId,
                    Constants.StoredIntervals.Last(),
                    priceType);

            // If we have no such a candle in repository, we return fake dates for further decision making in the caller code
            if (firstBiggestCandle == null)
                return (dateFrom: DateTime.MinValue, dateTo: DateTime.MinValue);

            var dtFrom = firstBiggestCandle.Timestamp; // The first candle's in storage timestamp
            var dtTo = DateTime.UtcNow.TruncateTo(CandleTimeInterval.Month); // The begining of the current month (for further candles reading with exclusive upper date-time border).

            return (dateFrom: dtFrom, dateTo: dtTo);
        }

        // The function we will use as predicate in LINQ to find extreme candles
        private bool IsExtremeCandle(ICandle candleToTest, double limitLow, double limitHigh, double epsilon)
        {
            if (candleToTest.Open   < epsilon ||
                candleToTest.Close  < epsilon ||
                candleToTest.High   < epsilon ||
                candleToTest.Low    < epsilon)
                return true;

            if (candleToTest.Open > limitHigh || candleToTest.Open < limitLow)
                return true;

            if (candleToTest.Close > limitHigh || candleToTest.Close < limitLow)
                return true;

            if (candleToTest.High > limitHigh || candleToTest.High < limitLow)
                return true;

            return candleToTest.Low > limitHigh || candleToTest.Low < limitLow;
        }

        private static CandleTimeInterval GetSmallerInterval(CandleTimeInterval interval)
        {
            if (!Constants.StoredIntervals.Contains(interval))
                throw new ArgumentException($"The candle of the given time interval {interval} can not be stored.");

            // The following is important while calculating month candles: we can't derive em from week ones for week
            // may start/stop not in borders of the month.
            if (interval == CandleTimeInterval.Month)
                return CandleTimeInterval.Day;
            
            if (interval == CandleTimeInterval.Sec)
                throw new ArgumentException($"There is no smaller stored candle time interval for {interval}.");

            var index = Constants.StoredIntervals.IndexOf(interval);
            return Constants.StoredIntervals[index - 1];
        }

        private static CandleTimeInterval GetBiggerInterval(CandleTimeInterval interval)
        {
            if (!Constants.StoredIntervals.Contains(interval))
                throw new ArgumentException($"The candle of the given time interval {interval} can not be stored.");

            if (interval == CandleTimeInterval.Month)
                throw new ArgumentException($"There is no bigger stored candle time interval for {interval}.");

            var index = Constants.StoredIntervals.IndexOf(interval);
            return Constants.StoredIntervals[index + 1];
        }

        #endregion
    }
}
