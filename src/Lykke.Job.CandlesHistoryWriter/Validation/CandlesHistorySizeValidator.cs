using System;
using Common;
using JetBrains.Annotations;
using Lykke.Job.CandlesProducer.Contract;

namespace Lykke.Job.CandlesHistoryWriter.Validation
{
    [UsedImplicitly]
    public class CandlesHistorySizeValidator
    {
        public int MaxCandlesCountWhichCanBeRequested { get; }

        public CandlesHistorySizeValidator(int maxCandlesCountWhichCanBeRequested)
        {
            MaxCandlesCountWhichCanBeRequested = maxCandlesCountWhichCanBeRequested;
        }

        public bool CanBeRequested(DateTime fromDate, DateTime toDate, CandleTimeInterval timeInterval, int factor = 1)
        {
            var ticksCount = GetIntervalTicksCount(fromDate, toDate, timeInterval);

            return ticksCount * factor <= MaxCandlesCountWhichCanBeRequested;
        }

        private static int GetIntervalTicksCount(DateTime fromDate, DateTime toDate, CandleTimeInterval timeInterval)
        {
            switch (timeInterval)
            {
                case CandleTimeInterval.Month:
                    return (toDate.Year - fromDate.Year) * 12 - fromDate.Month + toDate.Month + 1;
                case CandleTimeInterval.Day:
                    return (int) (toDate - fromDate).TotalDays + 1;
                case CandleTimeInterval.Week:
                    return (int) (toDate - DateTimeUtils.GetFirstWeekOfYear(fromDate)).TotalDays / 7 + 1;
                case CandleTimeInterval.Hour12:
                    return (int) (toDate - fromDate).TotalHours / 12 + 1;
                case CandleTimeInterval.Hour6:
                    return (int)(toDate - fromDate).TotalHours / 6 + 1;
                case CandleTimeInterval.Hour4:
                    return (int)(toDate - fromDate).TotalHours / 4 + 1;
                case CandleTimeInterval.Hour:
                    return (int)(toDate - fromDate).TotalHours + 1;
                case CandleTimeInterval.Min30:
                    return (int)(toDate - fromDate).TotalMinutes / 30 + 1;
                case CandleTimeInterval.Min15:
                    return (int)(toDate - fromDate).TotalMinutes / 15 + 1;
                case CandleTimeInterval.Min5:
                    return (int)(toDate - fromDate).TotalMinutes / 5 + 1;
                case CandleTimeInterval.Minute:
                    return (int)(toDate - fromDate).TotalMinutes + 1;
                case CandleTimeInterval.Sec:
                    return (int)(toDate - fromDate).TotalSeconds + 1;
                default:
                    throw new ArgumentOutOfRangeException(nameof(timeInterval), timeInterval, "Unexpected TimeInterval value.");
            }
        }
    }
}
