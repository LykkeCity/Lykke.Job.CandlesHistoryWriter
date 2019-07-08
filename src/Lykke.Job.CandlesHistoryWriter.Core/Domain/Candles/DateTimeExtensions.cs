// Copyright (c) 2019 Lykke Corp.
// See the LICENSE file in the project root for more information.

using System;
using Lykke.Job.CandlesProducer.Contract;

namespace Lykke.Job.CandlesHistoryWriter.Core.Domain.Candles
{
    public static class DateTimeExtensions
    {
        /// <summary>
        /// Adds tick value to the specified datetime
        /// </summary>
        public static DateTime AddIntervalTicks(this DateTime baseTime, int ticks, CandleTimeInterval interval)
        {
            switch (interval)
            {
                case CandleTimeInterval.Month:
                    return baseTime.AddMonths(ticks);
                case CandleTimeInterval.Week:
                    return baseTime.AddDays(ticks * 7);
                case CandleTimeInterval.Day:
                    return baseTime.AddDays(ticks);
                case CandleTimeInterval.Hour12:
                    return baseTime.AddHours(ticks * 12);
                case CandleTimeInterval.Hour6:
                    return baseTime.AddHours(ticks * 6);
                case CandleTimeInterval.Hour4:
                    return baseTime.AddHours(ticks * 4);
                case CandleTimeInterval.Hour:
                    return baseTime.AddHours(ticks);
                case CandleTimeInterval.Min30:
                    return baseTime.AddMinutes(ticks * 30);
                case CandleTimeInterval.Min15:
                    return baseTime.AddMinutes(ticks * 15);
                case CandleTimeInterval.Min5:
                    return baseTime.AddMinutes(ticks * 5);
                case CandleTimeInterval.Minute:
                    return baseTime.AddMinutes(ticks);
                case CandleTimeInterval.Sec:
                    return baseTime.AddSeconds(ticks);
                default:
                    throw new ArgumentOutOfRangeException(nameof(interval), interval, "Unexpected TimeInterval value.");
            }
        }
    }
}
