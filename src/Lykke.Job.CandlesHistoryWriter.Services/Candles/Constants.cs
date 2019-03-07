﻿using System.Collections.Immutable;
using Lykke.Job.CandlesProducer.Contract;

namespace Lykke.Job.CandlesHistoryWriter.Services.Candles
{
    public static class Constants
    {
        public static readonly ImmutableArray<CandleTimeInterval> DbStoredIntervals = ImmutableArray.Create
        (
            CandleTimeInterval.Minute,
            CandleTimeInterval.Hour,
            CandleTimeInterval.Day,
            CandleTimeInterval.Week,
            CandleTimeInterval.Month
        );

        public static readonly ImmutableArray<CandlePriceType> StoredPriceTypes = ImmutableArray.Create
        (
            CandlePriceType.Ask,
            CandlePriceType.Bid,
            CandlePriceType.Mid,
            CandlePriceType.Trades
        );
    }
}
