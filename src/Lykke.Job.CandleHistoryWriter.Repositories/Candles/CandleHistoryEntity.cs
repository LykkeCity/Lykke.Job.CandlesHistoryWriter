using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using Common;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.Candles;
using Lykke.Job.CandlesProducer.Contract;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using Newtonsoft.Json;

namespace Lykke.Job.CandleHistoryWriter.Repositories.Candles
{
    public class CandleHistoryEntity : ITableEntity
    {
        public CandleHistoryEntity()
        {
        }

        public CandleHistoryEntity(string partitionKey, string rowKey)
        {
            PartitionKey = partitionKey;
            RowKey = rowKey;
            Candles = new List<CandleHistoryItem>();
        }

        #region ITableEntity properties

        public string ETag { get; set; }
        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public DateTimeOffset Timestamp { get; set; }

        #endregion ITableEntity properties

        public DateTime DateTime
        {
            get
            {
                // extract from RowKey + Interval from PKey
                if (!string.IsNullOrEmpty(RowKey))
                {
                    return ParseRowKey(RowKey);
                }
                return default(DateTime);
            }
        }

        public CandlePriceType PriceType
        {
            get
            {
                if (!string.IsNullOrEmpty(PartitionKey))
                {
                    if (Enum.TryParse(PartitionKey, out CandlePriceType value))
                    {
                        return value;
                    }
                }
                return CandlePriceType.Unspecified;
            }
        }

        /// <summary>
        /// Candles, ordered by the tick
        /// </summary>
        public List<CandleHistoryItem> Candles { get; private set; }

        public void ReadEntity(IDictionary<string, EntityProperty> properties, OperationContext operationContext)
        {
            if (properties.TryGetValue("Data", out var property))
            {
                var json = property.StringValue;
                if (!string.IsNullOrEmpty(json))
                {
                    Candles = new List<CandleHistoryItem>(60);
                    Candles.AddRange(JsonConvert.DeserializeObject<IEnumerable<CandleHistoryItem>>(json));
                }
            }
        }

        public IDictionary<string, EntityProperty> WriteEntity(OperationContext operationContext)
        {
            var dict = new Dictionary<string, EntityProperty>
            {
                {"Data", new EntityProperty(JsonConvert.SerializeObject(Candles))}
            };

            return dict;
        }

        public static string GeneratePartitionKey(CandlePriceType priceType)
        {
            return $"{priceType}";
        }

        public static string GenerateRowKey(DateTime date, CandleTimeInterval interval)
        {
            DateTime time;
            switch (interval)
            {
                case CandleTimeInterval.Month:
                    time = new DateTime(date.Year, 1, 1, 0, 0, 0, DateTimeKind.Utc);
                    break;

                case CandleTimeInterval.Week:
                    time = DateTimeUtils.GetFirstWeekOfYear(date);
                    break;

                case CandleTimeInterval.Day:
                    time = new DateTime(date.Year, date.Month, 1, 0, 0, 0, DateTimeKind.Utc);
                    break;

                case CandleTimeInterval.Hour12:
                case CandleTimeInterval.Hour6:
                case CandleTimeInterval.Hour4:
                case CandleTimeInterval.Hour:
                    time = new DateTime(date.Year, date.Month, date.Day, 0, 0, 0, DateTimeKind.Utc);
                    break;

                case CandleTimeInterval.Min30:
                case CandleTimeInterval.Min15:
                case CandleTimeInterval.Min5:
                case CandleTimeInterval.Minute:
                    time = new DateTime(date.Year, date.Month, date.Day, date.Hour, 0, 0, DateTimeKind.Utc);
                    break;

                case CandleTimeInterval.Sec:
                    time = new DateTime(date.Year, date.Month, date.Day, date.Hour, date.Minute, 0, DateTimeKind.Utc);
                    break;

                default:
                    throw new ArgumentOutOfRangeException(nameof(interval), interval, null);
            }

            return FormatRowKey(time);
        }

        public int DeleteCandles(IEnumerable<ICandle> candlesToDelete)
        {
            var ticksToDelete = candlesToDelete
                .Select(c => GetIntervalTick(c.Timestamp, c.TimeInterval))
                .Distinct();

            return Candles.RemoveAll(c => 
                ticksToDelete.Contains(c.Tick));
        }

        public int ReplaceCandles(IEnumerable<ICandle> candlesToReplace)
        {
            var replacedCount = 0;

            foreach (var candle in candlesToReplace)
            {
                var tick = GetIntervalTick(candle.Timestamp, candle.TimeInterval);

                if (Candles.RemoveAll(c => c.Tick == tick) <= 0)
                    continue; // Can't replace if there was no candle with the requested tick.

                Candles.Add(candle.ToItem(tick));
                replacedCount++;
            }

            // Sorting candles for storing in DB in proper order.
            Candles.Sort((a, b) =>
            {
                if (a.Tick > b.Tick)
                    return 1;
                if (a.Tick < b.Tick)
                    return -1;
                return 0;
            });

            return replacedCount;
        }

        public void MergeCandles(IEnumerable<ICandle> candles, string assetPair, CandleTimeInterval timeInterval)
        {
            foreach (var candle in candles)
            {
                MergeCandle(assetPair, timeInterval, candle);
            }
        }

        // ReSharper disable once ParameterOnlyUsedForPreconditionCheck.Local
        private void MergeCandle(string assetPair, CandleTimeInterval interval, ICandle candle)
        {
            if (candle.AssetPairId != assetPair)
            {
                throw new InvalidOperationException($"Candle {candle.ToJson()} has invalid AssetPriceId");
            }
            if (candle.TimeInterval != interval)
            {
                throw new InvalidOperationException($"Candle {candle.ToJson()} has invalid TimeInterval");
            }
            if (candle.PriceType != PriceType)
            {
                throw new InvalidOperationException($"Candle {candle.ToJson()} has invalid PriceType");
            }

            // 1. Check if candle with specified time already exist
            // 2. If found - merge, else - add to list

            var tick = GetIntervalTick(candle.Timestamp, interval);

            // Considering that Candles is ordered by Tick
            for (var i = 0; i < Candles.Count; ++i)
            {
                var currentCandle = Candles[i];

                // While currentCandle.Tick < tick - just skipping

                // That's it, merge to existing candle
                if (currentCandle.Tick == tick)
                {
                    currentCandle.InplaceMergeWith(candle);
                    return;
                }

                // No candle is found but there are some candles after, so we should insert candle right before them
                if (currentCandle.Tick > tick)
                {
                    Candles.Insert(i, candle.ToItem(tick));
                    return;
                }
            }

            // No candle is found, and no candles after, so just add to the end
            Candles.Add(candle.ToItem(tick));
        }

        private static int GetIntervalTick(DateTime dateTime, CandleTimeInterval interval)
        {
            switch (interval)
            {
                case CandleTimeInterval.Month:
                    return dateTime.Month;

                case CandleTimeInterval.Week:
                    return (int)(dateTime - DateTimeUtils.GetFirstWeekOfYear(dateTime)).TotalDays / 7;

                case CandleTimeInterval.Day:
                    return dateTime.Day;

                case CandleTimeInterval.Hour12:
                    return dateTime.Hour / 12;

                case CandleTimeInterval.Hour6:
                    return dateTime.Hour / 6;

                case CandleTimeInterval.Hour4:
                    return dateTime.Hour / 4;

                case CandleTimeInterval.Hour:
                    return dateTime.Hour;

                case CandleTimeInterval.Min30:
                    return dateTime.Minute / 30;

                case CandleTimeInterval.Min15:
                    return dateTime.Minute / 15;

                case CandleTimeInterval.Min5:
                    return dateTime.Minute / 5;

                case CandleTimeInterval.Minute:
                    return dateTime.Minute;

                case CandleTimeInterval.Sec:
                    return dateTime.Second;

                default:
                    throw new ArgumentOutOfRangeException(nameof(interval), interval, "Unexpected TimeInterval value.");
            }
        }

        private static string FormatRowKey(DateTime dateUtc)
        {
            return dateUtc.ToString("s"); // sortable format
        }

        private static DateTime ParseRowKey(string value)
        {
            if (string.IsNullOrEmpty(value))
            {
                throw new ArgumentNullException(nameof(value));
            }

            if (DateTime.TryParseExact(value, "s", DateTimeFormatInfo.InvariantInfo, DateTimeStyles.AdjustToUniversal, out var date))
            {
                return DateTime.SpecifyKind(date, DateTimeKind.Utc);
            }

            if (long.TryParse(value, out var ticks))
            {
                return new DateTime(ticks, DateTimeKind.Utc);
            }

            throw new InvalidOperationException($"Failed to parse RowKey '{value}' as DateTime");
        }
    }
}
