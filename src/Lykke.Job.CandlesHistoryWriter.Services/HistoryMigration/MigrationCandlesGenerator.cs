using System;
using System.Collections.Concurrent;
using System.Collections.Immutable;
using System.Linq;
using JetBrains.Annotations;
using Lykke.Job.CandlesProducer.Contract;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.Candles;
using Lykke.Job.CandlesHistoryWriter.Core.Services;

namespace Lykke.Job.CandlesHistoryWriter.Services.HistoryMigration
{
    [UsedImplicitly(ImplicitUseKindFlags.InstantiatedNoFixedConstructorSignature)]
    public class MigrationCandlesGenerator : IHaveState<IImmutableDictionary<string, ICandle>>
    {
        private ConcurrentDictionary<string, Candle> _candles;

        public MigrationCandlesGenerator()
        {
            _candles = new ConcurrentDictionary<string, Candle>();
        }

        public MigrationCandleMergeResult Merge(string assetPair, CandlePriceType priceType, CandleTimeInterval timeInterval, DateTime timestamp, double open, double close, double low, double high)
        {
            var key = GetKey(assetPair, timeInterval, priceType);

            Candle oldCandle = null;
            var newCandle = _candles.AddOrUpdate(key,
                addValueFactory: k => Candle.Create(
                    assetPair: assetPair,
                    priceType: priceType,
                    timeInterval: timeInterval,
                    timestamp: timestamp,
                    open: open,
                    close: close,
                    high: high,
                    low: low,
                    tradingVolume: 0,
                    tradingOppositeVolume: 0,
                    lastUpdateTimestamp: timestamp),
                updateValueFactory: (k, old) =>
                {
                    oldCandle = old;

                    // Start new candle?
                    var intervalTimestamp = timestamp.TruncateTo(timeInterval);
                    if (oldCandle.Timestamp != intervalTimestamp)
                    {
                        return Candle.Create(
                            assetPair: assetPair,
                            priceType: priceType,
                            timeInterval: timeInterval,
                            timestamp: intervalTimestamp,
                            open: open,
                            close: close,
                            high: high,
                            low: low,
                            tradingVolume: 0,
                            tradingOppositeVolume: 0,
                            lastUpdateTimestamp: timestamp);
                    }

                    return oldCandle.Update(close, low, high, 0, 0, timestamp);
                });

            return new MigrationCandleMergeResult(newCandle, !newCandle.Equals(oldCandle));
        }

        private static string GetKey(string assetPair, CandleTimeInterval timeInterval, CandlePriceType type)
        {
            return $"{assetPair}-{type}-{timeInterval}";
        }

        public IImmutableDictionary<string, ICandle> GetState()
        {
            return _candles.ToArray().ToImmutableDictionary(i => i.Key, i => (ICandle)i.Value);
        }

        public void SetState(IImmutableDictionary<string, ICandle> state)
        {
            if (_candles.Count > 0)
            {
                throw new InvalidOperationException("Candles generator state already not empty");
            }

            _candles = new ConcurrentDictionary<string, Candle>(state.ToDictionary(
                i => i.Key,
                i => Candle.Copy(i.Value)));
        }

        public string DescribeState(IImmutableDictionary<string, ICandle> state)
        {
            return $"Candles count: {state.Count}";
        }

        public void RemoveAssetPair(string assetPair)
        {
            foreach (var priceType in Candles.Constants.StoredPriceTypes)
            {
                foreach (var timeInterval in Candles.Constants.StoredIntervals)
                {
                    _candles.TryRemove(GetKey(assetPair, timeInterval, priceType), out var _);
                }
            }
        }
    }
}
