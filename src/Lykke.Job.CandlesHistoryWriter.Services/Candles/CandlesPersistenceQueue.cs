using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Common;
using Common.Log;
using JetBrains.Annotations;
using Lykke.Job.CandlesHistoryWriter.Core.Domain;
using Lykke.Job.CandlesProducer.Contract;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.Candles;
using Lykke.Job.CandlesHistoryWriter.Core.Services;
using Lykke.Job.CandlesHistoryWriter.Core.Services.Candles;
using Lykke.Job.CandlesHistoryWriter.Services.Settings;
using Polly;

namespace Lykke.Job.CandlesHistoryWriter.Services.Candles
{
    [UsedImplicitly(ImplicitUseKindFlags.InstantiatedNoFixedConstructorSignature)]
    public class CandlesPersistenceQueue : 
        ProducerConsumer<IReadOnlyCollection<ICandle>>,
        ICandlesPersistenceQueue
    {
        private readonly ICandlesHistoryRepository _repository;
        private readonly ILog _log;
        private readonly IHealthService _healthService;
        private readonly PersistenceSettings _settings;
        private readonly StorageMode _storageMode;

        // TODO: Make dictionary with timestamp, assetPair, priceType, timeInterval key 
        // store only the last state of the candle, and thus, reduce memory consumtion

        private ConcurrentQueue<ICandle> _candlesToDispatch;
        
        public CandlesPersistenceQueue(
            ICandlesHistoryRepository repository,
            ILog log,
            IHealthService healthService,
            PersistenceSettings settings,
            StorageMode stmP) :

            base(nameof(CandlesPersistenceQueue), log)
        {
            _repository = repository;
            _log = log;
            _healthService = healthService;
            _settings = settings;
            _candlesToDispatch = new ConcurrentQueue<ICandle>();
            _storageMode = stmP;

        }

        public void EnqueueCandle(ICandle candle)
        {
            if (_healthService.CandlesToDispatchQueueLength > _settings.CandlesToDispatchLengthThrottlingThreshold)
            {
                Task.Delay(_settings.ThrottlingEnqueueDelay).GetAwaiter().GetResult();
            }

            _candlesToDispatch.Enqueue(candle);

            _healthService.TraceEnqueueCandle();
        }

        public IImmutableList<ICandle> GetState()
        {
            return _candlesToDispatch.ToArray().ToImmutableList();
        }

        public void SetState(IImmutableList<ICandle> state)
        {
            if (_candlesToDispatch.Count > 0)
            {
                throw new InvalidOperationException("Queue state can't be set when queue already not empty");
            }

            _candlesToDispatch = new ConcurrentQueue<ICandle>(state);

            _healthService.TraceSetPersistenceQueueState(state.Count);
        }

        public string DescribeState(IImmutableList<ICandle> state)
        {
            return $"Candles: {state.Count}";
        }

        public void DispatchCandlesToPersist(int maxBatchSize)
        {
            var candlesCount = _candlesToDispatch.Count;

            if (candlesCount == 0)
            {
                return;
            }

            candlesCount = Math.Min(candlesCount, maxBatchSize);

            var candles = new List<ICandle>(candlesCount);

            for (var i = 0; i < candlesCount; i++)
            {
                if (_candlesToDispatch.TryDequeue(out var candle))
                {
                    candles.Add(candle);
                }
                else
                {
                    break;
                }
            }

            _healthService.TraceCandlesBatchDispatched(candles.Count);

            // Add candles to producer/consumer's queue
            Produce(candles);
        }

        protected override async Task Consume(IReadOnlyCollection<ICandle> candles)
        {
            var sw = Stopwatch.StartNew();

            try
            {
                await PersistCandles(candles);
            }
            finally
            {
                _healthService.TraceCandlesBatchPersisted(candles.Count);
            }

            sw.Stop();

            await _log.WriteInfoAsync("Persist candles batch", string.Empty, 
                $"Candles batch with {candles.Count} candles is persisted in {sw.Elapsed}. Amount of batches in queue = {_healthService.BatchesToPersistQueueLength}. Amount of candles to dispath = {_healthService.CandlesToDispatchQueueLength}");
        }

        private async Task PersistCandles(IReadOnlyCollection<ICandle> candles)
        {
            if (!candles.Any())
            {
                return;
            }

            _healthService.TraceStartPersistCandles();

            try
            {
                if (_storageMode == StorageMode.SqlServer)
                {
                    var priceType = candles.FirstOrDefault().PriceType;
                    var timeInterval = candles.FirstOrDefault().TimeInterval;
                  var grouppedCandles = candles
                        .GroupBy(c => new
                        {
                            c.AssetPairId
                        });
                    var tasks = grouppedCandles
                        .Select(g => InsertSinglePartitionCandlesAsync(g, g.Key.AssetPairId, priceType, timeInterval));

                    await Task.WhenAll(tasks); }
                else
                {
                    var grouppedCandles = candles
                        .GroupBy(c => new
                        {
                            c.AssetPairId,
                            c.PriceType,
                            c.TimeInterval
                        });
                    var tasks = grouppedCandles
                        .Select(g => InsertSinglePartitionCandlesAsync(g, g.Key.AssetPairId, g.Key.PriceType, g.Key.TimeInterval));

                    await Task.WhenAll(tasks);
                }

               
            }
            finally
            {
                _healthService.TraceStopPersistCandles();
            }
        }

        private Task InsertSinglePartitionCandlesAsync(IEnumerable<ICandle> candles, string assetPairId, CandlePriceType priceType, CandleTimeInterval timeInterval)
        {
            return Policy
                .Handle<Exception>()
                // If we can't store the candles, we can't do anything else, so just retries until success
                .WaitAndRetryForeverAsync(
                    retryAttempt => TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)),
                    (exception, timeSpan) =>
                    {
                        var context = $"{assetPairId}-{priceType}-{timeInterval}";

                        return _log.WriteErrorAsync("Persist single partition candles with retries", context, exception);
                    })
                .ExecuteAsync(() => _repository.InsertOrMergeAsync(
                    candles,
                    assetPairId,
                    priceType,
                    timeInterval));
        }

    }

}
