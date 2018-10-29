using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Common;
using Common.Log;
using Lykke.Common.Log;
using Lykke.Job.CandlesProducer.Contract;
using Lykke.Service.Assets.Client.Models;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.Candles;
using Lykke.Job.CandlesHistoryWriter.Core.Services;
using Lykke.Job.CandlesHistoryWriter.Core.Services.Candles;
using Lykke.Job.CandlesHistoryWriter.Core.Services.HistoryMigration;
using Lykke.Job.CandlesHistoryWriter.Core.Services.HistoryMigration.HistoryProviders;
using Lykke.Job.CandlesHistoryWriter.Services.HistoryMigration.Telemetry;
using Lykke.Job.CandlesHistoryWriter.Services.Settings;

namespace Lykke.Job.CandlesHistoryWriter.Services.HistoryMigration
{
    public class AssetPairMigrationManager
    {
        private readonly AssetPairMigrationTelemetryService _telemetryService;
        private readonly AssetPair _assetPair;
        private readonly ILog _log;
        private readonly BidAskHCacheService _bidAskHCacheService;
        private readonly IHistoryProvider _historyProvider;
        private readonly ICandlesHistoryMigrationService _candlesHistoryMigrationService;
        private readonly Action<string> _onStoppedAction;
        private readonly MigrationSettings _settings;
        private readonly CancellationTokenSource _cts;
        private readonly IHealthService _healthService;
        private readonly ICandlesPersistenceQueue _candlesPersistenceQueue;
        private readonly MigrationCandlesGenerator _candlesGenerator;
        private DateTime _prevAskTimestamp;
        private DateTime _prevBidTimestamp;
        private DateTime _prevMidTimestamp;
        private volatile bool _isAskOrBidMigrationCompleted;

        private readonly ImmutableArray<CandleTimeInterval> _intervalsToGenerate = Candles.Constants
            .StoredIntervals
            .Where(i => i != CandleTimeInterval.Sec)
            .ToImmutableArray();

        public AssetPairMigrationManager(
            IHealthService healthService,
            ICandlesPersistenceQueue candlesPersistenceQueue,
            MigrationCandlesGenerator candlesGenerator,
            AssetPairMigrationTelemetryService telemetryService,
            AssetPair assetPair,
            ILogFactory logFactory,
            BidAskHCacheService bidAskHCacheService,
            IHistoryProvider historyProvider,
            ICandlesHistoryMigrationService candlesHistoryMigrationService,
            Action<string> onStoppedAction, 
            MigrationSettings settings)
        {
            _healthService = healthService;
            _candlesPersistenceQueue = candlesPersistenceQueue;
            _candlesGenerator = candlesGenerator;
            _telemetryService = telemetryService;
            _assetPair = assetPair;
            _log = logFactory.CreateLog(this);
            _bidAskHCacheService = bidAskHCacheService;
            _historyProvider = historyProvider;
            _candlesHistoryMigrationService = candlesHistoryMigrationService;
            _onStoppedAction = onStoppedAction;
            _settings = settings;

            _cts = new CancellationTokenSource();
        }

        public void Start()
        {
            Task.Run(() => MigrateAsync().Wait());
        }

        private async Task MigrateAsync()
        {
            try
            {
                _telemetryService.UpdateOverallProgress("Obtaining bid and ask start dates");

                (DateTime? askStartDate, DateTime? bidStartDate) = await GetStartDatesAsync();

                _prevAskTimestamp = askStartDate?.AddSeconds(-1) ?? DateTime.MinValue;
                _prevBidTimestamp = bidStartDate?.AddSeconds(-1) ?? DateTime.MinValue;
                
                _telemetryService.UpdateStartDates(askStartDate, bidStartDate);
                _telemetryService.UpdateOverallProgress("Obtaining bid and ask end dates");

                var now = DateTime.UtcNow.RoundToSecond();

                (ICandle askEndCandle, ICandle bidEndCandle) = await GetFirstTargetHistoryCandlesAsync(askStartDate, bidStartDate);

                var askEndDate = askEndCandle?.Timestamp ?? now;
                var bidEndDate = bidEndCandle?.Timestamp ?? now;

                _telemetryService.UpdateEndDates(askEndDate, bidEndDate);
                _telemetryService.UpdateOverallProgress("Processing bid and ask feed history, generating mid history");

                await Task.WhenAll(
                    ProcessAskAndBidHistoryAsync(askStartDate, askEndDate, askEndCandle, bidStartDate, bidEndDate, bidEndCandle),
                    askStartDate.HasValue && bidStartDate.HasValue
                        ? GenerateMidHistoryAsync(askStartDate.Value, bidStartDate.Value, askEndDate, bidEndDate)
                        : Task.CompletedTask);

                _telemetryService.UpdateOverallProgress("Done");
            }
            catch (Exception ex)
            {
                _telemetryService.UpdateOverallProgress($"Failed: {ex}");
                
                _log.Error(nameof(MigrateAsync), ex, context: _assetPair.Id);
            }
            finally
            {
                try
                {
                    _onStoppedAction.Invoke(_assetPair.Id);
                }
                catch (Exception ex)
                {
                    _log.Error(nameof(MigrateAsync), ex, context: _assetPair.Id);
                }
            }
        }

        private async Task<(DateTime? askStartDate, DateTime? bidStartDate)> GetStartDatesAsync()
        {
            var startDates = await Task.WhenAll(
                _historyProvider.GetStartDateAsync(_assetPair.Id, CandlePriceType.Ask),
                _historyProvider.GetStartDateAsync(_assetPair.Id, CandlePriceType.Bid));

            return (askStartDate: startDates[0], bidStartDate: startDates[1]);
        }

        private async Task<(ICandle askCandle, ICandle bidCandle)> GetFirstTargetHistoryCandlesAsync(DateTime? askStartDate, DateTime? bidStartDate)
        {
            var getAskEndCandleTask = askStartDate.HasValue
                ? _candlesHistoryMigrationService.GetFirstCandleOfHistoryAsync(_assetPair.Id, CandlePriceType.Ask)
                : Task.FromResult<ICandle>(null);
            var getBidEndCandleTask = bidStartDate.HasValue
                ? _candlesHistoryMigrationService.GetFirstCandleOfHistoryAsync(_assetPair.Id, CandlePriceType.Bid)
                : Task.FromResult<ICandle>(null);
            var endCandles = await Task.WhenAll(getAskEndCandleTask, getBidEndCandleTask);

            return (askCandle: endCandles[0], bidCandle: endCandles[1]);
        }

        private async Task ProcessAskAndBidHistoryAsync(
            DateTime? askStartDate, DateTime askEndDate, ICandle askEndCandle,
            DateTime? bidStartDate, DateTime bidEndDate, ICandle bidEndCandle)
        {
            try
            {
                var processAskCandlesTask = askStartDate.HasValue
                    ? _historyProvider.GetHistoryByChunksAsync(_assetPair, CandlePriceType.Ask, askEndDate, askEndCandle, ProcessHistoryChunkAsync, _cts.Token)
                    : Task.CompletedTask;
                var processBidkCandlesTask = bidStartDate.HasValue
                    ? _historyProvider.GetHistoryByChunksAsync(_assetPair, CandlePriceType.Bid, bidEndDate, bidEndCandle, ProcessHistoryChunkAsync, _cts.Token)
                    : Task.CompletedTask;

                await Task.WhenAny(processAskCandlesTask, processBidkCandlesTask);

                _isAskOrBidMigrationCompleted = true;

                await Task.WhenAll(processAskCandlesTask, processBidkCandlesTask);
            }
            catch
            {
                _cts.Cancel();
                throw;
            }
        }

        private async Task GenerateMidHistoryAsync(DateTime askStartTime, DateTime bidStartTime, DateTime askEndTime, DateTime bidEndTime)
        {
            try
            {
                var midStartTime = askStartTime < bidStartTime ? bidStartTime : askStartTime;
                var midEndTime = (askEndTime < bidEndTime ? askEndTime : bidEndTime).AddSeconds(-1);

                _prevMidTimestamp = midStartTime.AddSeconds(-1);

                while (_telemetryService.CurrentMidDate < midEndTime && !_cts.IsCancellationRequested)
                {
                    // Lets migrate some bid and ask history
                    await Task.Delay(1000);

                    var bidAskHistory = _bidAskHCacheService.PopReadyHistory();

                    if (!bidAskHistory.Any() && _isAskOrBidMigrationCompleted)
                    {
                        // If bid or ask migration is completed and there are not more bid/ask history to read,
                        // then no more mid candle can be created 

                        return;
                    }

                    var secMidCandles = new List<ICandle>();

                    foreach (var item in bidAskHistory)
                    {
                        _telemetryService.UpdateCurrentHistoryDate(item.timestamp, CandlePriceType.Mid);

                        if (_cts.IsCancellationRequested)
                        {
                            return;
                        }

                        if (item.ask == null || item.bid == null)
                        {
                            _log.Warning(nameof(GenerateMidHistoryAsync), "bid or ask candle is empty", context: $"{_assetPair}-{item.timestamp}");
                            continue;
                        }

                        var midSecCandle = MidCandlesFactory.Create(item.ask, item.bid);

                        secMidCandles.Add(midSecCandle);
                    }

                    if (ProcessSecCandles(secMidCandles))
                    {
                        return;
                    }
                }
            }
            catch
            {
                _cts.Cancel();
                throw;
            }
        }

        private async Task ProcessHistoryChunkAsync(IReadOnlyList<ICandle> candles)
        {
            try
            {
                ProcessSecCandles(candles);

                _bidAskHCacheService.PushHistory(candles);

                await Task.CompletedTask;
            }
            catch
            {
                _cts.Cancel();
                throw;
            }
        }

        private bool ProcessSecCandles(IEnumerable<ICandle> secCandles)
        {
            foreach (var candle in secCandles)
            {
                if (_cts.IsCancellationRequested)
                {
                    return true;
                }

                if (_healthService.CandlesToDispatchQueueLength > _settings.Quotes.CandlesToDispatchLengthThrottlingThreshold)
                {
                    try
                    {
                        Task.Delay(_settings.Quotes.ThrottlingDelay).GetAwaiter().GetResult();
                    }
                    catch (TaskCanceledException)
                    {
                        return true;
                    }
                }

                _telemetryService.UpdateCurrentHistoryDate(candle.Timestamp, candle.PriceType);
                
                CheckCandleOrder(candle);

                _candlesPersistenceQueue.EnqueueCandle(candle);

                _intervalsToGenerate
                    .AsParallel()
                    .ForAll(interval =>
                    {
                        var mergingResult = _candlesGenerator.Merge(
                            assetPair: candle.AssetPairId,
                            priceType: candle.PriceType,
                            timeInterval: interval,
                            timestamp: candle.Timestamp,
                            open: candle.Open,
                            close: candle.Close,
                            low: candle.Low,
                            high: candle.High);

                        if (mergingResult.WasChanged)
                        {
                            _candlesPersistenceQueue.EnqueueCandle(mergingResult.Candle);
                        }
                    });
            }

            return false;
        }

        private void CheckCandleOrder(ICandle candle)
        {
            DateTime CheckTimestamp(CandlePriceType priceType, DateTime prevTimestamp, DateTime currentTimestamp)
            {
                var distance = currentTimestamp - prevTimestamp;

                if (distance == TimeSpan.Zero)
                {
                    throw new InvalidOperationException($"Candle {priceType} timestamp duplicated at {currentTimestamp}");
                }
                if (distance < TimeSpan.Zero)
                {
                    throw new InvalidOperationException($"Candle {priceType} timestamp is to old at {currentTimestamp}, prev was {prevTimestamp}");
                }
                //if (distance > TimeSpan.FromSeconds(1))
                //{
                //    throw new InvalidOperationException($"Candle {priceType} timestamp is skipped at {currentTimestamp}, prev was {prevTimestamp}");
                //}

                return currentTimestamp;
            }

            switch (candle.PriceType)
            {
                case CandlePriceType.Ask:
                    _prevAskTimestamp = CheckTimestamp(CandlePriceType.Ask, _prevAskTimestamp, candle.Timestamp);
                    break;
                case CandlePriceType.Bid:
                    _prevBidTimestamp = CheckTimestamp(CandlePriceType.Bid, _prevBidTimestamp, candle.Timestamp);
                    break;
                case CandlePriceType.Mid:
                    _prevMidTimestamp = CheckTimestamp(CandlePriceType.Mid, _prevMidTimestamp, candle.Timestamp);
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(candle.PriceType), candle.PriceType, "Invalid price type");
            }
        }

        public void Stop()
        {
            _cts.Cancel();
        }
    }
}
