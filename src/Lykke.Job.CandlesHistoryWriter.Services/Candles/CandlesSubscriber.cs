using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Common;
using Common.Log;
using JetBrains.Annotations;
using Lykke.Common.Log;
using Lykke.Job.CandlesProducer.Contract;
using Lykke.RabbitMqBroker;
using Lykke.RabbitMqBroker.Subscriber;
using Lykke.Job.CandlesHistoryWriter.Core.Domain.Candles;
using Lykke.Job.CandlesHistoryWriter.Core.Services.Candles;
using Lykke.Job.CandlesHistoryWriter.Services.Settings;

namespace Lykke.Job.CandlesHistoryWriter.Services.Candles
{
    [UsedImplicitly(ImplicitUseKindFlags.InstantiatedNoFixedConstructorSignature)]
    public class CandlesSubscriber : ICandlesSubscriber
    {
        private readonly ILog _log;
        private readonly ILogFactory _logFactory;
        private readonly ICandlesManager _candlesManager;
        private readonly ICandlesChecker _candlesChecker;
        private readonly RabbitEndpointSettings _settings;
        private readonly ICandlesCacheInitalizationService _cacheInitalizationService;

        private RabbitMqSubscriber<CandlesUpdatedEvent> _subscriber;

        public CandlesSubscriber(
            ILogFactory logFactory, 
            ICandlesManager candlesManager,
            ICandlesChecker checker, 
            RabbitEndpointSettings settings,
            ICandlesCacheInitalizationService candlesCacheInitalizationService)
        {
            _log = logFactory.CreateLog(this);
            _logFactory = logFactory;
            _candlesManager = candlesManager;
            _candlesChecker = checker;
            _settings = settings;
            _cacheInitalizationService = candlesCacheInitalizationService;
        }

        public void Start()
        {
            var settings = RabbitMqSubscriptionSettings
                .ForSubscriber(_settings.ConnectionString, _settings.Namespace, "candles-v2", _settings.Namespace, "candleshistory")
                .MakeDurable();

            try
            {
                _subscriber = new RabbitMqSubscriber<CandlesUpdatedEvent>(_logFactory, settings,
                        new ResilientErrorHandlingStrategy(_logFactory, settings,
                            retryTimeout: TimeSpan.FromSeconds(10),
                            retryNum: 10,
                            next: new DeadQueueErrorHandlingStrategy(_logFactory, settings)))
                    .SetMessageDeserializer(new MessagePackMessageDeserializer<CandlesUpdatedEvent>())
                    .SetMessageReadStrategy(new MessageReadQueueStrategy())
                    .Subscribe(ProcessCandlesUpdatedEventAsync)
                    .CreateDefaultBinding();
                    //.Start();
            }
            catch (Exception ex)
            {
                _log.Error(nameof(Start), ex);
                throw;
            }
        }

        public void Stop()
        {
            _subscriber?.Stop();
        }

        private async Task ProcessCandlesUpdatedEventAsync(CandlesUpdatedEvent candlesUpdate)
        {
            try
            {
                if (_cacheInitalizationService.InitializationState != CacheInitializationState.Idle)
                {
                    await Task.Delay(5000);
                    throw new InvalidOperationException("Initialization in progress");
                };
                
                var validationErrors = ValidateQuote(candlesUpdate);

                if (validationErrors.Any())
                {
                    var message = string.Join("\r\n", validationErrors);
                    _log.Warning(nameof(ProcessCandlesUpdatedEventAsync), message, context: candlesUpdate.ToJson());

                    return;
                }

                var candles = candlesUpdate.Candles
                    .Where(candleUpdate =>
                        Constants.StoredIntervals.Contains(candleUpdate.TimeInterval) &&
                        _candlesChecker.CanHandleAssetPair(candleUpdate.AssetPairId))
                    .Select(candleUpdate => Candle.Create(
                        priceType: candleUpdate.PriceType,
                        assetPair: candleUpdate.AssetPairId,
                        timeInterval: candleUpdate.TimeInterval,
                        timestamp: candleUpdate.CandleTimestamp,
                        open: candleUpdate.Open,
                        close: candleUpdate.Close,
                        low: candleUpdate.Low,
                        high: candleUpdate.High,
                        tradingVolume: candleUpdate.TradingVolume,
                        tradingOppositeVolume: candleUpdate.TradingOppositeVolume,
                        lastTradePrice: candleUpdate.LastTradePrice,
                        lastUpdateTimestamp: candleUpdate.ChangeTimestamp))
                    .ToArray();

                await _candlesManager.ProcessCandlesAsync(candles);
            }
            catch (Exception)
            {
                _log.Warning(nameof(ProcessCandlesUpdatedEventAsync), "Failed to process candle", context: candlesUpdate.ToJson());
                throw;
            }
        }

        private static IReadOnlyCollection<string> ValidateQuote(CandlesUpdatedEvent message)
        {
            var errors = new List<string>();

            if (message == null)
            {
                errors.Add("message is null.");

                return errors;
            }

            if (message.ContractVersion == null)
            {
                errors.Add("Contract version is not specified");

                return errors;
            }

            if (message.ContractVersion.Major != CandlesProducer.Contract.Constants.ContractVersion.Major &&
                // Version 2 and 3 is still supported
                message.ContractVersion.Major != 2 &&
                message.ContractVersion.Major != 3)
            {
                errors.Add("Unsupported contract version");

                return errors;
            }

            if (message.Candles == null || !message.Candles.Any())
            {
                errors.Add("Candles is empty");

                return errors;
            }

            for (var i = 0; i < message.Candles.Count; ++i)
            {
                var candle = message.Candles[i];

                if (string.IsNullOrWhiteSpace(candle.AssetPairId))
                {
                    errors.Add($"Empty '{nameof(candle.AssetPairId)}' in the candle {i}");
                }
                if (candle.CandleTimestamp.Kind != DateTimeKind.Utc)
                {
                    errors.Add($"Invalid '{candle.CandleTimestamp}' kind (UTC is required) in the candle {i}");
                }

                if (candle.TimeInterval == CandleTimeInterval.Unspecified)
                {
                    errors.Add($"Invalid 'TimeInterval' in the candle {i}");
                }

                if (candle.PriceType == CandlePriceType.Unspecified)
                {
                    errors.Add($"Invalid 'PriceType' in the candle {i}");
                }
            }

            return errors;
        }

        public void Dispose()
        {
            Stop();
        }
    }
}
