using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Common;
using Common.Log;
using JetBrains.Annotations;
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
        private readonly ICandlesManager _candlesManager;
        private readonly ICandlesChecker _candlesChecker;
        private readonly RabbitEndpointSettings _settings;

        private RabbitMqSubscriber<CandlesUpdatedEvent> _subscriber;

        public CandlesSubscriber(ILog log, ICandlesManager candlesManager, ICandlesChecker checker, RabbitEndpointSettings settings)
        {
            _log = log;
            _candlesManager = candlesManager;
            _candlesChecker = checker;
            _settings = settings;
        }

        public void Start()
        {
            var settings = RabbitMqSubscriptionSettings
                .CreateForSubscriber(_settings.ConnectionString, _settings.Namespace, "candles-v2", _settings.Namespace, "candleshistory")
                .MakeDurable();

            try
            {
                _subscriber = new RabbitMqSubscriber<CandlesUpdatedEvent>(settings,
                        new ResilientErrorHandlingStrategy(_log, settings,
                            retryTimeout: TimeSpan.FromSeconds(10),
                            retryNum: 10,
                            next: new DeadQueueErrorHandlingStrategy(_log, settings)))
                    .SetMessageDeserializer(new MessagePackMessageDeserializer<CandlesUpdatedEvent>())
                    .SetMessageReadStrategy(new MessageReadQueueStrategy())
                    .Subscribe(ProcessCandlesUpdatedEventAsync)
                    .CreateDefaultBinding()
                    .SetLogger(_log)
                    .Start();
            }
            catch (Exception ex)
            {
                _log.WriteErrorAsync(nameof(CandlesSubscriber), nameof(Start), null, ex).Wait();
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
                var validationErrors = ValidateQuote(candlesUpdate);
                if (validationErrors.Any())
                {
                    var message = string.Join("\r\n", validationErrors);
                    await _log.WriteWarningAsync(nameof(CandlesSubscriber), nameof(CandlesUpdatedEvent), candlesUpdate.ToJson(), message);

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
                        lastUpdateTimestamp: candleUpdate.ChangeTimestamp))
                    .ToArray();

                await _candlesManager.ProcessCandlesAsync(candles);
            }
            catch (Exception)
            {
                await _log.WriteWarningAsync(nameof(CandlesSubscriber), nameof(ProcessCandlesUpdatedEventAsync), candlesUpdate.ToJson(), "Failed to process candle");
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
