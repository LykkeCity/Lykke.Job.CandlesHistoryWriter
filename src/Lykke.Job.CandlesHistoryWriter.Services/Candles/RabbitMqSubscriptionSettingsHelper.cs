using Lykke.Job.CandlesHistoryWriter.Services.Settings;
using Lykke.RabbitMqBroker.Subscriber;

namespace Lykke.Job.CandlesHistoryWriter.Services.Candles
{
    public class RabbitMqSubscriptionSettingsHelper : IRabbitMqSubscriptionSettingsHelper
    {
        private readonly RabbitEndpointSettings _settings;

        public RabbitMqSubscriptionSettingsHelper(RabbitEndpointSettings settings)
        {
            _settings = settings;
        }

        private RabbitMqSubscriptionSettings _settingsForCandlesUpdatedEvent;
        public RabbitMqSubscriptionSettings SettingsForCandlesUpdatedEvent
        {
            get
            {
                if (_settingsForCandlesUpdatedEvent == null)
                {
                    _settingsForCandlesUpdatedEvent = RabbitMqSubscriptionSettings
                        .CreateForSubscriber(_settings.ConnectionString, _settings.Namespace, "candles-v2", _settings.Namespace, "candleshistory")
                        .MakeDurable();
                }
                return _settingsForCandlesUpdatedEvent;
            }
        }
    }
}
