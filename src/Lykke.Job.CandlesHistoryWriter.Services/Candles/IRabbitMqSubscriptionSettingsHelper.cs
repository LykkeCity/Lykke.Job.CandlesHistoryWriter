using System;
using System.Collections.Generic;
using System.Text;
using Lykke.RabbitMqBroker.Subscriber;

namespace Lykke.Job.CandlesHistoryWriter.Services.Candles
{
    public interface IRabbitMqSubscriptionSettingsHelper
    {
        RabbitMqSubscriptionSettings SettingsForCandlesUpdatedEvent { get; }
    }
}
