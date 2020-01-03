// Copyright (c) 2019 Lykke Corp.
// See the LICENSE file in the project root for more information.

using Autofac;
using Common;
using Lykke.RabbitMqBroker.Subscriber;

namespace Lykke.Job.CandlesHistoryWriter.Core.Services.Candles
{
    public interface ICandlesSubscriber : IStartable, IStopable
    {
        RabbitMqSubscriptionSettings SubscriptionSettings { get; }
    }
}
