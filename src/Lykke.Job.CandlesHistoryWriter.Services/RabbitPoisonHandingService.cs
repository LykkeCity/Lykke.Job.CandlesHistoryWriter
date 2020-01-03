using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Common.Log;
using Lykke.Job.CandlesHistoryWriter.Services.Candles;
using Lykke.RabbitMqBroker.Publisher;
using Lykke.RabbitMqBroker.Subscriber;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Framing;

namespace Lykke.Job.CandlesHistoryWriter.Services
{
    public class RabbitPoisonHandingService<T> : IRabbitPoisonHandingService<T>, IDisposable where T : class
    {
        private readonly ILog _log;
        private readonly RabbitMqSubscriptionSettings _rabbitMqSubscriptionSettings;

        private readonly IMessageDeserializer<T> _messageDeserializer = new MessagePackMessageDeserializer<T>();
        private readonly IRabbitMqSerializer<T> _messageSerializer = new MessagePackMessageSerializer<T>();

        private readonly List<IModel> _channels = new List<IModel>();
        private IConnection _connection;
        private readonly SemaphoreSlim _semaphoreSlim = new SemaphoreSlim(1, 1);

        private string PoisonQueueName => $"{_rabbitMqSubscriptionSettings.QueueName}-poison";

        public RabbitPoisonHandingService(
            ILog log,
            IRabbitMqSubscriptionSettingsHelper rabbitMqSubscriptionSettingsHelper)
        {
            _log = log;
            _rabbitMqSubscriptionSettings = rabbitMqSubscriptionSettingsHelper.SettingsForCandlesUpdatedEvent;
        }

        public async Task<string> PutMessagesBack()
        {
            if (_semaphoreSlim.CurrentCount == 0)
            {
                throw new Exception($"Cannot start the process because it was already started and not yet finished.");
            }

            await _semaphoreSlim.WaitAsync(TimeSpan.FromMinutes(10));

            try
            {
                var factory = new ConnectionFactory { Uri = _rabbitMqSubscriptionSettings.ConnectionString };
                await _log.WriteInfoAsync(nameof(RabbitPoisonHandingService<T>), nameof(PutMessagesBack),
                    $"Trying to connect to {factory.Endpoint} ({_rabbitMqSubscriptionSettings.ExchangeName})");

                _connection = factory.CreateConnection();

                var publishingChannel = _connection.CreateModel();
                var subscriptionChannel = _connection.CreateModel();
                _channels.AddRange(new[] { publishingChannel, subscriptionChannel });

                var publishingArgs = new Dictionary<string, object>()
                {
                    {"x-dead-letter-exchange", _rabbitMqSubscriptionSettings.DeadLetterExchangeName}
                };

                subscriptionChannel.QueueDeclare(PoisonQueueName,
                    _rabbitMqSubscriptionSettings.IsDurable, false, false, null);

                var messagesFound = subscriptionChannel.MessageCount(PoisonQueueName);
                var processedMessages = 0;
                var result = "Undefined";

                if (messagesFound == 0)
                {
                    result = "No messages found in poison queue. Terminating the process.";

                    await _log.WriteWarningAsync(nameof(RabbitPoisonHandingService<T>), nameof(PutMessagesBack),
                        $"No messages found in poison queue. Terminating the process.");
                    FreeResources();
                    return result;
                }
                else
                {
                    await _log.WriteInfoAsync(nameof(RabbitPoisonHandingService<T>), nameof(PutMessagesBack),
                        $"{messagesFound} messages found in poison queue. Starting the process.");
                }

                publishingChannel.QueueDeclare(_rabbitMqSubscriptionSettings.QueueName,
                    _rabbitMqSubscriptionSettings.IsDurable, false, false, publishingArgs);

                var consumer = new EventingBasicConsumer(subscriptionChannel);
                consumer.Received += (ch, ea) =>
                {
                    var message = RepackMessage(ea.Body);

                    if (message != null)
                    {
                        try
                        {
                            var properties = !string.IsNullOrEmpty(_rabbitMqSubscriptionSettings.RoutingKey)
                                ? new BasicProperties { Type = _rabbitMqSubscriptionSettings.RoutingKey }
                                : null;

                            publishingChannel.BasicPublish(_rabbitMqSubscriptionSettings.ExchangeName,
                                _rabbitMqSubscriptionSettings.RoutingKey ?? "", properties, message);

                            subscriptionChannel.BasicAck(ea.DeliveryTag, false);

                            processedMessages++;
                        }
                        catch (Exception e)
                        {
                            _log.WriteErrorAsync(nameof(RabbitPoisonHandingService<T>), nameof(PutMessagesBack), $"Error resending message: {e.Message}", e);
                        }
                    }
                };

                var sw = new Stopwatch();
                sw.Start();

                var tag = subscriptionChannel.BasicConsume(PoisonQueueName, false,
                    consumer);

                await _log.WriteInfoAsync(nameof(RabbitPoisonHandingService<T>), nameof(PutMessagesBack),
                    $"Consumer {tag} started.");

                while (processedMessages < messagesFound)
                {
                    Thread.Sleep(100);

                    if (sw.ElapsedMilliseconds > 30000)
                    {
                        await _log.WriteWarningAsync(nameof(RabbitPoisonHandingService<T>), nameof(PutMessagesBack),
                            $"Messages resend takes more than 30s. Terminating the process.");

                        break;
                    }
                }

                result = $"Messages resend finished. Initial number of messages {messagesFound}. Processed number of messages {processedMessages}";

                await _log.WriteInfoAsync(nameof(RabbitPoisonHandingService<T>), nameof(PutMessagesBack), result);

                FreeResources();

                return result;
            }
            catch (Exception exception)
            {
                var result =
                    $"Exception [{exception.Message}] thrown while putting messages back from poison to queue {_rabbitMqSubscriptionSettings.QueueName}. Stopping the process.";

                await _log.WriteErrorAsync(nameof(RabbitPoisonHandingService<T>), nameof(PutMessagesBack), result, exception);

                return result;
            }
        }

        private void FreeResources()
        {
            foreach (var channel in _channels)
            {
                channel?.Close();
                channel?.Dispose();
            }
            _connection?.Close();
            _connection?.Dispose();

            _semaphoreSlim.Release();

            _log.WriteInfo(nameof(RabbitPoisonHandingService<T>), nameof(FreeResources), $"Channels and connection disposed.");
        }

        public void Dispose()
        {
            FreeResources();
        }

        private byte[] RepackMessage(byte[] serializedMessage)
        {
            T message;
            try
            {
                message = _messageDeserializer.Deserialize(serializedMessage);
            }
            catch (Exception exception)
            {
                _log.WriteErrorAsync(this.GetType().Name, nameof(RepackMessage),
                    $"Failed to deserialize the message: {serializedMessage} with {_messageDeserializer.GetType().Name}. Stopping.",
                    exception).GetAwaiter().GetResult();
                return null;
            }

            return _messageSerializer.Serialize(message);
        }
    }
}
