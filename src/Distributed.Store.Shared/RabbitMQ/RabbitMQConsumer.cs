using Distributed.Store.Shared.Kafka.Interfaces;
using Distributed.Store.Shared.RabbitMQ.Interfaces;
using Distributed.Store.Shared.RabbitMQ.Models;
using Distributed.Store.Shared.Tools;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using OpenTelemetry.Context.Propagation;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Diagnostics;
using System.Text;
using System.Text.Json;

namespace Distributed.Store.Shared.RabbitMQ
{
    public class RabbitMQConsumer<TData> : IRabbitMQConsumer<TData>
    {
        private readonly RabbitMQConfig _settings;
        private readonly ConnectionFactory _connectionFactory;
        private readonly IMessageHandler<TData> _handler;
        private readonly ILogger<RabbitMQConsumer<TData>> _logger;
        public RabbitMQConsumer(
            IOptions<RabbitMQConfig> rabbitMQConfig,
            IMessageHandler<TData> handler,
            ILogger<RabbitMQConsumer<TData>> logger)
        {
            _settings = rabbitMQConfig.Value;
            _handler = handler;
            _logger = logger;
            _connectionFactory = new ConnectionFactory()
            {
                HostName = _settings.Hostname,
                Password = _settings.Credentials!.Password,
                UserName = _settings.Credentials.Username
            };
        }
        public Task StartAsync(string queueName, CancellationToken cancelToken = default)
        {
            return Task.Run(async () => await Consume(queueName, cancelToken), cancelToken);
        }

        private async Task Consume(string queueName, CancellationToken cancellationToken)
        {
            using IConnection connection = _connectionFactory.CreateConnection();
            using IModel channel = connection.CreateModel();
            channel.BasicQos(0, 10, false);
            var cancellationTokenShutdown = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            var consumer = new EventingBasicConsumer(channel);

            consumer.Shutdown += (_, ea) =>
            {
                _logger.LogError("RabbitMQ event shutdown was raised shutdown args {args}", ea.ToString());
                cancellationTokenShutdown.Cancel();
            };
            consumer.Received += async (_, ea) => await HandleMessage(channel, ea, cancellationToken);

            channel.BasicConsume(queue: queueName, autoAck: false, consumer: consumer);

            try
            {
                await Task.Delay(Timeout.Infinite, cancellationToken);
            }
            catch (OperationCanceledException)
            { }
        }

        private static IEnumerable<string> ExtractRabbitMQHeaders(IDictionary<string, object> headers, string key)
        {
            if (headers != null && headers.TryGetValue(key, out var headerValue))
            {
                if (headerValue is byte[] byteValue)
                {
                    yield return Encoding.UTF8.GetString(byteValue);
                }
                else if (headerValue is string strValue)
                {
                    yield return strValue;
                }
            }
        }

        private async Task HandleMessage(IModel channel, BasicDeliverEventArgs ea, CancellationToken cancellationToken)
        {
            var body = ea.Body.ToArray();
            var deliveryTag = ea.DeliveryTag;
            TData message;
            try
            {
                message = body.Deserialize<TData>();
            }
            catch (JsonException e)
            {
                _logger.LogError(e, "Failed to deserialize JSON message in rabbitMQ consumer from exchange {exchange}, routing key {routingKey}, delivery tag {deliveryTag}, message content: {content}"
                                , ea.Exchange, ea.RoutingKey, ea.DeliveryTag, Encoding.UTF8.GetString(body));
                HandleErrorMessage(channel, ea);
                return;
            }

            _logger.LogDebug("Received: from exchange {exchange}, routing key {routingKey}, delivery tag {deliveryTag}, message {message}, and traceId {IdTransaction}",
                                           ea.Exchange, ea.RoutingKey, ea.DeliveryTag, message, Activity.Current?.TraceId);

            if (ea.BasicProperties.Headers.Any())
            {
                var context = Propagators.DefaultTextMapPropagator.Extract(default, ea.BasicProperties.Headers, ExtractRabbitMQHeaders);
                if (context.ActivityContext.TraceId != default)
                    TraceTool.ModifyOTLPActivity(context.ActivityContext.TraceId.ToString());
            }

            try
            {
                await _handler.HandleMessage(message, cancellationToken);
                channel.BasicAck(deliveryTag: deliveryTag, multiple: false);
                _logger.LogDebug("ACK to: from exchange {exchange}, routing key {routingKey}, delivery tag {deliveryTag}, message {message}, and traceId {IdTransaction}",
                                           ea.Exchange, ea.RoutingKey, ea.DeliveryTag, message, Activity.Current?.TraceId);
            }
            catch (Exception e)
            {
                _logger.LogError(e, "Failed to handle message: from exchange {exchange}, routing key {routingKey}, delivery tag {deliveryTag}, message {message}, and traceId {IdTransaction}",
                                           ea.Exchange, ea.RoutingKey, ea.DeliveryTag, message, Activity.Current?.TraceId);
                HandleErrorMessage(channel, ea);
            }
        }

        private static void HandleErrorMessage(IModel channel, BasicDeliverEventArgs ea)
        {
            var properties = channel.CreateBasicProperties();
            if (ea.BasicProperties.Headers.TryGetValue("x-retry-count", out var retryCountObj) && retryCountObj is long retryCount)
            {
                if (retryCount < 5)
                {
                    properties.Headers = new Dictionary<string, object> { { "x-retry-count", ++retryCount } };
                    channel.BasicPublish(ea.Exchange, ea.RoutingKey, properties, ea.Body);
                    channel.BasicAck(ea.DeliveryTag, false); // acknowledge the original message
                }
                else
                {
                    // if the retry count is 5 or more, do not republish and simply ACK the message
                    channel.BasicAck(ea.DeliveryTag, false);
                }
            }
            else
            {
                properties.Headers = new Dictionary<string, object> { { "x-retry-count", 1 } };
                channel.BasicPublish(ea.Exchange, ea.RoutingKey, properties, ea.Body);
                channel.BasicAck(ea.DeliveryTag, false); // acknowledge the original message
            }
        }

    }
}
