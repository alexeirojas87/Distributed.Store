using Distributed.Store.Shared.RabbitMQ.Interfaces;
using Distributed.Store.Shared.RabbitMQ.Models;
using Distributed.Store.Shared.Tools;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using OpenTelemetry;
using OpenTelemetry.Context.Propagation;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;
using System.Diagnostics;

namespace Distributed.Store.Shared.RabbitMQ
{
    public class RabbitMQProducer<TData> : IRabbitMQProducer<TData>, IDisposable
    {
        private readonly object _lock = new();
        private readonly RabbitMQConfig _config;
        private readonly IConnectionFactory _connectionFactory;
        private readonly ILogger<RabbitMQProducer<TData>> _logger;
        private IConnection _connection;
        private bool disposedValue;

        public RabbitMQProducer(
            IConnectionFactory connectionFactory,
            IOptions<RabbitMQConfig> rabbitMQConfig,
            ILogger<RabbitMQProducer<TData>> logger)
        {
            _config = rabbitMQConfig.Value;
            _logger = logger;
            _connectionFactory = connectionFactory;
            StarProducer();
        }

        public Task Produce(TData message, string routingKey = "", CancellationToken cancellationToken = default)
        {
            using var channel = _connection.CreateModel();
            PublishSingle(message, channel, routingKey);

            return Task.CompletedTask;
        }

        public Task ProduceMany(IEnumerable<TData> messages, string routingKey = "", CancellationToken cancellationToken = default)
        {
            using var channel = _connection.CreateModel();
            foreach (var message in messages)
            {
                PublishSingle(message, channel, routingKey);
            }

            return Task.CompletedTask;
        }

        private void PublishSingle(TData message, IModel model, string routingKey = "")
        {
            var properties = model.CreateBasicProperties();
            properties.Persistent = true;
            properties.Headers = new Dictionary<string, object>();

            if (Activity.Current is not null)
                Propagators.DefaultTextMapPropagator.Inject(new PropagationContext(Activity.Current.Context, Baggage.Current), properties.Headers, InjectRabbitMQHeaders);

            lock (_lock)
            {
                try
                {
                    model.BasicPublish(exchange: _config.Exchange.Name,
                    routingKey: routingKey,
                    basicProperties: properties,
                    body: message.SerializeToUtf8Bytes());
                    _logger.LogDebug("Published message {message} in exchange {exchange} with routing key {routingKey}", message, _config.Exchange, routingKey);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error while publishing a message");
                    throw;
                }
            }
        }

        private static void InjectRabbitMQHeaders(IDictionary<string, object> headers, string key, string value)
        {
            headers.Add(key, value);
        }
        internal void StarProducer()
        {
            try
            {
                if (_connection == null || !_connection.IsOpen)
                    _connection = _connectionFactory.CreateConnection();
            }
            catch (BrokerUnreachableException ex)
            {
                _logger.LogError(ex, "Unreachable error in RabbitMQ producer");
                throw;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "General error in RabbitMQ producer");
                throw;
            }

        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    _connection.Dispose();
                }
                disposedValue = true;
            }
        }

        public void Dispose()
        {
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }
    }
}
