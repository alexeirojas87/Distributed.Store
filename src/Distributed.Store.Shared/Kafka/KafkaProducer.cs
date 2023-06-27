using Chr.Avro.Confluent;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Distributed.Store.Shared.Kafka.Interfaces;
using Distributed.Store.Shared.Kafka.Models;
using Distributed.Store.Shared.Tools;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using OpenTelemetry;
using OpenTelemetry.Context.Propagation;
using Polly;
using System.Diagnostics;
using System.Text;

namespace Distributed.Store.Shared.Kafka
{
    public class KafkaProducer<TKey, TData> : IKafkaProducer<TKey, TData>, IDisposable
    {
        private readonly ILogger<KafkaProducer<TKey, TData>> _logger;
        private readonly int _maxRetryAttempts;
        private readonly IProducer<TKey, TData> _producer;
        public KafkaProducer(
            IOptions<KafkaConfig> options,
            ILogger<KafkaProducer<TKey, TData>> logger)
        {
            _logger = logger;
            _maxRetryAttempts = options.Value.MaxRetryAttempts;
            var producerOptions = options.Value.ProducerConfig;
            _producer = options.Value.ValueDeserializer switch
            {
                "avro" => new ProducerBuilder<TKey, TData>(producerOptions)
                                        .SetErrorHandler((_, e) => KafkaErrorHandler.HandleError(_logger, e))
                                        .SetAvroValueSerializer(new CachedSchemaRegistryClient(options.Value.SchemaRegistryConfig), AutomaticRegistrationBehavior.Always)
                                        .Build(),
                _ => new ProducerBuilder<TKey, TData>(producerOptions)
                                        .SetErrorHandler((_, e) => KafkaErrorHandler.HandleError(_logger, e))
                                        .SetValueSerializer(new KafkaValueSerializer<TData>())
                                        .Build(),
            };
        }
        /// <summary>
        /// Produce message to Kafka using polly retry mechanism
        /// </summary>
        /// <param name="key"></param>
        /// <param name="data"></param>
        /// <param name="topicName"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        /// <exception cref="ArgumentNullException"></exception>
        public async Task<KafkaResponse> Produce(TKey key, TData data, string topicName, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();

            _ = data ?? throw new ArgumentNullException(nameof(data));

            var serializedData = data.Serialize();

            var kafkaResponse = new KafkaResponse
            {
                Completed = false,
                ErrorMessage = $"ERROR: Message delivery failed after {_maxRetryAttempts} attempts. Topic {topicName} Value: {serializedData}"
            };

            try
            {
                _ = await Policy.Handle<Exception>()
                      .WaitAndRetryAsync
                      (
                      retryCount: _maxRetryAttempts,
                      sleepDurationProvider: retryAttempt => TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)),
                      onRetry: (exception, timeSpan, context) =>
                      {
                          _logger.LogDebug("Retry Message error: {exMessage}. in topic {topicName}. Value: {message}", exception.Message, topicName, data);
                      }
                      ).ExecuteAndCaptureAsync(async () =>
                      {
                          //inject opentelemetry headers
                          var headers = new Headers();
                          if (Activity.Current is not null)
                              Propagators.DefaultTextMapPropagator.Inject(new PropagationContext(Activity.Current.Context, Baggage.Current), headers, InjectKafkaHeaders);

                          var deliveryResult = await _producer.ProduceAsync(topicName, new Message<TKey, TData>
                          {
                              Key = key,
                              Value = data,
                              Headers = headers
                          }, cancellationToken);

                          if (deliveryResult.Status == PersistenceStatus.Persisted)
                          {
                              _logger.LogDebug("Message to {topicName} sent Value: {message}. Status: {status}, TraceId {traceid}", topicName, data, deliveryResult.Status, Activity.Current?.TraceId);
                              kafkaResponse.Completed = true;
                              kafkaResponse.ErrorMessage = "";
                          }
                          else
                          {
                              _logger.LogError("ERROR: Message not ack'd by all brokers, topic {topicName}, Value: {@message}. Status: {@status}, TraceId {traceid}", topicName, data, deliveryResult.Status, Activity.Current?.TraceId);
                              kafkaResponse.ErrorMessage = $"ERROR: Message not ack'd by all brokers, topic {topicName}, (Value: '{serializedData}')";
                          }
                      });

                return kafkaResponse;
            }
            catch (Exception ex)
            {
                _logger.LogError("General error producing Kafka message. {error}", ex.Message);
                return new KafkaResponse
                {
                    Completed = false,
                    ErrorMessage = $"General error producing Kafka message. (Value: {serializedData}, Error: {ex.Message})"
                };
            }
        }

        private static void InjectKafkaHeaders(Headers headers, string key, string value)
        {
            headers.Add(new Header(key, Encoding.UTF8.GetBytes(value)));
        }

        public void Dispose()
        {
            _producer.Flush();
            _producer.Dispose();
            GC.SuppressFinalize(this);
        }
    }
}
