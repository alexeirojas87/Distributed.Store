using Chr.Avro.Confluent;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Distributed.Store.Shared.Kafka.Interfaces;
using Distributed.Store.Shared.Kafka.Models;
using Distributed.Store.Shared.Tools;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using OpenTelemetry.Context.Propagation;
using System.Diagnostics;
using System.Text;

namespace Distributed.Store.Shared.Kafka
{
    public class KafkaConsumer<TKey, TData> : IKafkaConsumer<TKey, TData>, IDisposable
    {
        private readonly IConsumer<TKey, TData> _consumer;
        private readonly IMessageHandler<TData> _handler;
        private readonly ILogger<KafkaConsumer<TKey, TData>> _logger;
        private bool _alreadyRunning = false;
        private readonly KafkaConfig _kafkaConfig;
        public KafkaConsumer(
            IOptions<KafkaConfig> kafkaConfigs,
            IMessageHandler<TData> handler,
            ILogger<KafkaConsumer<TKey, TData>> logger)
        {
            _logger = logger;
            _kafkaConfig = kafkaConfigs.Value;
            _handler = handler;

            _consumer = _kafkaConfig.ValueDeserializer switch
            {
                "avro" => new ConsumerBuilder<TKey, TData>(_kafkaConfig.ConsumerConfig)
                                        .SetAvroValueDeserializer(new CachedSchemaRegistryClient(_kafkaConfig.SchemaRegistryConfig))
                                        .SetErrorHandler((_, e) => KafkaErrorHandler.HandleError(_logger, e))
                                        .Build(),
                _ => new ConsumerBuilder<TKey, TData>(_kafkaConfig.ConsumerConfig)
                                        .SetValueDeserializer(new KafkaValueSerializer<TData>())
                                        .SetErrorHandler((_, e) => KafkaErrorHandler.HandleError(_logger, e))
                                        .Build(),
            };
        }

        public async Task Consume(string topicName, CancellationToken cancellationToken)
        {
            if (_alreadyRunning)
                throw new InvalidOperationException("Consumer has been already started");

            _alreadyRunning = true;

            _consumer.Subscribe(topicName);

            using (_consumer)
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    try
                    {
                        cancellationToken.ThrowIfCancellationRequested();

                        var result = _consumer.Consume(cancellationToken);
                        _logger.LogDebug("Received: {messageKey}:{message} from partition: {partitionValue}, and traceId {IdTransaction} EOF {eof}",
                                                result.Message.Key, result.Message.Value?.Serialize(), result.Partition.Value, Activity.Current?.TraceId, result.IsPartitionEOF);
                        if (result.Message.Headers.Any())
                        {
                            var context = Propagators.DefaultTextMapPropagator.Extract(default, result.Message.Headers, ExtractKafkaHeaders);
                            if (context.ActivityContext.TraceId != default)
                                TraceTool.ModifyOTLPActivity(context.ActivityContext.TraceId.ToString());
                        }

                        if (result.IsPartitionEOF)
                            continue;

                        await _handler.HandleMessage(result.Message.Value, cancellationToken);

                        await CommitMessage(result, cancellationToken);
                    }
                    catch (ConsumeException exc)
                    {
                        _logger.LogWarning("Consume Exception was occurred. {error}, and traceId {IdTransaction}", exc.Message, Activity.Current?.TraceId);
                        if (exc.Error.IsFatal)
                            throw;
                    }
                    catch (OperationCanceledException)
                    {
                        _logger.LogInformation("EventMessageConsumers is Closed");
                        _consumer.Close();
                        throw;
                    }
                    catch (Exception exc)
                    {
                        _logger.LogError("General error in consumer {error}", exc.Message);
                        throw;
                    }
                }
            }
        }

        private async Task CommitMessage(ConsumeResult<TKey, TData> result, CancellationToken cancellationToken)
        {
            if (_kafkaConfig.ConsumerConfig?.EnableAutoCommit is null || (bool)_kafkaConfig.ConsumerConfig.EnableAutoCommit)
                return;

            var attempts = 0;
            do
            {
                try
                {
                    attempts++;
                    _consumer.Commit(result);
                    _consumer.StoreOffset(result);
                    _logger.LogDebug("Commit to: {messageKey}:{message} from partition: {partitionValue}, and traceId {IdTransaction}",
                                                result.Message.Key, result.Message.Value, result.Partition.Value, Activity.Current?.TraceId);
                    break;
                }
                catch (KafkaException exc)
                {
                    if (attempts == _kafkaConfig.CommitMaxRetryCount)
                    {
                        _logger.LogError(exc, "Offset committing error at attempt #{attempts}: {reason}, and traceId {IdTransaction}, exit for reaching the maximum retry limit", attempts, exc.Error.Reason, Activity.Current?.TraceId);
                        throw;
                    }

                    _logger.LogError(exc, "Offset committing error at attempt #{attempts}: {reason}, and traceId {IdTransaction}", attempts, exc.Error.Reason, Activity.Current?.TraceId);
                    await Task.Delay(_kafkaConfig.CommitRetryDelay, cancellationToken);
                }
            } while (!cancellationToken.IsCancellationRequested);
        }

        private static IEnumerable<string> ExtractKafkaHeaders(Headers headers, string key)
        {
            var header = headers.Where(h => h.Key == key).LastOrDefault();
            return header != null ? new[] { Encoding.UTF8.GetString(header.GetValueBytes()) } : Enumerable.Empty<string>();
        }
        public void Dispose()
        {
            _consumer.Close();
            _consumer.Dispose();
            GC.SuppressFinalize(this);
        }
    }
}
