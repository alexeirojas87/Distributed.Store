using Distributed.Store.Shared.Kafka.Models;

namespace Distributed.Store.Shared.Kafka.Interfaces
{
    public interface IKafkaProducer<TKey, TData>
    {
        Task<KafkaResponse> Produce(TKey key, TData data, string topicName, CancellationToken cancellationToken);
    }
}
