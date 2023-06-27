namespace Distributed.Store.Shared.Kafka.Interfaces
{
    public interface IKafkaConsumer<TKey, TData>
    {
        Task Consume(string topicName, CancellationToken cancellationToken);
    }
}
