namespace Distributed.Store.Shared.RabbitMQ.Interfaces
{
    public interface IRabbitMQProducer<TData>
    {
        Task Produce(TData message, string? routingKey = null, CancellationToken cancellationToken = default);
        Task ProduceMany(IEnumerable<TData> messages, string? routingKey = null, CancellationToken cancellationToken = default);
    }
}
