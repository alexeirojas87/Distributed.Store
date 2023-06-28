namespace Distributed.Store.Shared.RabbitMQ.Interfaces
{
    public interface IRabbitMQProducer<TData>
    {
        Task Produce(TData message, string routingKey = "", CancellationToken cancellationToken = default);
        Task ProduceMany(IEnumerable<TData> messages, string routingKey = "", CancellationToken cancellationToken = default);
    }
}
